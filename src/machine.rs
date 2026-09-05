//! The state machine.
//!
//! `apply` is pure: no clock, no RNG, no I/O. It takes `&Order` and returns
//! a new one, so an `Err` cannot publish a partial mutation — the draft is
//! local and is dropped on the error path.

use std::cmp::Ordering;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::order::{
    derive_status, funding_status, stranded_funds, to_outbox, Effect, Order, OrderStatus,
    OutboxEntry,
};
use crate::policy::FulfillmentPolicy;
use crate::settlement::{
    promoted_set_ref, refund_excess_id, EventKind, Finality, IdempotencyKey, ReversalKind,
    ReversalReason, Settlement,
};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum MachineError {
    #[error("illegal transition {from:?} --{event}-->")]
    Illegal { from: OrderStatus, event: &'static str },
    #[error("currency mismatch: order is {expected}, event is {actual}")]
    CurrencyMismatch { expected: String, actual: String },
    #[error("amount overflow")]
    AmountOverflow,
    #[error("event for order {expected} routed to {actual}")]
    WrongOrder { expected: Uuid, actual: Uuid },
    #[error("fulfil rejected: {why}")]
    FulfillRejected { why: &'static str },
    #[error("negative amount on {event}: {minor}")]
    NegativeAmount { event: &'static str, minor: i64 },
    #[error("refund of {amount} exceeds the {held} this attempt still holds")]
    RefundExceedsHeld { amount: i64, held: i64 },
    #[error("invalid attempt: {why}")]
    InvalidAttempt { why: &'static str },
    #[error("no attempt {provider}/{provider_invoice_id} on this order")]
    UnknownAttempt { provider: String, provider_invoice_id: String },
}

pub struct ApplyResult {
    pub order: Order,
    pub key: IdempotencyKey,
    pub effects: Vec<OutboxEntry>,
}

/// Whether the arm sets status itself or lets `derive_status` compute it.
enum Status {
    Derive,
    Set(OrderStatus),
}

pub struct OrderMachine<P> {
    pub policy: P,
}

impl<P: FulfillmentPolicy> OrderMachine<P> {
    pub fn new(policy: P) -> Self {
        Self { policy }
    }

    pub fn apply(
        &self,
        order: &Order,
        event: &Settlement,
        now: OffsetDateTime,
    ) -> Result<ApplyResult, MachineError> {
        if event.order_id() != order.id {
            return Err(MachineError::WrongOrder {
                expected: event.order_id(),
                actual: order.id,
            });
        }

        // Every `amount` on the wire is a magnitude, never a signed
        // correction. A negative one walks a summing accumulator backwards,
        // and once `net` exceeds anything ever observed the next
        // observation turns the difference into a `RefundExcess` for money
        // that never arrived. Rejected before any state is touched, and
        // from every status, so a driver bug is dead-lettered rather than
        // absorbed by the late-event arm.
        match event {
            Settlement::Reversed { amount, .. }
            | Settlement::Refunded { amount, .. }
            | Settlement::Disputed { amount, .. }
                if amount.minor < 0 =>
            {
                return Err(MachineError::NegativeAmount {
                    event: event.name(),
                    minor: amount.minor,
                })
            }
            // A non-negative delta falls through here, as does `Observed`:
            // `observed_total` is max-joined, not summed, so a negative one
            // is absorbed by the join and cannot move the accumulator.
            _ => {}
        }

        let key = event.idempotency_key();
        let mut next = order.clone();
        let ended = order.status.is_closed() || order.reversal_closed;

        let (status, raw_effects): (Status, Vec<Effect>) = match event {
            // ---- funds arriving after the order ended ------------------
            // Recorded, never silently swallowed. A late on-chain payment
            // past invoice expiry is routine; the money exists whether or
            // not the invoice does.
            Settlement::Observed { provider, provider_invoice_id, observed_total, .. }
                if ended =>
            {
                let attempt = next.attempt_mut(provider, provider_invoice_id)?;
                attempt.observed_total = attempt.observed_total.max_join(observed_total)?;
                attempt.last_tx_ref = Some(event.tx_ref().to_string());
                (
                    Status::Set(order.status),
                    vec![Effect::UnexpectedFunds {
                        provider: provider.clone(),
                        provider_invoice_id: provider_invoice_id.clone(),
                        observed: observed_total.clone(),
                    }],
                )
            }

            // ---- funding ----------------------------------------------
            Settlement::Observed {
                provider,
                provider_invoice_id,
                observed_total,
                finality,
                at,
                ..
            } => {
                let attempt = next.attempt_mut(provider, provider_invoice_id)?;
                attempt.observed_total = attempt.observed_total.max_join(observed_total)?;
                attempt.finality = Some(join_finality(attempt.finality.as_ref(), finality));
                attempt.last_tx_ref = Some(event.tx_ref().to_string());

                // Anchor the chargeback clock on the observation that first
                // makes THIS invoice whole, not on an earlier partial.
                if attempt.chargeback_window_ends.is_none()
                    && attempt.net()?.cmp_amount(&attempt.quoted)? != Ordering::Less
                {
                    // `at` is provider-supplied and gates when funds become
                    // irreversible, so it is never allowed past the ingest
                    // clock: a future-dated timestamp would hold the window
                    // open forever and the order could never ship. An `at` in
                    // the past is kept — the rail measures its window from the
                    // transaction, and a delayed webhook or a reconcile replay
                    // of real history must not have its clock restarted.
                    let anchor = (*at).min(now);
                    attempt.chargeback_window_ends =
                        self.policy.chargeback_window(provider, anchor);
                }

                let fx = self.funding_effects(&next)?;
                (Status::Derive, fx)
            }

            // ---- reversals --------------------------------------------
            // Accepted from any non-expired status, `Reversed` included:
            // sequential partial chargebacks must still record their loss.
            Settlement::Reversed { provider, provider_invoice_id, amount, reason, .. }
                if !order.status.is_closed() =>
            {
                let attempt = next.attempt_mut(provider, provider_invoice_id)?;
                attempt.reversed_total = attempt.reversed_total.add(amount)?;
                attempt.last_tx_ref = Some(event.tx_ref().to_string());
                next.saw_closing_reversal |= reason.kind.is_closing();

                let net = next.net()?;
                if next.saw_closing_reversal && net.minor <= 0 {
                    next.reversal_closed = true;
                }

                let mut fx = vec![Effect::RecordLoss {
                    reason: reason.clone(),
                    amount: amount.clone(),
                }];
                if next.fulfilled_at.is_some() {
                    fx.push(Effect::RecallFulfillment {
                        why: format!("{:?}:{}", reason.kind, reason.code),
                    });
                }
                // Still recoverable: ask for the remainder. A closing
                // reversal ends the order, so no point asking there. Never
                // ask for more than `due` just because net went negative —
                // clamp the net to zero before subtracting it from `due`.
                let funding = funding_status(&net, &next.due)?;
                if !next.reversal_closed
                    && matches!(funding, OrderStatus::Underpaid | OrderStatus::AwaitingPayment)
                {
                    fx.push(Effect::AwaitRemainder {
                        missing: next.due.sub(&net.clamp_zero())?.clamp_zero(),
                    });
                }
                (Status::Derive, fx)
            }

            // ---- refunds executed --------------------------------------
            Settlement::Refunded { provider, provider_invoice_id, amount, .. }
                if !order.status.is_closed() =>
            {
                let attempt = next.attempt_mut(provider, provider_invoice_id)?;
                // `refunded_total` sums and is subtracted from the excess, so
                // an inflated report drives `outstanding_excess` to zero and
                // the payer's real excess is never sent back — a silent loss
                // with no effect and no dead letter. Bounded here, before any
                // state is touched, so a driver bug dead-letters instead.
                let held = attempt.refundable()?;
                if amount.cmp_amount(&held)? == Ordering::Greater {
                    return Err(MachineError::RefundExceedsHeld {
                        amount: amount.minor,
                        held: held.minor,
                    });
                }
                attempt.refunded_total = attempt.refunded_total.add(amount)?;
                attempt.last_tx_ref = Some(event.tx_ref().to_string());
                (Status::Derive, vec![])
            }

            // ---- disputes ----------------------------------------------
            Settlement::Disputed { provider, provider_invoice_id, deadline, amount, .. }
                if !ended =>
            {
                let attempt =
                    order.attempt(provider, provider_invoice_id).ok_or_else(|| {
                        MachineError::UnknownAttempt {
                            provider: provider.clone(),
                            provider_invoice_id: provider_invoice_id.clone(),
                        }
                    })?;
                if amount.currency != order.due.currency
                    && amount.currency != attempt.quoted.currency
                {
                    return Err(MachineError::CurrencyMismatch {
                        expected: order.due.currency.clone(),
                        actual: amount.currency.clone(),
                    });
                }
                next.dispute_open = true;
                (
                    Status::Derive,
                    vec![
                        Effect::HoldFulfillment { why: "dispute open".into() },
                        Effect::OpenDispute { deadline: *deadline, amount: amount.clone() },
                    ],
                )
            }

            Settlement::DisputeResolved { provider, provider_invoice_id, won, .. }
                if order.dispute_open =>
            {
                if next.attempt(provider, provider_invoice_id).is_none() {
                    return Err(MachineError::UnknownAttempt {
                        provider: provider.clone(),
                        provider_invoice_id: provider_invoice_id.clone(),
                    });
                }
                next.dispute_open = false;
                if *won {
                    let fx = self.funding_effects(&next)?;
                    (Status::Derive, fx)
                } else {
                    let lost = {
                        let attempt = next.attempt_mut(provider, provider_invoice_id)?;
                        let lost = attempt.net()?.clamp_zero();
                        attempt.reversed_total = attempt.reversed_total.add(&lost)?;
                        lost
                    };
                    next.saw_closing_reversal = true;
                    // Other invoices may still be funding, so only latch
                    // the order closed if the *order's* net has hit zero.
                    if next.net()?.minor <= 0 {
                        next.reversal_closed = true;
                    }
                    let mut fx = vec![Effect::RecordLoss {
                        reason: ReversalReason {
                            kind: ReversalKind::Chargeback,
                            code: "lost".into(),
                        },
                        amount: lost,
                    }];
                    if next.fulfilled_at.is_some() {
                        fx.push(Effect::RecallFulfillment { why: "dispute lost".into() });
                    }
                    (Status::Derive, fx)
                }
            }

            // ---- provider-declared endings ------------------------------
            // These name one invoice. An order with several attempts is not
            // ended just because one of them expired or failed.
            Settlement::Expired { provider, provider_invoice_id, at, .. } => {
                if next.attempt(provider, provider_invoice_id).is_none() {
                    return Err(MachineError::UnknownAttempt {
                        provider: provider.clone(),
                        provider_invoice_id: provider_invoice_id.clone(),
                    });
                }
                let is_only_attempt = order.attempts.len() == 1;
                if is_only_attempt
                    && matches!(
                        order.status,
                        OrderStatus::Pending
                            | OrderStatus::AwaitingPayment
                            | OrderStatus::Underpaid
                    )
                {
                    (Status::Set(OrderStatus::Expired), stranded_funds(&next))
                } else {
                    next.attempt_mut(provider, provider_invoice_id)?.expires_at = Some(*at);
                    (Status::Set(order.status), vec![])
                }
            }

            Settlement::Failed { provider, provider_invoice_id, .. } => {
                if next.attempt(provider, provider_invoice_id).is_none() {
                    return Err(MachineError::UnknownAttempt {
                        provider: provider.clone(),
                        provider_invoice_id: provider_invoice_id.clone(),
                    });
                }
                let is_only_attempt = order.attempts.len() == 1;
                if is_only_attempt
                    && matches!(
                        order.status,
                        OrderStatus::Pending
                            | OrderStatus::AwaitingPayment
                            | OrderStatus::Underpaid
                    )
                {
                    (Status::Set(OrderStatus::Failed), stranded_funds(&next))
                } else {
                    (Status::Set(order.status), vec![])
                }
            }

            // Late non-funding event on a finished order: ack, change nothing.
            _ if ended => (Status::Set(order.status), vec![]),

            ev => {
                return Err(MachineError::Illegal {
                    from: order.status,
                    event: ev.name(),
                })
            }
        };

        next.status = match status {
            Status::Derive => derive_status(&next)?,
            Status::Set(s) => s,
        };

        next.updated_at = now;

        Ok(ApplyResult {
            effects: to_outbox(&key, next.id, raw_effects),
            order: next,
            key,
        })
    }

    /// Explicit command. `Fulfilled` is absorbing in `derive_status` via
    /// `fulfilled_at`, so this is gated on both status and money: an outbox
    /// worker draining a stale `MayFulfill` must not be able to mark an
    /// underfunded order shipped.
    pub fn mark_fulfilled(
        &self,
        order: &Order,
        now: OffsetDateTime,
    ) -> Result<ApplyResult, MachineError> {
        if !matches!(order.status, OrderStatus::Paid | OrderStatus::Overpaid) {
            return Err(MachineError::FulfillRejected { why: "status is not Paid/Overpaid" });
        }
        if order.net()?.cmp_amount(&order.due)? == Ordering::Less {
            return Err(MachineError::FulfillRejected { why: "net below due" });
        }

        let key = order_key(order, EventKind::Fulfill, format!("fulfill:{}", order.id));
        let mut next = order.clone();
        next.fulfilled_at = Some(now);
        next.status = derive_status(&next)?;
        next.updated_at = now;

        Ok(ApplyResult { effects: to_outbox(&key, next.id, vec![]), order: next, key })
    }

    /// Time-driven transitions the rails will not send. Own `EventKind` and
    /// an order-scoped `tx_ref`, so two pre-observation orders expiring in
    /// the same sweep cannot collide on the unique index.
    pub fn on_clock(&self, order: &Order, now: OffsetDateTime) -> Option<ApplyResult> {
        let mut next = order.clone();

        if matches!(
            order.status,
            OrderStatus::Pending | OrderStatus::AwaitingPayment | OrderStatus::Underpaid
        ) && order.expires_at.is_some_and(|t| now >= t)
        {
            next.status = OrderStatus::Expired;

            let effects = stranded_funds(order);

            let key = order_key(order, EventKind::Clock, format!("expire:{}", order.id));
            next.updated_at = now;
            return Some(ApplyResult { effects: to_outbox(&key, next.id, effects), order: next, key });
        }

        let mut promoted: Vec<(String, String)> = Vec::new();
        for a in next.attempts.iter_mut() {
            if a.chargeback_window_ends.is_some_and(|end| now >= end)
                && !matches!(a.finality, Some(Finality::Final))
            {
                a.finality = Some(Finality::Final);
                promoted.push((a.provider.clone(), a.provider_invoice_id.clone()));
            }
        }
        if promoted.is_empty() {
            return None;
        }
        promoted.sort();

        let effects = if next.fulfilled_at.is_some() {
            vec![Effect::MarkSettled]
        } else if matches!(
            order.status,
            OrderStatus::Paid | OrderStatus::Overpaid | OrderStatus::Fulfilled
        ) && next
            .attempts
            .iter()
            .filter(|a| a.is_funding())
            .all(|a| self.policy.may_fulfill(&a.provider, &a.finality_or_none()))
        {
            vec![Effect::MayFulfill]
        } else {
            vec![]
        };

        let pairs: Vec<(&str, &str)> =
            promoted.iter().map(|(p, i)| (p.as_str(), i.as_str())).collect();
        let key = order_key(order, EventKind::Clock, promoted_set_ref(&pairs));
        next.updated_at = now;
        Some(ApplyResult { effects: to_outbox(&key, next.id, effects), order: next, key })
    }

    /// What follows from the order's current funding state alone, with no
    /// reference to which event produced it. Called from both a fresh
    /// observation and a won dispute, so the decision cannot depend on
    /// which path arrived here.
    fn funding_effects(&self, order: &Order) -> Result<Vec<Effect>, MachineError> {
        let net = order.net()?;
        let funding = funding_status(&net, &order.due)?;
        let mut fx = Vec::new();
        match funding {
            OrderStatus::AwaitingPayment | OrderStatus::Underpaid => {
                fx.push(Effect::HoldFulfillment { why: "underpaid".into() });
                fx.push(Effect::AwaitRemainder {
                    missing: order.due.sub(&net.clamp_zero())?.clamp_zero(),
                });
            }
            OrderStatus::Paid | OrderStatus::Overpaid => {
                if order.dispute_open {
                    fx.push(Effect::HoldFulfillment { why: "dispute still open".into() });
                } else if order.fulfilled_at.is_some() {
                    // Already shipped; a deeper confirmation is not a re-ship.
                    fx.push(Effect::MarkSettled);
                } else if order
                    .attempts
                    .iter()
                    .filter(|a| a.is_funding())
                    .all(|a| self.policy.may_fulfill(&a.provider, &a.finality_or_none()))
                {
                    fx.push(Effect::MayFulfill);
                } else {
                    fx.push(Effect::HoldFulfillment {
                        why: "policy rejected finality on a funding attempt".into(),
                    });
                }

                // Only the part not yet refunded, allocated across attempts
                // in store order so the same ledger state always produces
                // the same `refund_id`s.
                let mut remaining = order.outstanding_excess()?;
                for a in &order.attempts {
                    if !remaining.is_positive() {
                        break;
                    }
                    let refundable = a.refundable()?;
                    if !refundable.is_positive() {
                        continue;
                    }
                    let send = a.to_rail_currency(&remaining)?.min_of(&refundable)?;
                    if !send.is_positive() {
                        continue;
                    }
                    let refund_id = refund_excess_id(
                        order.id,
                        &a.provider,
                        &a.provider_invoice_id,
                        &remaining,
                        &a.net()?,
                        &a.refunded_total,
                    );
                    fx.push(Effect::RefundExcess {
                        provider: a.provider.clone(),
                        provider_invoice_id: a.provider_invoice_id.clone(),
                        amount: send.clone(),
                        refund_id,
                    });
                    remaining = remaining.sub(&a.to_order_currency(&send)?)?.clamp_zero();
                }
            }
            _ => {}
        }
        Ok(fx)
    }
}

fn order_key(order: &Order, kind: EventKind, tx_ref: String) -> IdempotencyKey {
    IdempotencyKey {
        order_id: order.id,
        provider: String::new(),
        kind,
        provider_invoice_id: String::new(),
        tx_ref,
    }
}

fn join_finality(current: Option<&Finality>, incoming: &Finality) -> Finality {
    match current {
        Some(c) => c.join(incoming),
        None => incoming.clone(),
    }
}
