//! The aggregate.
//!
//! An order is a fold over its payment attempts. Each attempt is one invoice
//! on one rail and carries its own three monotone accumulators, its own
//! finality, and its own chargeback clock. Status is derived from that fold
//! plus a small set of monotone flags, never latched in an arm.

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::machine::MachineError;
use crate::money::Money;
use crate::settlement::{Finality, IdempotencyKey, ReversalReason};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderStatus {
    Pending,
    AwaitingPayment,
    Paid,
    Overpaid,
    Underpaid,
    Fulfilled,
    /// Shipped, then the funding went away without a closing reversal —
    /// an ACH return or a partial clawback. Not terminal: the customer can
    /// still make it whole.
    Recalled,
    Disputed,
    Reversed,
    Expired,
    Failed,
}

impl OrderStatus {
    /// Nothing further changes these. Funds arriving against one are still
    /// recorded, but as `UnexpectedFunds`, not as progress.
    pub fn is_closed(&self) -> bool {
        matches!(self, OrderStatus::Expired | OrderStatus::Failed)
    }
}

/// One invoice, on one rail.
///
/// An order has several whenever it is re-invoiced after an expiry, fails
/// over to another processor, or is part-paid on two rails. None of what an
/// attempt holds composes across invoices, which is why it lives here rather
/// than on the order:
///
/// - `observed_total` is the provider's *cumulative total for that invoice*.
///   Max-joining two invoices' totals into one field keeps the larger and
///   silently discards the smaller.
/// - Finality is per-rail. A Lightning payment being instantly `Final` says
///   nothing about an on-chain one sitting at two confirmations.
/// - The chargeback clock is per-rail: 180 days for a card, none at all for
///   Lightning.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attempt {
    pub provider: String,
    pub provider_invoice_id: String,

    /// What this invoice must collect, in the rail's own currency.
    pub quoted: Money,
    /// What collecting `quoted` in full is worth against [`Order::due`], in
    /// the order's currency.
    ///
    /// `covers / quoted` *is* the exchange rate, locked when the invoice was
    /// created. The machine never consults a live rate: `apply` is pure, and
    /// a ledger whose totals depend on the rate at read time is not
    /// reproducible from its own event log. For a same-currency rail the two
    /// are equal and every conversion below is the identity.
    pub covers: Money,

    /// Max-joined. Never decreases. Rail currency.
    pub observed_total: Money,
    /// Summed. Never decreases. Rail currency.
    pub reversed_total: Money,
    /// Summed. Never decreases. Refunds the driver actually executed.
    pub refunded_total: Money,

    pub finality: Option<Finality>,
    pub last_tx_ref: Option<String>,
    /// When this *invoice* dies. Distinct from [`Order::expires_at`]: a late
    /// payment against a dead invoice still funds a live order.
    pub expires_at: Option<OffsetDateTime>,
    pub chargeback_window_ends: Option<OffsetDateTime>,
}

impl Attempt {
    /// A cross-currency attempt: collect `quoted` on the rail, worth `covers`
    /// against the order. Both are fixed here and never recomputed.
    pub fn new(
        provider: impl Into<String>,
        provider_invoice_id: impl Into<String>,
        quoted: Money,
        covers: Money,
    ) -> Result<Self, MachineError> {
        if quoted.minor <= 0 {
            return Err(MachineError::InvalidAttempt { why: "quoted must be positive" });
        }
        if covers.minor < 0 {
            return Err(MachineError::InvalidAttempt { why: "covers must not be negative" });
        }
        let rail = quoted.currency.clone();
        Ok(Self {
            provider: provider.into(),
            provider_invoice_id: provider_invoice_id.into(),
            quoted,
            covers,
            observed_total: Money::zero(rail.clone()),
            reversed_total: Money::zero(rail.clone()),
            refunded_total: Money::zero(rail),
            finality: None,
            last_tx_ref: None,
            expires_at: None,
            chargeback_window_ends: None,
        })
    }

    /// The common case: the rail settles in the order's own currency, so the
    /// rate is the identity.
    pub fn same_currency(
        provider: impl Into<String>,
        provider_invoice_id: impl Into<String>,
        amount: Money,
    ) -> Result<Self, MachineError> {
        Self::new(provider, provider_invoice_id, amount.clone(), amount)
    }

    pub fn matches(&self, provider: &str, provider_invoice_id: &str) -> bool {
        self.provider == provider && self.provider_invoice_id == provider_invoice_id
    }

    /// observed − reversed, in the rail's currency. May go negative when
    /// reconcile starts partway through an invoice's history.
    pub fn net(&self) -> Result<Money, MachineError> {
        self.observed_total.sub(&self.reversed_total)
    }

    /// Rail currency → order currency, through this attempt's locked rate.
    pub fn to_order_currency(&self, rail: &Money) -> Result<Money, MachineError> {
        rail.scale_to(&self.covers, &self.quoted)
    }

    /// Order currency → rail currency, through the same locked rate. Floored,
    /// so an amount that does not reach one minor unit of the rail's currency
    /// converts to zero rather than being rounded up into existence.
    pub fn to_rail_currency(&self, order: &Money) -> Result<Money, MachineError> {
        order.scale_to(&self.quoted, &self.covers)
    }

    /// What this attempt contributes to the order, in the order's currency.
    pub fn contribution(&self) -> Result<Money, MachineError> {
        self.to_order_currency(&self.net()?)
    }

    /// Funds still held under this attempt that could be sent back: net less
    /// what has already been refunded. Rail currency.
    pub fn refundable(&self) -> Result<Money, MachineError> {
        Ok(self.net()?.sub(&self.refunded_total)?.clamp_zero())
    }

    /// Whether this attempt is holding money, and so has to satisfy its
    /// rail's finality policy before the order can ship.
    pub fn is_funding(&self) -> bool {
        self.net().map(|n| n.is_positive()).unwrap_or(false)
    }

    pub fn finality_or_none(&self) -> Finality {
        self.finality.clone().unwrap_or(Finality::Provisional { confirmations: 0 })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Order {
    pub id: Uuid,
    /// Derived on every event by `derive_status`. Never assigned directly
    /// except by the provider-declared endings.
    pub status: OrderStatus,
    /// What the customer owes, in the order's currency. Attempts convert
    /// into this currency through their own locked rate.
    pub due: Money,

    /// One per invoice, in creation order. The store MUST return them in a
    /// stable order: refund allocation walks this list, so a shuffled one
    /// would produce a different `refund_id` for the same ledger state.
    pub attempts: Vec<Attempt>,

    /// Monotone: a closing-kind reversal has been seen, on any attempt.
    pub saw_closing_reversal: bool,
    /// Monotone latch: closing reversal *and* net reached zero. Latched so
    /// that funds arriving later against a closed-out order cannot un-close
    /// it by making the live comparison false again.
    pub reversal_closed: bool,
    pub dispute_open: bool,

    /// The order's own deadline, which is what `on_clock` expires. An
    /// individual invoice dying is `Attempt::expires_at` and does not end
    /// the order.
    pub expires_at: Option<OffsetDateTime>,
    pub fulfilled_at: Option<OffsetDateTime>,
    pub updated_at: OffsetDateTime,
}

impl Order {
    pub fn new(id: Uuid, due: Money, now: OffsetDateTime) -> Self {
        let mut o = Self {
            id,
            status: OrderStatus::Pending,
            due,
            attempts: Vec::new(),
            saw_closing_reversal: false,
            reversal_closed: false,
            dispute_open: false,
            expires_at: None,
            fulfilled_at: None,
            updated_at: now,
        };
        o.status = derive_status(&o).unwrap_or(OrderStatus::Pending);
        o
    }

    /// Register an invoice against this order. Call it when `create_invoice`
    /// succeeds, and commit it with the order — a driver's webhooks cannot be
    /// applied until the attempt they name exists.
    pub fn open_attempt(&mut self, attempt: Attempt) -> Result<(), MachineError> {
        if attempt.covers.currency != self.due.currency {
            return Err(MachineError::CurrencyMismatch {
                expected: self.due.currency.clone(),
                actual: attempt.covers.currency.clone(),
            });
        }
        if self
            .attempts
            .iter()
            .any(|a| a.matches(&attempt.provider, &attempt.provider_invoice_id))
        {
            return Err(MachineError::InvalidAttempt { why: "attempt already open" });
        }
        self.attempts.push(attempt);
        Ok(())
    }

    pub fn attempt(&self, provider: &str, provider_invoice_id: &str) -> Option<&Attempt> {
        self.attempts.iter().find(|a| a.matches(provider, provider_invoice_id))
    }

    pub(crate) fn attempt_mut(
        &mut self,
        provider: &str,
        provider_invoice_id: &str,
    ) -> Result<&mut Attempt, MachineError> {
        self.attempts
            .iter_mut()
            .find(|a| a.matches(provider, provider_invoice_id))
            .ok_or_else(|| MachineError::UnknownAttempt {
                provider: provider.to_string(),
                provider_invoice_id: provider_invoice_id.to_string(),
            })
    }

    /// Fold a per-attempt rail-currency quantity into the order's currency.
    fn fold<F>(&self, pick: F) -> Result<Money, MachineError>
    where
        F: Fn(&Attempt) -> Result<Money, MachineError>,
    {
        let mut total = Money::zero(self.due.currency.clone());
        for a in &self.attempts {
            total = total.add(&a.to_order_currency(&pick(a)?)?)?;
        }
        Ok(total)
    }

    /// Everything ever observed, in the order's currency. Monotone.
    pub fn observed(&self) -> Result<Money, MachineError> {
        self.fold(|a| Ok(a.observed_total.clone()))
    }

    /// observed − reversed, summed across attempts, in the order's currency.
    pub fn net(&self) -> Result<Money, MachineError> {
        self.fold(|a| a.net())
    }

    /// Refunds actually executed, in the order's currency.
    pub fn refunded(&self) -> Result<Money, MachineError> {
        self.fold(|a| Ok(a.refunded_total.clone()))
    }

    /// Excess still owed back to the payer: `net − due − refunded`.
    pub fn outstanding_excess(&self) -> Result<Money, MachineError> {
        let net = self.net()?;
        if net.cmp_amount(&self.due)? != Ordering::Greater {
            return Ok(Money::zero(self.due.currency.clone()));
        }
        Ok(net.sub(&self.due)?.sub(&self.refunded()?)?.clamp_zero())
    }
}

/// `net` against `due`, with no reference to how the order got there.
///
/// The `net <= 0` short-circuit is guarded on `due` so that a zero-price
/// order is `Paid` on arrival rather than stuck `AwaitingPayment` for a
/// payment that is never coming.
pub fn funding_status(net: &Money, due: &Money) -> Result<OrderStatus, MachineError> {
    let ord = net.cmp_amount(due)?;
    if net.minor <= 0 && ord == Ordering::Less {
        return Ok(OrderStatus::AwaitingPayment);
    }
    Ok(match ord {
        Ordering::Less => OrderStatus::Underpaid,
        Ordering::Equal => OrderStatus::Paid,
        Ordering::Greater => OrderStatus::Overpaid,
    })
}

/// Status is a function of accumulated state, not a decision made inside
/// whichever arm happened to run last. Re-evaluated after every event, so
/// the observation that brings net to zero after a chargeback closes the
/// order exactly as the chargeback that arrives after payment does.
pub fn derive_status(o: &Order) -> Result<OrderStatus, MachineError> {
    if o.status.is_closed() {
        return Ok(o.status);
    }
    if o.reversal_closed {
        return Ok(OrderStatus::Reversed);
    }
    if o.dispute_open {
        return Ok(OrderStatus::Disputed);
    }
    let net = o.net()?;
    if o.fulfilled_at.is_some() {
        return Ok(if net.cmp_amount(&o.due)? == Ordering::Less {
            OrderStatus::Recalled
        } else {
            OrderStatus::Fulfilled
        });
    }
    funding_status(&net, &o.due)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Effect {
    MayFulfill,
    HoldFulfillment { why: String },
    RecallFulfillment { why: String },
    /// Worker calls `PaymentBackend::refund` with this id against this
    /// invoice, then emits `Settlement::Refunded` on success so
    /// `refunded_total` advances. `amount` is in the rail's currency —
    /// that is the only currency the driver can actually send.
    RefundExcess {
        provider: String,
        provider_invoice_id: String,
        amount: Money,
        refund_id: Uuid,
    },
    AwaitRemainder { missing: Money },
    OpenDispute { deadline: OffsetDateTime, amount: Money },
    RecordLoss { reason: ReversalReason, amount: Money },
    MarkSettled,
    /// Money arrived against an order that had already ended, or was left
    /// stranded on one that expired part-paid. Never silent: a late on-chain
    /// payment after invoice expiry is routine, and the customer's funds must
    /// not vanish from the record.
    UnexpectedFunds {
        provider: String,
        provider_invoice_id: String,
        observed: Money,
    },
}

impl Effect {
    pub fn purpose(&self) -> &'static str {
        match self {
            Effect::MayFulfill => "may-fulfill",
            Effect::HoldFulfillment { .. } => "hold",
            Effect::RecallFulfillment { .. } => "recall",
            Effect::RefundExcess { .. } => "refund-excess",
            Effect::AwaitRemainder { .. } => "await",
            Effect::OpenDispute { .. } => "dispute",
            Effect::RecordLoss { .. } => "loss",
            Effect::MarkSettled => "settled",
            Effect::UnexpectedFunds { .. } => "unexpected-funds",
        }
    }

    /// The attempt an effect is scoped to, if any.
    ///
    /// One event can now produce several effects of the same purpose — a
    /// refund against each of two overpaid invoices, say. Without the scope
    /// in the derived id they would collapse onto one outbox row and only
    /// one of the two refunds would ever be sent.
    pub fn scope(&self) -> Option<(&str, &str)> {
        match self {
            Effect::RefundExcess { provider, provider_invoice_id, .. }
            | Effect::UnexpectedFunds { provider, provider_invoice_id, .. } => {
                Some((provider, provider_invoice_id))
            }
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct OutboxEntry {
    pub id: Uuid,
    pub order_id: Uuid,
    pub idempotency_key: IdempotencyKey,
    pub effect: Effect,
}

pub(crate) fn to_outbox(
    key: &IdempotencyKey,
    order_id: Uuid,
    effects: Vec<Effect>,
) -> Vec<OutboxEntry> {
    effects
        .into_iter()
        .map(|effect| {
            let id = match effect.scope() {
                Some((p, inv)) => key.derived_uuid(&[effect.purpose(), p, inv]),
                None => key.derived_uuid(&[effect.purpose()]),
            };
            OutboxEntry { id, order_id, idempotency_key: key.clone(), effect }
        })
        .collect()
}
