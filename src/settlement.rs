//! What a driver reports, and how those reports are named uniquely.

use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::money::Money;

/// Namespace for derived ids. Stable across restarts and retries.
///
/// A private namespace, deliberately: this was previously the well-known
/// RFC 4122 DNS namespace, which any other system also derives ids in.
/// Changing it changes every derived outbox id and every `refund_id`, so it
/// is free before the first production row exists and a migration after.
const NS_DERIVED: Uuid = Uuid::from_u128(0x8c345deb9c6f4e0bae10404da6916596);

/// Upper bound on provider-supplied identifiers. These become columns in a
/// btree primary key; an unbounded one fails the index insert at commit
/// time, which turns into an unbounded provider retry loop.
pub const MAX_ID_LEN: usize = 200;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Finality {
    Provisional {
        confirmations: u32,
    },
    /// Cards are never `Final` from a webhook. Only `on_clock` promotes
    /// them, once the chargeback window closes.
    Final,
}

impl Finality {
    /// Monotone join. Confirmations never decrease; `Final` absorbs.
    pub fn join(&self, incoming: &Finality) -> Finality {
        match (self, incoming) {
            (Finality::Final, _) | (_, Finality::Final) => Finality::Final,
            (
                Finality::Provisional { confirmations: a },
                Finality::Provisional { confirmations: b },
            ) => Finality::Provisional { confirmations: (*a).max(*b) },
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReversalKind {
    AchReturn,
    Chargeback,
    Reorg,
    IssuerFreeze,
    ProviderClawback,
    Other,
}

impl ReversalKind {
    /// Kinds that close the invoice once net reaches zero. An ACH return or
    /// a reorg leaves it open so the customer can pay again.
    pub fn is_closing(&self) -> bool {
        matches!(self, ReversalKind::Chargeback | ReversalKind::IssuerFreeze)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReversalReason {
    pub kind: ReversalKind,
    /// Structured code where the rail has one (R10, 13.1). Empty otherwise.
    pub code: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EventKind {
    Observed,
    Reversed,
    Refunded,
    Disputed,
    DisputeResolved,
    Fulfill,
    Expired,
    Failed,
    Clock,
}

impl EventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            EventKind::Observed => "Observed",
            EventKind::Reversed => "Reversed",
            EventKind::Refunded => "Refunded",
            EventKind::Disputed => "Disputed",
            EventKind::DisputeResolved => "DisputeResolved",
            EventKind::Fulfill => "Fulfill",
            EventKind::Expired => "Expired",
            EventKind::Failed => "Failed",
            EventKind::Clock => "Clock",
        }
    }

    pub fn from_str_exact(s: &str) -> Option<Self> {
        match s {
            "Observed" => Some(EventKind::Observed),
            "Reversed" => Some(EventKind::Reversed),
            "Refunded" => Some(EventKind::Refunded),
            "Disputed" => Some(EventKind::Disputed),
            "DisputeResolved" => Some(EventKind::DisputeResolved),
            "Fulfill" => Some(EventKind::Fulfill),
            "Expired" => Some(EventKind::Expired),
            "Failed" => Some(EventKind::Failed),
            "Clock" => Some(EventKind::Clock),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Settlement {
    /// Cumulative funds observed against the invoice.
    ///
    /// `observed_total` is the running total the provider has seen for this
    /// invoice, **not** the amount of `tx_ref`. `tx_ref` names the
    /// triggering transaction and exists only to namespace the key.
    Observed {
        order_id: Uuid,
        provider: String,
        provider_invoice_id: String,
        observed_total: Money,
        tx_ref: String,
        finality: Finality,
        at: OffsetDateTime,
    },
    /// `amount` is the size of *this* clawback — a delta. Different
    /// `tx_ref`s stack. Does not by itself close the invoice.
    Reversed {
        order_id: Uuid,
        provider: String,
        provider_invoice_id: String,
        tx_ref: String,
        amount: Money,
        reason: ReversalReason,
        at: OffsetDateTime,
    },
    /// A refund the driver actually executed. Feeds `refunded_total` so
    /// excess refunds stay idempotent when the excess itself changes.
    /// Without this the machine re-derives a fresh instruction every time
    /// an overpay grows and refunds the difference twice.
    Refunded {
        order_id: Uuid,
        provider: String,
        provider_invoice_id: String,
        tx_ref: String,
        refund_id: Uuid,
        amount: Money,
        at: OffsetDateTime,
    },
    Disputed {
        order_id: Uuid,
        provider: String,
        provider_invoice_id: String,
        tx_ref: String,
        amount: Money,
        deadline: OffsetDateTime,
        at: OffsetDateTime,
    },
    /// Distinct from `Observed`. A confirmation-depth webhook must not
    /// resolve a chargeback.
    DisputeResolved {
        order_id: Uuid,
        provider: String,
        provider_invoice_id: String,
        tx_ref: String,
        won: bool,
        at: OffsetDateTime,
    },
    Expired {
        order_id: Uuid,
        provider: String,
        provider_invoice_id: String,
        at: OffsetDateTime,
    },
    Failed {
        order_id: Uuid,
        provider: String,
        provider_invoice_id: String,
        /// Stable token, not free text. Drivers map provider strings here.
        code: String,
        at: OffsetDateTime,
    },
}

impl Settlement {
    pub fn order_id(&self) -> Uuid {
        match self {
            Settlement::Observed { order_id, .. }
            | Settlement::Reversed { order_id, .. }
            | Settlement::Refunded { order_id, .. }
            | Settlement::Disputed { order_id, .. }
            | Settlement::DisputeResolved { order_id, .. }
            | Settlement::Expired { order_id, .. }
            | Settlement::Failed { order_id, .. } => *order_id,
        }
    }

    /// MUST be the driver's own `name()`, never a field parsed out of the
    /// webhook body. It is part of the unique index, so a payload-derived
    /// value lets an attacker vary it to defeat duplicate suppression —
    /// and `reversed_total` sums, so replayed clawbacks compound.
    /// `ingest` rejects any event whose provider disagrees with the backend.
    pub fn provider(&self) -> &str {
        match self {
            Settlement::Observed { provider, .. }
            | Settlement::Reversed { provider, .. }
            | Settlement::Refunded { provider, .. }
            | Settlement::Disputed { provider, .. }
            | Settlement::DisputeResolved { provider, .. }
            | Settlement::Expired { provider, .. }
            | Settlement::Failed { provider, .. } => provider,
        }
    }

    pub fn provider_invoice_id(&self) -> &str {
        match self {
            Settlement::Observed { provider_invoice_id, .. }
            | Settlement::Reversed { provider_invoice_id, .. }
            | Settlement::Refunded { provider_invoice_id, .. }
            | Settlement::Disputed { provider_invoice_id, .. }
            | Settlement::DisputeResolved { provider_invoice_id, .. }
            | Settlement::Expired { provider_invoice_id, .. }
            | Settlement::Failed { provider_invoice_id, .. } => provider_invoice_id,
        }
    }

    pub fn tx_ref(&self) -> &str {
        match self {
            Settlement::Observed { tx_ref, .. }
            | Settlement::Reversed { tx_ref, .. }
            | Settlement::Refunded { tx_ref, .. }
            | Settlement::Disputed { tx_ref, .. }
            | Settlement::DisputeResolved { tx_ref, .. } => tx_ref,
            Settlement::Expired { .. } | Settlement::Failed { .. } => "",
        }
    }

    /// The value that names this event uniquely within its
    /// `(order_id, provider, kind, provider_invoice_id)` group — what goes
    /// into the index's `tx_ref` column.
    ///
    /// For every kind but one that is `tx_ref`. A refund is named by its
    /// `refund_id`: that is the identity `PaymentBackend::refund` is
    /// idempotent on, and a provider replaying an already-executed refund
    /// commonly reports it under a fresh transaction reference. Keyed on
    /// `tx_ref` those two reports are distinct rows, and `refunded_total`
    /// sums, so one refund is booked twice and the excess still owed to the
    /// payer silently disappears.
    pub fn key_ref(&self) -> Cow<'_, str> {
        match self {
            Settlement::Refunded { refund_id, .. } => Cow::Owned(refund_id.to_string()),
            ev => Cow::Borrowed(ev.tx_ref()),
        }
    }

    pub fn kind(&self) -> EventKind {
        match self {
            Settlement::Observed { .. } => EventKind::Observed,
            Settlement::Reversed { .. } => EventKind::Reversed,
            Settlement::Refunded { .. } => EventKind::Refunded,
            Settlement::Disputed { .. } => EventKind::Disputed,
            Settlement::DisputeResolved { .. } => EventKind::DisputeResolved,
            Settlement::Expired { .. } => EventKind::Expired,
            Settlement::Failed { .. } => EventKind::Failed,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Settlement::Observed { .. } => "Observed",
            Settlement::Reversed { .. } => "Reversed",
            Settlement::Refunded { .. } => "Refunded",
            Settlement::Disputed { .. } => "Disputed",
            Settlement::DisputeResolved { .. } => "DisputeResolved",
            Settlement::Expired { .. } => "Expired",
            Settlement::Failed { .. } => "Failed",
        }
    }

    /// Structural only. No `Debug` dumps, no reason strings.
    pub fn idempotency_key(&self) -> IdempotencyKey {
        IdempotencyKey {
            order_id: self.order_id(),
            provider: self.provider().to_string(),
            kind: self.kind(),
            provider_invoice_id: self.provider_invoice_id().to_string(),
            tx_ref: self.key_ref().into_owned(),
        }
    }
}

/// Unique index: `(order_id, provider, kind, provider_invoice_id, tx_ref)`.
///
/// `order_id` leads because clock and fulfil events have no provider fields
/// before the first observation — without it, every pre-observation expiry
/// in the system collapses onto one row.
///
/// `tx_ref` holds [`Settlement::key_ref`], not [`Settlement::tx_ref`]. The
/// two differ only for `Refunded`, which is keyed on its `refund_id`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IdempotencyKey {
    pub order_id: Uuid,
    pub provider: String,
    pub kind: EventKind,
    pub provider_invoice_id: String,
    pub tx_ref: String,
}

/// Length-prefixed field join. A plain separator is not injective: with
/// `|`, invoice `"c|d"` + tx `"t"` and invoice `"c"` + tx `"d|t"` hash to
/// the same value, so two distinct events would share an outbox row id.
fn canonical(parts: &[&str]) -> Vec<u8> {
    let mut out = Vec::new();
    for p in parts {
        out.extend_from_slice(&(p.len() as u64).to_be_bytes());
        out.extend_from_slice(p.as_bytes());
    }
    out
}

impl IdempotencyKey {
    /// Outbox row id. Stable across `apply` retries.
    ///
    /// `purpose` is a slice rather than one string because an effect may be
    /// scoped to an attempt, and the parts must stay separately delimited:
    /// joining them by hand would reintroduce exactly the non-injectivity
    /// that `canonical` exists to prevent.
    pub fn derived_uuid(&self, purpose: &[&str]) -> Uuid {
        let kind = format!("{:?}", self.kind);
        let oid = self.order_id.to_string();
        let mut parts: Vec<&str> =
            vec![&oid, &self.provider, &kind, &self.provider_invoice_id, &self.tx_ref];
        parts.extend_from_slice(purpose);
        Uuid::new_v5(&NS_DERIVED, &canonical(&parts))
    }
}

/// Names the set of attempts a clock tick promoted to `Final`.
///
/// The parts are provider-supplied, so they go through [`canonical`] rather
/// than a separator join: with `","`, the sets `["a,b"]` and `["a", "b"]`
/// produce one string, the second promotion commits as `Duplicate`, and the
/// finality promotion is silently lost — the attempt never reaches `Final` and
/// the order never ships. Hashing also bounds the result, which a join does
/// not: N attempts times `MAX_ID_LEN` outgrows the `tx_ref` column, and a key
/// the machine mints itself never passes through `validate`.
///
/// `provider` is included because an invoice id is only unique within one.
pub(crate) fn promoted_set_ref(pairs: &[(&str, &str)]) -> String {
    let mut parts: Vec<&str> = Vec::with_capacity(pairs.len() * 2 + 1);
    parts.push("window-closed");
    for (provider, invoice) in pairs {
        parts.push(provider);
        parts.push(invoice);
    }
    format!("window-closed:{}", Uuid::new_v5(&NS_DERIVED, &canonical(&parts)))
}

/// Identifies "the refund that sends `amount` back through this invoice".
///
/// Keyed on everything the amount is a pure function of, and nothing else.
/// The amount is `min(order_excess, attempt_net - attempt_refunded)`, so all
/// three go in. Keying on the amount alone breaks both ways: recomputing the
/// same excess after a dispute win would mint a second id, and a *growing*
/// overpay whose delta happened to equal the previous one would reuse an id
/// the driver has already consumed. Keying on the attempt alone breaks too —
/// funds landing on a second invoice change this invoice's share without
/// changing anything about the invoice itself.
pub fn refund_excess_id(
    order_id: Uuid,
    provider: &str,
    provider_invoice_id: &str,
    order_excess: &Money,
    attempt_net: &Money,
    attempt_refunded: &Money,
) -> Uuid {
    let oid = order_id.to_string();
    let excess_s = order_excess.minor.to_string();
    let net_s = attempt_net.minor.to_string();
    let done_s = attempt_refunded.minor.to_string();
    Uuid::new_v5(
        &NS_DERIVED,
        &canonical(&[
            "refund-excess",
            &oid,
            provider,
            provider_invoice_id,
            &excess_s,
            &order_excess.currency,
            &net_s,
            &done_s,
            &attempt_net.currency,
        ]),
    )
}

#[cfg(test)]
mod tests {
    use super::EventKind;

    #[test]
    fn event_kind_as_str_is_stable() {
        use EventKind::*;
        for (kind, token) in [
            (Observed, "Observed"),
            (Reversed, "Reversed"),
            (Refunded, "Refunded"),
            (Disputed, "Disputed"),
            (DisputeResolved, "DisputeResolved"),
            (Fulfill, "Fulfill"),
            (Expired, "Expired"),
            (Failed, "Failed"),
            (Clock, "Clock"),
        ] {
            assert_eq!(kind.as_str(), token);
        }
    }
}
