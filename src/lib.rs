//! Provider-agnostic payment orchestration.
//!
//! Invariants
//! ----------
//! 1. **PCI.** PAN, track data, and CVV never cross the driver boundary.
//!    Hosted fields, network tokens, or a redirect only. Every deployment
//!    of this crate stays in SAQ-A scope, and a driver that breaks that is
//!    out of contract.
//! 2. **Drivers observe, they do not classify.** A driver reports
//!    `observed_total`; under, exact, and over are computed here from
//!    `due`, so a driver cannot forge a classification.
//! 3. **Authentication is a type.** `decode` consumes a
//!    [`VerifiedBody`](backend::VerifiedBody), which can only be built by a
//!    constructor that performs a constant-time comparison or requires the
//!    driver to name the external scheme it used.
//! 4. **`apply` is pure.** No clock, no RNG, no I/O. It takes `&Order` and
//!    returns a new one, so an error cannot publish a partial mutation.
//!    Every derived id is a UUIDv5 over canonical, length-prefixed input.
//! 5. **Idempotency is a unique index** on
//!    `(order_id, provider, kind, provider_invoice_id, tx_ref)`, written in
//!    the same transaction as the order row and the outbox rows.
//! 6. **Money is three monotone accumulators**, never one field moving both
//!    ways: `observed_total` max-joins, `reversed_total` and
//!    `refunded_total` only add. `net = observed − reversed`. A stale
//!    snapshot therefore cannot resurrect reversed funds.
//! 7. **Effects are outbox rows**, committed with the order. A worker
//!    drains them, so a crash between decision and action loses nothing.
//!
//! What commutes and what does not
//! -------------------------------
//! The money accumulators are a lattice: permuting any set of *accepted*
//! events yields the same totals. `observed_total` is also idempotent;
//! `reversed_total` and `refunded_total` deliberately are not — summing is
//! monotone, not idempotent, and duplicate suppression belongs to the
//! store's unique index. Folding it into `apply` would mean the aggregate
//! carries a seen-set, which is an in-process `HashSet` with extra steps.
//!
//! Status is a fold over an ordered log and is path-dependent by design:
//! it is derived from the accumulators plus monotone flags
//! (`reversal_closed`, `dispute_open`, `fulfilled_at`), and terminal states
//! stop accepting progress. Do not assert permutation-invariance on status.

// A crate that decides where money goes has no business reaching for `unsafe`.
// `forbid` rather than `deny` so that no module can opt back in locally.
#![forbid(unsafe_code)]

pub mod backend;
pub mod machine;
pub mod memory;
pub mod money;
pub mod order;
pub mod policy;
pub mod settlement;
pub mod store;

pub use backend::{CreateInvoice, Invoice, PayError, PaymentBackend, RefundRequest, VerifiedBody};
pub use machine::{ApplyResult, MachineError, OrderMachine};
pub use memory::{DeadLetter, MemoryStore};
pub use money::Money;
pub use order::{Attempt, Effect, Order, OrderStatus, OutboxEntry};
pub use policy::{FulfillmentPolicy, StaticPolicy};
pub use settlement::{
    refund_excess_id, EventKind, Finality, IdempotencyKey, ReversalKind, ReversalReason,
    Settlement, MAX_ID_LEN,
};
pub use store::{
    ingest, on_webhook, reconcile, CommitResult, OrderCatalog, OrderStore, PersistError,
};

#[cfg(test)]
mod tests;
