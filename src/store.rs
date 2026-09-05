//! Persistence port and the ingest loop.
//!
//! Duplicate suppression lives in the unique index and nowhere else.
//! `reversed_total` and `refunded_total` are sums, so replaying one event
//! twice would double the loss or the refund — the index is what prevents
//! that, not a seen-set on the aggregate. Putting one there would be the
//! in-process `HashSet` again, relocated into the order row.

use async_trait::async_trait;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::backend::{PayError, PaymentBackend};
use crate::machine::{ApplyResult, OrderMachine};
use crate::order::Order;
use crate::policy::FulfillmentPolicy;
use crate::settlement::{Settlement, MAX_ID_LEN};

#[derive(Debug, thiserror::Error)]
pub enum PersistError {
    #[error("unknown order {0}")]
    UnknownOrder(Uuid),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitResult {
    Applied,
    /// Unique index hit. Order and outbox were not mutated.
    Duplicate,
}

#[async_trait]
pub trait OrderStore: Send + Sync {
    async fn load(&self, id: Uuid) -> Result<Order, PersistError>;

    /// ```sql
    /// BEGIN;
    ///   INSERT INTO processed_events
    ///     (order_id, provider, kind, provider_invoice_id, tx_ref)
    ///     VALUES (...);            -- PK over exactly those five;
    ///                              -- tx_ref is `Settlement::key_ref()`,
    ///                              -- which is the refund_id for refunds
    ///                              -- conflict => Duplicate, roll back
    ///   UPDATE orders SET ...;
    ///   -- replace attempts for this order_id, in `seq` order
    ///   INSERT INTO outbox (...);
    /// COMMIT;
    /// ```
    async fn commit(&self, result: ApplyResult) -> Result<CommitResult, PersistError>;

    /// `raw` is the original transport bytes where there are any, so a
    /// dead-lettered event can be replayed from source rather than from a
    /// parse that may itself be the bug. Empty for reconcile.
    async fn dead_letter(
        &self,
        event: &Settlement,
        raw: &[u8],
        why: String,
    ) -> Result<(), PersistError>;
}

#[async_trait]
pub trait OrderCatalog: OrderStore {
    async fn insert(&self, order: Order) -> Result<(), PersistError>;

    /// Undrained, unheld rows this worker is allowed to act on, oldest first.
    ///
    /// A read, not a lease — it takes no lock and marks nothing, so two workers
    /// running concurrently against one store will both see the same rows. That
    /// is survivable because every action behind it is idempotent
    /// (`mark_fulfilled` is gated on the order's own state; `refund` is
    /// idempotent on `refund_id`), but it is not a work queue and the name no
    /// longer claims to be one.
    ///
    /// `provider` scopes the claim: a row carrying an [`Effect::scope`] is
    /// claimable only by the worker holding that rail's backend, because
    /// `drain_once` will otherwise send provider A's refund to backend B. Rows
    /// with no scope are order-level instructions and any worker may take them
    /// — `mark_fulfilled` is gated on the order's own state, so a second
    /// worker reaching the same row changes nothing.
    ///
    /// [`Effect::scope`]: crate::order::Effect::scope
    async fn pending_outbox(
        &self,
        provider: &str,
        limit: usize,
    ) -> Result<Vec<crate::order::OutboxEntry>, PersistError>;

    async fn mark_drained(&self, id: Uuid, at: OffsetDateTime) -> Result<(), PersistError>;

    /// Take a row out of the claim window *without* marking it done.
    ///
    /// `pending_outbox` is a `LIMIT n` window over undrained rows, so a row that
    /// is never drained occupies a slot in every future window. `UnexpectedFunds`
    /// is never drained by design — a human has to look — so `n` of them
    /// permanently starve every `MayFulfill` and `RefundExcess` behind them:
    /// fulfilment and refunds stop dead, and nothing errors while it happens.
    ///
    /// A held row is still there, still undrained, and still the human's work
    /// queue. It is only out of the *worker's* way.
    async fn hold_row(
        &self,
        id: Uuid,
        at: OffsetDateTime,
        why: String,
    ) -> Result<(), PersistError>;

    async fn ids_needing_clock(&self, now: OffsetDateTime) -> Result<Vec<Uuid>, PersistError>;
}

/// Rejects events a driver should never have produced, before they reach
/// the machine or the index.
fn validate(event: &Settlement, expected_provider: &str) -> Result<(), String> {
    if event.provider() != expected_provider {
        // `provider` is index material. A driver that fills it from the
        // payload rather than from `name()` lets an attacker vary it to
        // defeat duplicate suppression, and sums compound on replay.
        return Err(format!(
            "provider {:?} does not match backend {:?}",
            event.provider(),
            expected_provider
        ));
    }
    for (field, value) in [
        ("provider", event.provider()),
        ("provider_invoice_id", event.provider_invoice_id()),
        ("tx_ref", event.tx_ref()),
    ] {
        if value.len() > MAX_ID_LEN {
            // These are btree primary-key columns. An oversized one fails
            // the index insert at commit, which becomes an endless
            // provider retry loop.
            return Err(format!("{field} exceeds {MAX_ID_LEN} bytes"));
        }
    }
    Ok(())
}

/// One bad event does not fail the batch. An illegal transition, a currency
/// mismatch, or a provider mismatch is a data problem — retries will never
/// fix it, so it is dead-lettered rather than surfaced as a 5xx. Transient
/// store failures do propagate, because a retry is exactly what those want.
pub async fn ingest<P, S>(
    machine: &OrderMachine<P>,
    store: &S,
    expected_provider: &str,
    events: &[Settlement],
    raw: &[u8],
    now: OffsetDateTime,
) -> Result<(), PersistError>
where
    P: FulfillmentPolicy,
    S: OrderStore,
{
    for ev in events {
        if let Err(why) = validate(ev, expected_provider) {
            store.dead_letter(ev, raw, why).await?;
            continue;
        }

        // Every store failure propagates, `UnknownOrder` included: a webhook
        // racing invoice creation must surface as an error the provider will
        // retry, not a swallowed 2xx and not a dead letter.
        let order = store.load(ev.order_id()).await?;

        match machine.apply(&order, ev, now) {
            Ok(result) => match store.commit(result).await? {
                CommitResult::Applied | CommitResult::Duplicate => {}
            },
            Err(e) => store.dead_letter(ev, raw, e.to_string()).await?,
        }
    }
    Ok(())
}

pub async fn on_webhook<B, P, S>(
    backend: &B,
    machine: &OrderMachine<P>,
    store: &S,
    headers: &[(String, String)],
    body: &[u8],
    now: OffsetDateTime,
) -> Result<(), PayError>
where
    B: PaymentBackend,
    P: FulfillmentPolicy,
    S: OrderStore,
{
    let verified = backend.verify(headers, body).await?;
    let events = backend.decode(&verified)?;
    ingest(machine, store, backend.name(), &events, body, now)
        .await
        .map_err(|e| PayError::Other(e.into()))
}

pub async fn reconcile<B, P, S>(
    backend: &B,
    machine: &OrderMachine<P>,
    store: &S,
    since: OffsetDateTime,
    now: OffsetDateTime,
) -> Result<(), PayError>
where
    B: PaymentBackend,
    P: FulfillmentPolicy,
    S: OrderStore,
{
    let events = backend.fetch_settlements(since).await?;
    ingest(machine, store, backend.name(), &events, &[], now)
        .await
        .map_err(|e| PayError::Other(e.into()))
}
