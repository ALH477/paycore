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

        let order = match store.load(ev.order_id()).await {
            Ok(o) => o,
            Err(PersistError::UnknownOrder(id)) => return Err(PersistError::UnknownOrder(id)),
            Err(e) => return Err(e),
        };

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
