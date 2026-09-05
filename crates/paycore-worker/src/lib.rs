//! Outbox drain and clock tick.
//!
//! Both loops are pure translation from an [`OutboxEntry`] or a due order id
//! to store calls: no branch here decides *whether* money moves, only
//! *what to do next* with a decision the machine already made and
//! committed. A `MayFulfill` row can go stale between the event that queued
//! it and the drain that reads it — a later reversal can undercut the
//! order in between — so `mark_fulfilled` is re-checked here rather than
//! trusted from the outbox alone.

use paycore::{
    ingest, CommitResult, Effect, FulfillmentPolicy, MachineError, OrderCatalog, OrderMachine,
    OutboxEntry, PayError, PaymentBackend, PersistError, RefundRequest, Settlement,
};
use time::OffsetDateTime;

#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    #[error(transparent)]
    Persist(#[from] PersistError),
    #[error(transparent)]
    Machine(#[from] MachineError),
    #[error(transparent)]
    Pay(#[from] PayError),
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DrainStats {
    pub fulfilled: u32,
    pub stale: u32,
    pub refunded: u32,
    /// Left in the claim window on purpose: the provider said `Unavailable`,
    /// which is transient by contract, so the next pass tries again.
    pub retries: u32,
    /// Parked for a human and taken out of the claim window.
    pub held: u32,
    pub acked: u32,
}

pub async fn drain_once<B, P, S>(
    backend: &B,
    machine: &OrderMachine<P>,
    store: &S,
    now: OffsetDateTime,
    limit: usize,
) -> Result<DrainStats, WorkerError>
where
    B: PaymentBackend,
    P: FulfillmentPolicy,
    S: OrderCatalog,
{
    let mut stats = DrainStats::default();

    // Scoped to this backend's rail. An unscoped row is an order-level
    // instruction any worker may take; a scoped one belongs to the worker
    // holding that provider's backend, or `refund` goes to the wrong rail and
    // the `Refunded` that follows is dead-lettered for a provider mismatch —
    // leaving `refunded_total` behind and the same excess refunded twice.
    for entry in store.pending_outbox(backend.name(), limit).await? {
        let OutboxEntry { id, order_id, effect, .. } = entry;

        match effect {
            Effect::MayFulfill => {
                let order = store.load(order_id).await?;
                match machine.mark_fulfilled(&order, now) {
                    Ok(result) => {
                        store.commit(result).await?;
                        store.mark_drained(id, now).await?;
                        stats.fulfilled += 1;
                    }
                    Err(MachineError::FulfillRejected { .. }) => {
                        store.mark_drained(id, now).await?;
                        stats.stale += 1;
                    }
                    Err(e) => return Err(e.into()),
                }
            }

            Effect::RefundExcess { provider, provider_invoice_id, amount, refund_id } => {
                let req = RefundRequest {
                    refund_id,
                    provider: provider.clone(),
                    provider_invoice_id: provider_invoice_id.clone(),
                    amount: amount.clone(),
                    reason: Some("excess".to_string()),
                };
                match backend.refund(req).await {
                    Ok(()) | Err(PayError::RefundIdempotent(_)) => {
                        let ev = Settlement::Refunded {
                            order_id,
                            provider,
                            provider_invoice_id,
                            tx_ref: refund_id.to_string(),
                            refund_id,
                            amount,
                            at: now,
                        };
                        ingest(machine, store, backend.name(), &[ev], &[], now).await?;
                        store.mark_drained(id, now).await?;
                        stats.refunded += 1;
                    }
                    // Transient by contract: leave the row for the next pass.
                    Err(PayError::Unavailable) => {
                        stats.retries += 1;
                    }
                    // Anything else will fail again on every retry — an amount
                    // the driver cannot express, a rejected request — so it is
                    // parked rather than left to abort the loop and starve
                    // every row queued behind it.
                    Err(e) => {
                        store.hold_row(id, now, format!("refund failed: {e}")).await?;
                        stats.held += 1;
                    }
                }
            }

            // Never drained: money arrived against an order that had already
            // ended, and a human has to decide what happens to it. Held so it
            // stops occupying a slot in every future claim window.
            Effect::UnexpectedFunds { provider, provider_invoice_id, .. } => {
                store
                    .hold_row(
                        id,
                        now,
                        format!("unexpected funds on {provider}/{provider_invoice_id}"),
                    )
                    .await?;
                stats.held += 1;
            }

            _ => {
                store.mark_drained(id, now).await?;
                stats.acked += 1;
            }
        }
    }

    Ok(stats)
}

pub async fn tick_clock<P, S>(
    machine: &OrderMachine<P>,
    store: &S,
    now: OffsetDateTime,
) -> Result<u32, WorkerError>
where
    P: FulfillmentPolicy,
    S: OrderCatalog,
{
    let mut n = 0u32;

    for id in store.ids_needing_clock(now).await? {
        let order = store.load(id).await?;
        if let Some(result) = machine.on_clock(&order, now) {
            match store.commit(result).await? {
                CommitResult::Applied => n += 1,
                CommitResult::Duplicate => {}
            }
        }
    }

    Ok(n)
}
