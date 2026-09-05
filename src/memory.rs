//! Reference `OrderStore`. The unique index and the one-transaction commit
//! live here so a real database cannot invent a second protocol.
//!
//! Not a production database. Clone the order out, hold one mutex across
//! index insert + order write + outbox insert, roll the whole thing back
//! on a duplicate key. That is the contract `commit` documents in SQL.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use async_trait::async_trait;
use uuid::Uuid;

use crate::machine::ApplyResult;
use crate::order::{Order, OutboxEntry};
use crate::settlement::{IdempotencyKey, Settlement};
use crate::store::{CommitResult, OrderStore, PersistError};

#[derive(Clone, Debug)]
pub struct DeadLetter {
    pub order_id: Uuid,
    pub event: Settlement,
    pub raw: Vec<u8>,
    pub why: String,
}

struct Inner {
    orders: HashMap<Uuid, Order>,
    processed: HashSet<IdempotencyKey>,
    outbox: Vec<OutboxEntry>,
    dead: Vec<DeadLetter>,
}

pub struct MemoryStore {
    inner: Mutex<Inner>,
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                orders: HashMap::new(),
                processed: HashSet::new(),
                outbox: Vec::new(),
                dead: Vec::new(),
            }),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Seed an order. Invoices must already be `open_attempt`ed — `apply`
    /// will not create them. Duplicate id is an error, not an upsert.
    pub fn insert(&self, order: Order) -> Result<(), PersistError> {
        let mut g = self.lock();
        if g.orders.contains_key(&order.id) {
            return Err(PersistError::Other(anyhow::anyhow!("order {} already exists", order.id)));
        }
        g.orders.insert(order.id, order);
        Ok(())
    }

    pub fn outbox(&self) -> Vec<OutboxEntry> {
        self.lock().outbox.clone()
    }

    pub fn dead_letters(&self) -> Vec<DeadLetter> {
        self.lock().dead.clone()
    }
}

#[async_trait]
impl OrderStore for MemoryStore {
    async fn load(&self, id: Uuid) -> Result<Order, PersistError> {
        self.lock().orders.get(&id).cloned().ok_or(PersistError::UnknownOrder(id))
    }

    async fn commit(&self, result: ApplyResult) -> Result<CommitResult, PersistError> {
        let mut g = self.lock();
        if g.processed.contains(&result.key) {
            return Ok(CommitResult::Duplicate);
        }
        if !g.orders.contains_key(&result.order.id) {
            return Err(PersistError::UnknownOrder(result.order.id));
        }
        g.processed.insert(result.key);
        g.outbox.extend(result.effects);
        g.orders.insert(result.order.id, result.order);
        Ok(CommitResult::Applied)
    }

    async fn dead_letter(
        &self,
        event: &Settlement,
        raw: &[u8],
        why: String,
    ) -> Result<(), PersistError> {
        self.lock().dead.push(DeadLetter {
            order_id: event.order_id(),
            event: event.clone(),
            raw: raw.to_vec(),
            why,
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order::{Attempt, OrderStatus};
    use crate::settlement::{Finality, ReversalKind, ReversalReason};
    use crate::*;
    use time::{Duration, OffsetDateTime};

    const USD: &str = "USD";
    const P: &str = "btcpay";

    fn t(s: i64) -> OffsetDateTime {
        OffsetDateTime::UNIX_EPOCH + Duration::seconds(s)
    }
    fn oid() -> Uuid {
        Uuid::from_u128(0x1111_1111_1111_1111_1111_1111_1111_1111)
    }
    fn usd(n: i64) -> Money {
        Money::new(n, USD)
    }
    fn m() -> OrderMachine<StaticPolicy> {
        OrderMachine::new(StaticPolicy::new(1, None))
    }
    fn order(due: i64) -> Order {
        let mut o = Order::new(oid(), usd(due), t(0));
        o.open_attempt(Attempt::same_currency(P, "inv-1", usd(due)).unwrap()).unwrap();
        o.status = OrderStatus::AwaitingPayment;
        o
    }
    fn obs(total: i64, tx: &str) -> Settlement {
        Settlement::Observed {
            order_id: oid(),
            provider: P.into(),
            provider_invoice_id: "inv-1".into(),
            observed_total: usd(total),
            tx_ref: tx.into(),
            finality: Finality::Provisional { confirmations: 6 },
            at: t(10),
        }
    }
    fn rev(amount: i64, tx: &str) -> Settlement {
        Settlement::Reversed {
            order_id: oid(),
            provider: P.into(),
            provider_invoice_id: "inv-1".into(),
            tx_ref: tx.into(),
            amount: usd(amount),
            reason: ReversalReason { kind: ReversalKind::AchReturn, code: "R01".into() },
            at: t(20),
        }
    }

    #[test]
    fn duplicate_reversal_does_not_double_the_sum() {
        let store = MemoryStore::new();
        store.insert(order(100)).unwrap();
        let mm = m();
        pollster::block_on(ingest(&mm, &store, P, &[obs(100, "tx-a")], b"", t(10))).unwrap();
        pollster::block_on(ingest(&mm, &store, P, &[rev(40, "r1")], b"", t(20))).unwrap();
        pollster::block_on(ingest(&mm, &store, P, &[rev(40, "r1")], b"", t(21))).unwrap();
        let o = pollster::block_on(store.load(oid())).unwrap();
        let a = o.attempt(P, "inv-1").unwrap();
        assert_eq!(a.reversed_total, usd(40), "unique index must drop the replay");
        assert_eq!(o.net().unwrap(), usd(60));
        assert_eq!(store.dead_letters().len(), 0);
    }

    #[test]
    fn duplicate_commit_leaves_outbox_untouched() {
        let store = MemoryStore::new();
        store.insert(order(100)).unwrap();
        let mm = m();
        pollster::block_on(ingest(&mm, &store, P, &[obs(100, "tx-a")], b"", t(10))).unwrap();
        let n = store.outbox().len();
        assert!(n > 0, "first apply must emit");
        pollster::block_on(ingest(&mm, &store, P, &[obs(100, "tx-a")], b"", t(11))).unwrap();
        assert_eq!(store.outbox().len(), n);
        let o = pollster::block_on(store.load(oid())).unwrap();
        assert_eq!(o.observed().unwrap(), usd(100));
    }

    #[test]
    fn unknown_order_is_not_dead_lettered() {
        let store = MemoryStore::new();
        let err = pollster::block_on(ingest(&m(), &store, P, &[obs(100, "tx-a")], b"raw", t(10)));
        assert!(matches!(err, Err(PersistError::UnknownOrder(_))));
        assert!(store.dead_letters().is_empty());
    }

    #[test]
    fn load_returns_attempts_in_insert_order() {
        let store = MemoryStore::new();
        let mut o = Order::new(oid(), usd(100), t(0));
        o.open_attempt(Attempt::same_currency("a", "inv-A", usd(40)).unwrap()).unwrap();
        o.open_attempt(Attempt::same_currency("b", "inv-B", usd(60)).unwrap()).unwrap();
        store.insert(o).unwrap();
        let loaded = pollster::block_on(store.load(oid())).unwrap();
        let ids: Vec<_> =
            loaded.attempts.iter().map(|a| a.provider_invoice_id.as_str()).collect();
        assert_eq!(ids, ["inv-A", "inv-B"]);
    }
}
