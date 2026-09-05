use std::sync::{Arc, Mutex};

use paycore::{
    ingest, Attempt, CreateInvoice, Finality, Invoice, MemoryStore, Money, Order, OrderCatalog,
    OrderMachine, OrderStatus, OrderStore, PayError, PaymentBackend, RefundRequest, ReversalKind,
    ReversalReason, Settlement, StaticPolicy, VerifiedBody,
};
use paycore_worker::{drain_once, tick_clock};
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

const USD: &str = "USD";
const P: &str = "btcpay";

fn t(s: i64) -> OffsetDateTime {
    OffsetDateTime::UNIX_EPOCH + Duration::seconds(s)
}

fn oid() -> Uuid {
    Uuid::from_u128(0x2222_2222_2222_2222_2222_2222_2222_2222)
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

struct FakeBackend {
    name: &'static str,
    refunds: Arc<Mutex<Vec<RefundRequest>>>,
    fail_refunds: bool,
}

#[async_trait::async_trait]
impl PaymentBackend for FakeBackend {
    fn name(&self) -> &'static str {
        self.name
    }

    async fn create_invoice(&self, _req: CreateInvoice) -> Result<Invoice, PayError> {
        unimplemented!("not exercised by the drain worker")
    }

    async fn verify(
        &self,
        _headers: &[(String, String)],
        _body: &[u8],
    ) -> Result<VerifiedBody, PayError> {
        unimplemented!("not exercised by the drain worker")
    }

    fn decode(&self, _body: &VerifiedBody) -> Result<Vec<Settlement>, PayError> {
        Ok(vec![])
    }

    async fn refund(&self, req: RefundRequest) -> Result<(), PayError> {
        if self.fail_refunds {
            return Err(PayError::Unavailable);
        }
        self.refunds.lock().unwrap().push(req);
        Ok(())
    }

    async fn fetch_settlements(&self, _since: OffsetDateTime) -> Result<Vec<Settlement>, PayError> {
        Ok(vec![])
    }
}

fn backend(fail_refunds: bool) -> (FakeBackend, Arc<Mutex<Vec<RefundRequest>>>) {
    let refunds = Arc::new(Mutex::new(Vec::new()));
    (FakeBackend { name: P, refunds: refunds.clone(), fail_refunds }, refunds)
}

#[test]
fn drain_may_fulfill_marks_order_fulfilled() {
    let store = MemoryStore::new();
    store.insert(order(100)).unwrap();
    let mm = m();
    let (be, _refunds) = backend(false);

    pollster::block_on(ingest(&mm, &store, P, &[obs(100, "tx-a")], b"", t(10))).unwrap();
    pollster::block_on(drain_once(&be, &mm, &store, t(11), 32)).unwrap();

    let o = pollster::block_on(store.load(oid())).unwrap();
    assert_eq!(o.status, OrderStatus::Fulfilled);
    assert!(o.fulfilled_at.is_some());

    pollster::block_on(drain_once(&be, &mm, &store, t(12), 32)).unwrap();
    let o = pollster::block_on(store.load(oid())).unwrap();
    assert_eq!(o.status, OrderStatus::Fulfilled);
}

#[test]
fn drain_stale_may_fulfill_does_not_ship_underpaid() {
    let store = MemoryStore::new();
    store.insert(order(100)).unwrap();
    let mm = m();
    let (be, _refunds) = backend(false);

    pollster::block_on(ingest(&mm, &store, P, &[obs(100, "tx-a")], b"", t(10))).unwrap();
    pollster::block_on(ingest(&mm, &store, P, &[rev(100, "r1")], b"", t(20))).unwrap();

    pollster::block_on(drain_once(&be, &mm, &store, t(21), 32)).unwrap();

    let o = pollster::block_on(store.load(oid())).unwrap();
    assert!(o.fulfilled_at.is_none());

    let remaining = pollster::block_on(store.pending_outbox("btcpay", 32)).unwrap();
    assert!(
        !remaining.iter().any(|e| matches!(e.effect, paycore::Effect::MayFulfill)),
        "stale MayFulfill must be drained, not left for a retry to ship"
    );
}

#[test]
fn drain_refund_excess_is_idempotent_on_refund_id() {
    let store = MemoryStore::new();
    store.insert(order(100)).unwrap();
    let mm = m();
    let (be, refunds) = backend(false);

    pollster::block_on(ingest(&mm, &store, P, &[obs(150, "tx-a")], b"", t(10))).unwrap();
    pollster::block_on(drain_once(&be, &mm, &store, t(11), 32)).unwrap();

    assert_eq!(refunds.lock().unwrap().len(), 1);
    let o = pollster::block_on(store.load(oid())).unwrap();
    assert_eq!(o.outstanding_excess().unwrap(), usd(0));

    pollster::block_on(drain_once(&be, &mm, &store, t(12), 32)).unwrap();
    assert_eq!(refunds.lock().unwrap().len(), 1);
}

#[test]
fn drain_refund_unavailable_leaves_outbox() {
    let store = MemoryStore::new();
    store.insert(order(100)).unwrap();
    let mm = m();
    let (be, refunds) = backend(true);

    pollster::block_on(ingest(&mm, &store, P, &[obs(150, "tx-a")], b"", t(10))).unwrap();
    pollster::block_on(drain_once(&be, &mm, &store, t(11), 32)).unwrap();

    assert!(refunds.lock().unwrap().is_empty());

    let remaining = pollster::block_on(store.pending_outbox("btcpay", 32)).unwrap();
    assert!(remaining.iter().any(|e| matches!(e.effect, paycore::Effect::RefundExcess { .. })));
}

#[test]
fn tick_clock_expires_and_records_unexpected_funds() {
    let store = MemoryStore::new();
    let mut o = order(100);
    o.expires_at = Some(t(50));
    store.insert(o).unwrap();
    let mm = m();

    pollster::block_on(ingest(&mm, &store, P, &[obs(60, "tx-a")], b"", t(10))).unwrap();
    let n = pollster::block_on(tick_clock(&mm, &store, t(60))).unwrap();
    assert_eq!(n, 1);

    let o = pollster::block_on(store.load(oid())).unwrap();
    assert_eq!(o.status, OrderStatus::Expired);

    let outbox = store.outbox();
    assert!(outbox.iter().any(|e| matches!(e.effect, paycore::Effect::UnexpectedFunds { .. })));
}

/// F13 (HIGH): `UnexpectedFunds` is never drained by design — a human has to
/// look — but `pending_outbox` is a `LIMIT n` window over undrained rows. An
/// undrained row occupies a slot in *every* future window, so `n` of them
/// permanently starve every `MayFulfill` and `RefundExcess` queued behind them.
/// Fulfilment and refunds stop dead and nothing errors while it happens:
/// `drain_once` returns `held: n` and exits clean.
#[test]
fn held_rows_do_not_starve_the_outbox() {
    const LIMIT: usize = 4;
    let store = MemoryStore::new();
    let mm = m();
    let (be, _) = backend(false);

    // Fill the whole claim window with rows that will never be drained: an
    // order that ended, then late payments landing against it.
    let mut ended = order(100);
    ended.status = OrderStatus::Expired;
    pollster::block_on(OrderCatalog::insert(&store, ended)).unwrap();
    for i in 0..LIMIT {
        let ev = obs(10 * (i as i64 + 1), &format!("late-{i}"));
        pollster::block_on(ingest(&mm, &store, P, &[ev], b"", t(20 + i as i64))).unwrap();
    }
    let stats = pollster::block_on(drain_once(&be, &mm, &store, t(30), LIMIT)).unwrap();
    assert_eq!(stats.held, LIMIT as u32, "every late payment needs a human");
    assert_eq!(store.held().len(), LIMIT, "and each is parked, not merely counted");

    // A fully-funded order arrives behind them. Its MayFulfill must still be
    // reachable: the held rows are out of the way, not gone.
    let store2 = MemoryStore::new();
    let mut ended2 = order(100);
    ended2.status = OrderStatus::Expired;
    pollster::block_on(OrderCatalog::insert(&store2, ended2)).unwrap();
    for i in 0..LIMIT {
        let ev = obs(10 * (i as i64 + 1), &format!("late-{i}"));
        pollster::block_on(ingest(&mm, &store2, P, &[ev], b"", t(20 + i as i64))).unwrap();
    }
    pollster::block_on(drain_once(&be, &mm, &store2, t(30), LIMIT)).unwrap();

    let live = Uuid::from_u128(0x3333);
    let mut o = Order::new(live, usd(100), t(0));
    o.open_attempt(Attempt::same_currency(P, "inv-live", usd(100)).unwrap()).unwrap();
    o.status = OrderStatus::AwaitingPayment;
    pollster::block_on(OrderCatalog::insert(&store2, o)).unwrap();
    let pay = Settlement::Observed {
        order_id: live,
        provider: P.into(),
        provider_invoice_id: "inv-live".into(),
        observed_total: usd(100),
        tx_ref: "pay".into(),
        finality: Finality::Provisional { confirmations: 6 },
        at: t(40),
    };
    pollster::block_on(ingest(&mm, &store2, P, &[pay], b"", t(40))).unwrap();

    let stats = pollster::block_on(drain_once(&be, &mm, &store2, t(50), LIMIT)).unwrap();
    assert_eq!(stats.fulfilled, 1, "a held row must not block the order behind it");
    let shipped = pollster::block_on(store2.load(live)).unwrap();
    assert_eq!(shipped.status, OrderStatus::Fulfilled);
}

/// F19 (MEDIUM): `drain_once` never checked that a row belonged to the backend
/// it was handed. In a multi-provider deployment it would POST provider A's
/// refund to backend B; the follow-up `ingest` stamps `backend.name()`, so the
/// `Refunded` is dead-lettered for a provider mismatch, `refunded_total` never
/// advances, and the same excess is refunded again the moment it changes.
#[test]
fn a_worker_does_not_drain_another_providers_refund() {
    let store = MemoryStore::new();
    let mm = m();
    pollster::block_on(OrderCatalog::insert(&store, order(100))).unwrap();
    // Overpay on the "btcpay" invoice, producing a btcpay-scoped RefundExcess.
    pollster::block_on(ingest(&mm, &store, P, &[obs(150, "tx1")], b"", t(10))).unwrap();
    assert!(
        store.outbox().iter().any(|e| matches!(e.effect, paycore::Effect::RefundExcess { .. })),
        "the overpay must queue a refund"
    );

    // A worker for a different rail must not touch it.
    let other = FakeBackend {
        name: "stripe",
        refunds: Arc::new(Mutex::new(Vec::new())),
        fail_refunds: false,
    };
    let seen = other.refunds.clone();
    let stats = pollster::block_on(drain_once(&other, &mm, &store, t(20), 32)).unwrap();
    assert_eq!(stats.refunded, 0);
    assert!(seen.lock().unwrap().is_empty(), "stripe must not be asked to refund a btcpay invoice");
    assert_eq!(store.dead_letters().len(), 0, "and nothing is dead-lettered in the process");

    // The rail that owns it still drains it.
    let (mine, mine_seen) = backend(false);
    let stats = pollster::block_on(drain_once(&mine, &mm, &store, t(30), 32)).unwrap();
    assert_eq!(stats.refunded, 1);
    assert_eq!(mine_seen.lock().unwrap().len(), 1);
}
