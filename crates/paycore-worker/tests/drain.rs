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

    let remaining = pollster::block_on(store.claim_outbox(32)).unwrap();
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

    let remaining = pollster::block_on(store.claim_outbox(32)).unwrap();
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
