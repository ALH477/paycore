use std::sync::{Arc, Mutex};

use paycore::order::{Attempt, Order, OrderStatus};
use paycore::*;
use paycore_sqlite::SqliteStore;
use paycore_worker::drain_once;
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

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
fn seed() -> Order {
    let mut o = Order::new(oid(), usd(100), t(0));
    o.open_attempt(Attempt::same_currency(P, "inv-1", usd(100)).unwrap()).unwrap();
    o.status = OrderStatus::AwaitingPayment;
    o
}

struct FakeBackend {
    name: &'static str,
    refunds: Arc<Mutex<Vec<RefundRequest>>>,
}

#[async_trait::async_trait]
impl PaymentBackend for FakeBackend {
    fn name(&self) -> &'static str {
        self.name
    }
    async fn create_invoice(&self, _req: CreateInvoice) -> Result<Invoice, PayError> {
        unimplemented!("not used")
    }
    async fn verify(
        &self,
        _h: &[(String, String)],
        _b: &[u8],
    ) -> Result<VerifiedBody, PayError> {
        unimplemented!("not used")
    }
    fn decode(&self, _b: &VerifiedBody) -> Result<Vec<Settlement>, PayError> {
        Ok(vec![])
    }
    async fn refund(&self, req: RefundRequest) -> Result<(), PayError> {
        self.refunds.lock().unwrap().push(req);
        Ok(())
    }
    async fn fetch_settlements(
        &self,
        _since: OffsetDateTime,
    ) -> Result<Vec<Settlement>, PayError> {
        Ok(vec![])
    }
}

#[test]
fn sqlite_overpay_drain_refunds_once() {
    let store = SqliteStore::open_in_memory().unwrap();
    pollster::block_on(store.insert(seed())).unwrap();
    let mm = m();
    let refunds = Arc::new(Mutex::new(Vec::new()));
    let be = FakeBackend { name: P, refunds: refunds.clone() };

    let obs = Settlement::Observed {
        order_id: oid(),
        provider: P.into(),
        provider_invoice_id: "inv-1".into(),
        observed_total: usd(150),
        tx_ref: "tx-a".into(),
        finality: Finality::Provisional { confirmations: 6 },
        at: t(10),
    };
    pollster::block_on(ingest(&mm, &store, P, &[obs], b"", t(10))).unwrap();
    pollster::block_on(drain_once(&be, &mm, &store, t(11), 32)).unwrap();

    let o = pollster::block_on(store.load(oid())).unwrap();
    assert_eq!(o.outstanding_excess().unwrap(), usd(0));
    assert_eq!(refunds.lock().unwrap().len(), 1);

    pollster::block_on(drain_once(&be, &mm, &store, t(12), 32)).unwrap();
    assert_eq!(refunds.lock().unwrap().len(), 1);
}
