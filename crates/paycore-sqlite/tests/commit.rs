use paycore::{
    ingest, Attempt, Finality, Money, Order, OrderCatalog, OrderMachine, OrderStatus, OrderStore,
    PersistError, ReversalKind, ReversalReason, Settlement, StaticPolicy,
};
use paycore_sqlite::SqliteStore;
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
fn sqlite_duplicate_reversal_does_not_double_the_sum() {
    let store = SqliteStore::open_in_memory().unwrap();
    pollster::block_on(store.insert(seed())).unwrap();
    let mm = m();
    pollster::block_on(ingest(&mm, &store, P, &[obs(100, "tx-a")], b"", t(10))).unwrap();
    pollster::block_on(ingest(&mm, &store, P, &[rev(40, "r1")], b"", t(20))).unwrap();
    pollster::block_on(ingest(&mm, &store, P, &[rev(40, "r1")], b"", t(21))).unwrap();

    let o = pollster::block_on(store.load(oid())).unwrap();
    let a = o.attempt(P, "inv-1").unwrap();
    assert_eq!(a.reversed_total, usd(40), "unique index must drop the replay");
    assert_eq!(o.net().unwrap(), usd(60));
    assert_eq!(store.dead_letter_count(), 0);
}

#[test]
fn sqlite_unknown_order_is_not_dead_lettered() {
    let store = SqliteStore::open_in_memory().unwrap();
    let err = pollster::block_on(ingest(&m(), &store, P, &[obs(100, "tx-a")], b"raw", t(10)));
    assert!(matches!(err, Err(PersistError::UnknownOrder(_))));
    assert_eq!(store.dead_letter_count(), 0);
}

#[test]
fn sqlite_load_returns_attempts_in_insert_order() {
    let store = SqliteStore::open_in_memory().unwrap();
    let mut o = Order::new(oid(), usd(100), t(0));
    o.open_attempt(Attempt::same_currency("a", "inv-A", usd(40)).unwrap()).unwrap();
    o.open_attempt(Attempt::same_currency("b", "inv-B", usd(60)).unwrap()).unwrap();
    pollster::block_on(store.insert(o)).unwrap();

    let loaded = pollster::block_on(store.load(oid())).unwrap();
    let ids: Vec<_> = loaded.attempts.iter().map(|a| a.provider_invoice_id.as_str()).collect();
    assert_eq!(ids, ["inv-A", "inv-B"]);
}

#[test]
fn sqlite_claimed_outbox_skips_drained_rows() {
    let store = SqliteStore::open_in_memory().unwrap();
    pollster::block_on(store.insert(seed())).unwrap();
    let mm = m();
    pollster::block_on(ingest(&mm, &store, P, &[obs(100, "tx-a")], b"", t(10))).unwrap();

    let first = pollster::block_on(store.claim_outbox(32)).unwrap();
    assert!(!first.is_empty());
    let id = first[0].id;
    pollster::block_on(store.mark_drained(id, t(11))).unwrap();

    let second = pollster::block_on(store.claim_outbox(32)).unwrap();
    assert!(second.iter().all(|e| e.id != id));
}
