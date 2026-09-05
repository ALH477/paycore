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

    let first = pollster::block_on(store.pending_outbox(P, 32)).unwrap();
    assert!(!first.is_empty());
    let id = first[0].id;
    pollster::block_on(store.mark_drained(id, t(11))).unwrap();

    let second = pollster::block_on(store.pending_outbox(P, 32)).unwrap();
    assert!(second.iter().all(|e| e.id != id));
}

/// F15 (HIGH): `SCHEMA` was bare `CREATE TABLE` and `open` ran it on every
/// call, so a file-backed store could be opened exactly once — every process
/// restart died with "table orders already exists". Invisible to the suite
/// because every other test here uses `open_in_memory`.
#[test]
fn sqlite_file_store_survives_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("paycore.db");
    let path = path.to_str().unwrap();

    {
        let store = SqliteStore::open(path).unwrap();
        pollster::block_on(OrderCatalog::insert(&store, seed())).unwrap();
    }

    let store = SqliteStore::open(path).expect("reopening an existing database must work");
    let order = pollster::block_on(store.load(oid())).unwrap();
    assert_eq!(order.due, usd(100));
    assert_eq!(order.attempts.len(), 1, "attempts survive the reopen too");
}

/// F15: foreign keys default OFF in SQLite, which would leave every
/// REFERENCES clause in the schema decorative.
#[test]
fn sqlite_enforces_foreign_keys() {
    let store = SqliteStore::open_in_memory().unwrap();
    let ev = obs(100, "tx-a");
    // No order row was ever inserted, so the processed_events FK has nothing
    // to point at and the whole commit must fail rather than orphan a row.
    let r = pollster::block_on(store.commit(m().apply(&seed(), &ev, t(10)).unwrap()));
    assert!(r.is_err(), "a commit against a missing order row must not succeed");
}

/// F29: `orders.status` round-tripped through `serde_json`, so a `text` column
/// held `"\"Paid\""` — quotes and all — while `processed_events.kind` used the
/// stable `as_str()` token. It could not be filtered in SQL, which is why the
/// clock sweep had to load every order in the table to find the due ones.
#[test]
fn sqlite_stores_status_as_a_plain_token() {
    let store = SqliteStore::open_in_memory().unwrap();
    pollster::block_on(OrderCatalog::insert(&store, seed())).unwrap();
    pollster::block_on(ingest(&m(), &store, P, &[obs(100, "tx-a")], b"", t(10))).unwrap();
    assert_eq!(store.status_token(oid()).unwrap(), "Paid");
}

/// F27: the clock sweep used to select every order in the table and issue a
/// `load_order` per row. It now asks SQL for the candidates — and must still
/// return exactly the orders that are actually due.
#[test]
fn sqlite_clock_sweep_selects_only_due_orders() {
    let store = SqliteStore::open_in_memory().unwrap();
    let mut expiring = seed();
    expiring.expires_at = Some(t(50));
    pollster::block_on(OrderCatalog::insert(&store, expiring)).unwrap();

    let mut quiet = Order::new(Uuid::from_u128(0x5555), usd(100), t(0));
    quiet.open_attempt(Attempt::same_currency(P, "inv-quiet", usd(100)).unwrap()).unwrap();
    quiet.status = OrderStatus::AwaitingPayment;
    pollster::block_on(OrderCatalog::insert(&store, quiet)).unwrap();

    assert!(pollster::block_on(store.ids_needing_clock(t(40))).unwrap().is_empty());
    let due = pollster::block_on(store.ids_needing_clock(t(60))).unwrap();
    assert_eq!(due, vec![oid()], "only the order whose deadline has passed");
}
