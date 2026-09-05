//! Regressions for the audit findings, one per finding (`F<n>`). These used
//! to be proof-of-concept probes that demonstrated a bug by asserting the
//! *buggy* behaviour; each has since been inverted to assert the fix, so
//! every test in this file is expected to pass. C1 (negative deltas) and H2
//! (refunds keyed on refund_id) were fixed earlier and their probes already
//! live in `src/tests.rs` as regressions.
use async_trait::async_trait;
use paycore::order::{derive_status, Attempt};
use paycore::*;
use std::sync::atomic::{AtomicUsize, Ordering as AOrd};
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

/// A `due > 0` order carries the invoice every helper event below targets:
/// `apply` requires the attempt to already exist.
fn order(due: i64) -> Order {
    let mut o = Order::new(oid(), usd(due), t(0));
    if due > 0 {
        o.open_attempt(Attempt::same_currency(P, "inv-1", usd(due)).unwrap()).unwrap();
        o.status = OrderStatus::AwaitingPayment;
    } else {
        o.status = OrderStatus::Paid;
    }
    o
}

fn obs_p(total: i64, tx: &str, inv: &str, provider: &str, at: i64) -> Settlement {
    Settlement::Observed {
        order_id: oid(),
        provider: provider.into(),
        provider_invoice_id: inv.into(),
        observed_total: usd(total),
        tx_ref: tx.into(),
        finality: Finality::Provisional { confirmations: 6 },
        at: t(at),
    }
}
fn obs(total: i64, tx: &str, at: i64) -> Settlement {
    obs_p(total, tx, "inv-1", P, at)
}
fn rev(amount: i64, tx: &str, kind: ReversalKind, at: i64) -> Settlement {
    Settlement::Reversed {
        order_id: oid(),
        provider: P.into(),
        provider_invoice_id: "inv-1".into(),
        tx_ref: tx.into(),
        amount: usd(amount),
        reason: ReversalReason { kind, code: "R01".into() },
        at: t(at),
    }
}

// ---- F2: a reversal larger than anything observed no longer over-bills ----
#[test]
fn regression_reversal_exceeding_observed_caps_the_remainder_at_due() {
    let mm = m();
    let paid = mm.apply(&order(100), &obs(100, "tx1", 10), t(10)).unwrap().order;
    let r = mm.apply(&paid, &rev(400, "r1", ReversalKind::AchReturn, 20), t(20)).unwrap();
    let missing = r.effects.iter().find_map(|e| match &e.effect {
        Effect::AwaitRemainder { missing } => Some(missing.minor),
        _ => None,
    });
    assert_eq!(missing, Some(100), "must never ask a 100-dollar customer for 400");
}

// ---- F4: two invoices on one order are both counted -----------------------
#[test]
fn regression_two_invoices_are_both_counted_toward_the_order() {
    let mm = m();
    // Customer pays 40 on invoice A, then the merchant opens a second
    // invoice (rail failover, or a top-up) and the customer pays it in
    // full there too.
    let mut o = Order::new(oid(), usd(100), t(0));
    o.open_attempt(Attempt::same_currency(P, "inv-A", usd(100)).unwrap()).unwrap();
    o.open_attempt(Attempt::same_currency(P, "inv-B", usd(100)).unwrap()).unwrap();
    o.status = OrderStatus::AwaitingPayment;

    let a = mm.apply(&o, &obs_p(40, "a-tx", "inv-A", P, 10), t(10)).unwrap().order;
    assert_eq!(a.observed().unwrap(), usd(40));
    let b = mm.apply(&a, &obs_p(100, "b-tx", "inv-B", P, 20), t(20)).unwrap().order;
    assert_eq!(b.observed().unwrap(), usd(140), "140 arrived; the ledger says 140");
    assert_eq!(b.status, OrderStatus::Overpaid);

    // And the other direction: two smaller payments that together make the
    // order whole are neither lost nor double-counted.
    let mut o2 = Order::new(oid(), usd(100), t(0));
    o2.open_attempt(Attempt::same_currency(P, "inv-A", usd(100)).unwrap()).unwrap();
    o2.open_attempt(Attempt::same_currency(P, "inv-B", usd(100)).unwrap()).unwrap();
    o2.status = OrderStatus::AwaitingPayment;
    let c = mm.apply(&o2, &obs_p(70, "a-tx", "inv-A", P, 10), t(10)).unwrap().order;
    let d = mm.apply(&c, &obs_p(30, "b-tx", "inv-B", P, 20), t(20)).unwrap().order;
    assert_eq!(d.observed().unwrap(), usd(100), "70 + 30 across two invoices adds up to the full due");
    assert_eq!(d.status, OrderStatus::Paid);
}

// ---- F5: an event naming an unopened invoice cannot rewrite the order -----
#[test]
fn regression_foreign_invoice_id_is_rejected_not_adopted() {
    let mm = m();
    let paid = mm.apply(&order(100), &obs_p(100, "tx1", "inv-1", P, 10), t(10)).unwrap().order;
    let before = paid.clone();
    let hijack = obs_p(100, "tx9", "inv-SOMEONE-ELSE", P, 20);
    assert!(matches!(mm.apply(&paid, &hijack, t(20)), Err(MachineError::UnknownAttempt { .. })));
    assert_eq!(paid, before, "a rejected event must rewrite nothing");
}

// ---- F6: a partial payment stranded by expiry is recorded, not silent -----
#[test]
fn regression_expiry_of_a_partial_payment_is_recorded() {
    let mm = m();
    let mut o = order(100);
    o.expires_at = Some(t(50));
    let partial = mm.apply(&o, &obs(60, "tx1", 10), t(10)).unwrap().order;
    assert_eq!(partial.status, OrderStatus::Underpaid);
    let expired = mm.on_clock(&partial, t(60)).unwrap();
    assert_eq!(expired.order.status, OrderStatus::Expired);
    assert_eq!(expired.order.observed().unwrap(), usd(60), "customer's 60 is on the books");
    assert!(
        expired
            .effects
            .iter()
            .any(|e| matches!(&e.effect, Effect::UnexpectedFunds { observed, .. } if *observed == usd(60))),
        "money stranded by expiry must not vanish with an empty effects list"
    );
}

// ---- F7: Disputed.amount currency is validated against the order ----------
#[test]
fn regression_dispute_amount_currency_is_checked() {
    let mm = m();
    let paid = mm.apply(&order(100), &obs(100, "tx1", 10), t(10)).unwrap().order;
    let ev = Settlement::Disputed {
        order_id: oid(),
        provider: P.into(),
        provider_invoice_id: "inv-1".into(),
        tx_ref: "d1".into(),
        amount: Money::new(100, "BTC"),
        deadline: t(9000),
        at: t(20),
    };
    assert!(matches!(mm.apply(&paid, &ev, t(20)), Err(MachineError::CurrencyMismatch { .. })));
}

// ---- F8: derived-id namespace is private, not the public RFC 4122 URL one --
#[test]
fn regression_ns_derived_is_not_the_rfc4122_dns_namespace() {
    let id = refund_excess_id(oid(), P, "inv-1", &usd(150), &usd(150), &usd(0));
    let dns_ns = Uuid::parse_str("6ba7b810-9dad-11d1-80b4-00c04fd430c8").unwrap();
    let via_dns_ns = Uuid::new_v5(&dns_ns, &{
        let mut v = Vec::new();
        for p in ["refund-excess", &oid().to_string(), P, "inv-1", "150", "USD", "150", "0", "USD"] {
            v.extend_from_slice(&(p.len() as u64).to_be_bytes());
            v.extend_from_slice(p.as_bytes());
        }
        v
    });
    assert_ne!(id, via_dns_ns, "NS_DERIVED must be a private namespace, not the well-known DNS one");
}

// ---- F9: a due == 0 order is Paid on arrival and ships immediately --------
#[test]
fn regression_zero_due_order_is_paid_and_fulfillable() {
    let mm = m();
    let o = order(0);
    assert!(o.attempts.is_empty(), "a zero-price order never needed an invoice");
    assert_eq!(derive_status(&o).unwrap(), OrderStatus::Paid);
    assert!(mm.mark_fulfilled(&o, t(10)).is_ok(), "a zero-price order ships immediately");
}

// ---- F10: a webhook racing invoice creation is a hard error, not a 200 -----
#[derive(Default)]
struct NoOrderStore {
    dead: AtomicUsize,
    ok: AtomicUsize,
}

#[async_trait]
impl OrderStore for NoOrderStore {
    async fn load(&self, id: Uuid) -> Result<Order, PersistError> {
        // The invoice-creation transaction has not committed yet.
        Err(PersistError::UnknownOrder(id))
    }
    async fn commit(&self, _r: ApplyResult) -> Result<CommitResult, PersistError> {
        self.ok.fetch_add(1, AOrd::SeqCst);
        Ok(CommitResult::Applied)
    }
    async fn dead_letter(&self, _e: &Settlement, _raw: &[u8], _why: String) -> Result<(), PersistError> {
        self.dead.fetch_add(1, AOrd::SeqCst);
        Ok(())
    }
}

#[test]
fn regression_unknown_order_is_a_hard_error_not_a_dead_letter() {
    let store = NoOrderStore::default();
    // A perfectly valid, correctly-signed payment notification that simply
    // arrived before the order row did.
    let r = pollster::block_on(ingest(&m(), &store, P, &[obs(100, "tx1", 10)], b"raw", t(10)));
    assert!(
        matches!(r, Err(PersistError::UnknownOrder(_))),
        "a race with invoice creation must surface as an error, not a swallowed 2xx"
    );
    assert_eq!(store.dead.load(AOrd::SeqCst), 0, "not a permanent data problem — must not be dead-lettered");
    assert_eq!(store.ok.load(AOrd::SeqCst), 0);
}
