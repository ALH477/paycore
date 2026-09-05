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
    assert_eq!(
        d.observed().unwrap(),
        usd(100),
        "70 + 30 across two invoices adds up to the full due"
    );
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
        for p in ["refund-excess", &oid().to_string(), P, "inv-1", "150", "USD", "150", "0", "USD"]
        {
            v.extend_from_slice(&(p.len() as u64).to_be_bytes());
            v.extend_from_slice(p.as_bytes());
        }
        v
    });
    assert_ne!(
        id, via_dns_ns,
        "NS_DERIVED must be a private namespace, not the well-known DNS one"
    );
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
    async fn dead_letter(
        &self,
        _e: &Settlement,
        _raw: &[u8],
        _why: String,
    ) -> Result<(), PersistError> {
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
    assert_eq!(
        store.dead.load(AOrd::SeqCst),
        0,
        "not a permanent data problem — must not be dead-lettered"
    );
    assert_eq!(store.ok.load(AOrd::SeqCst), 0);
}

// ---- F12: a provider-declared ending must not strand funds silently -------
//
// F6 fixed this for `on_clock`. The provider-declared endings kept the old
// behaviour: terminal status, empty effect list, and the customer's partial
// payment recorded on the attempt row and nowhere a human would look. BTCPay's
// `InvoiceExpired` maps straight onto this arm, and an invoice expiring
// part-paid is routine on-chain.
#[test]
fn regression_provider_expiry_of_a_partial_payment_is_recorded() {
    let mm = m();
    let partial = mm.apply(&order(100), &obs(60, "tx1", 10), t(10)).unwrap().order;
    assert_eq!(partial.status, OrderStatus::Underpaid);

    let ev = Settlement::Expired {
        order_id: oid(),
        provider: P.into(),
        provider_invoice_id: "inv-1".into(),
        at: t(50),
    };
    let ended = mm.apply(&partial, &ev, t(50)).unwrap();
    assert_eq!(ended.order.status, OrderStatus::Expired);
    assert_eq!(ended.order.observed().unwrap(), usd(60), "the 60 is still on the books");
    assert!(
        ended.effects.iter().any(|e| matches!(
            &e.effect,
            Effect::UnexpectedFunds { observed, .. } if *observed == usd(60)
        )),
        "money stranded by a provider expiry needs a human, exactly as a clock expiry does"
    );
}

#[test]
fn regression_provider_failure_of_a_partial_payment_is_recorded() {
    let mm = m();
    let partial = mm.apply(&order(100), &obs(60, "tx1", 10), t(10)).unwrap().order;
    let ev = Settlement::Failed {
        order_id: oid(),
        provider: P.into(),
        provider_invoice_id: "inv-1".into(),
        code: "invalid".into(),
        at: t(50),
    };
    let ended = mm.apply(&partial, &ev, t(50)).unwrap();
    assert_eq!(ended.order.status, OrderStatus::Failed);
    assert!(
        ended.effects.iter().any(|e| matches!(&e.effect, Effect::UnexpectedFunds { .. })),
        "a failed invoice holding funds is not an empty effect list"
    );
}

// ---- F16: a future-dated provider timestamp cannot hold the window open ---
#[test]
fn regression_chargeback_anchor_never_runs_past_the_ingest_clock() {
    let mm = OrderMachine::new(StaticPolicy::new(1, Some(Duration::days(180))));
    // A provider reporting `at` a year out would pin the window a year and a
    // half away, and the order could never be promoted to Final or shipped.
    let far_future = obs(100, "tx1", 10_000 + 365 * 86_400);
    let funded = mm.apply(&order(100), &far_future, t(10)).unwrap().order;

    let ends = funded.attempts[0].chargeback_window_ends.expect("window opens on the full payment");
    assert_eq!(
        ends,
        t(10) + Duration::days(180),
        "the anchor is clamped to the ingest clock, not taken from the payload"
    );
    assert!(mm.on_clock(&funded, t(10) + Duration::days(181)).is_some(), "and it does close");
}

#[test]
fn chargeback_anchor_keeps_a_past_timestamp() {
    let mm = OrderMachine::new(StaticPolicy::new(1, Some(Duration::days(180))));
    // The rail measures its window from the transaction, so a delayed webhook
    // or a reconcile replay of real history must not restart the clock.
    let funded = mm.apply(&order(100), &obs(100, "tx1", 10), t(5_000)).unwrap().order;
    assert_eq!(funded.attempts[0].chargeback_window_ends, Some(t(10) + Duration::days(180)));
}

// ---- F17: the clock promotion key was a non-injective separator join ------
#[test]
fn regression_clock_promotion_key_is_injective_across_invoice_ids() {
    let mm = OrderMachine::new(StaticPolicy::new(1, Some(Duration::days(1))));

    // Two orders whose promoted attempt-id sets differ only in where the comma
    // falls. Joined with `","` both produce `window-closed:a,b`, so the second
    // commits as Duplicate and its finality promotion is lost for good.
    let key_for = |ids: &[&str]| {
        let mut o = Order::new(oid(), usd(100), t(0));
        for id in ids {
            o.open_attempt(Attempt::same_currency(P, *id, usd(100)).unwrap()).unwrap();
        }
        o.status = OrderStatus::AwaitingPayment;
        for id in ids {
            o = mm.apply(&o, &obs_p(100, "tx", id, P, 10), t(10)).unwrap().order;
        }
        mm.on_clock(&o, t(10) + Duration::days(2)).expect("windows close").key
    };

    assert_ne!(key_for(&["a,b"]), key_for(&["a", "b"]));
}

#[test]
fn regression_clock_promotion_key_is_bounded() {
    let mm = OrderMachine::new(StaticPolicy::new(1, Some(Duration::days(1))));
    let long: Vec<String> = (0..8).map(|i| format!("{}{i}", "x".repeat(MAX_ID_LEN - 1))).collect();
    let mut o = Order::new(oid(), usd(800), t(0));
    for id in &long {
        o.open_attempt(Attempt::same_currency(P, id.as_str(), usd(100)).unwrap()).unwrap();
    }
    o.status = OrderStatus::AwaitingPayment;
    for id in &long {
        o = mm.apply(&o, &obs_p(100, "tx", id, P, 10), t(10)).unwrap().order;
    }
    let key = mm.on_clock(&o, t(10) + Duration::days(2)).unwrap().key;
    assert!(
        key.tx_ref.len() <= MAX_ID_LEN,
        "a machine-minted key never passes through `validate`, so it must be bounded by \
         construction: got {} bytes",
        key.tx_ref.len()
    );
}

// ---- F18: a zero-`covers` attempt wedged the order permanently ------------
//
// `covers` is the denominator in `to_rail_currency`, which `funding_effects`
// calls for every attempt holding refundable funds. A zero one made that error,
// so every subsequent observation on the order was dead-lettered with no way
// back. `quoted` was already required to be positive; `covers` was not.
#[test]
fn regression_zero_covers_attempt_is_rejected_at_construction() {
    let err = Attempt::new(P, "inv-1", Money::new(100, "BTC"), usd(0));
    assert!(matches!(err, Err(MachineError::InvalidAttempt { .. })), "{err:?}");
    assert!(Attempt::new(P, "inv-1", Money::new(100, "BTC"), usd(1)).is_ok());
}

// ---- F20: `refunded_total` was unbounded ----------------------------------
//
// It sums, and it is subtracted from the excess, so an inflated report drives
// `outstanding_excess` to zero and the payer's real excess is never sent back.
// No effect, no dead letter, no status change — a silent loss on the customer's
// side of the ledger.
#[test]
fn regression_refund_cannot_exceed_what_the_attempt_holds() {
    let mm = m();
    let overpaid = mm.apply(&order(100), &obs(150, "tx1", 10), t(10)).unwrap().order;
    assert_eq!(overpaid.outstanding_excess().unwrap(), usd(50));

    let inflated = Settlement::Refunded {
        order_id: oid(),
        provider: P.into(),
        provider_invoice_id: "inv-1".into(),
        tx_ref: "r1".into(),
        refund_id: Uuid::from_u128(9),
        amount: usd(500),
        at: t(20),
    };
    assert!(matches!(
        mm.apply(&overpaid, &inflated, t(20)),
        Err(MachineError::RefundExceedsHeld { .. })
    ));
    assert_eq!(
        overpaid.outstanding_excess().unwrap(),
        usd(50),
        "the excess the payer is owed is untouched by the rejected report"
    );

    // The genuine refund of that excess is still accepted.
    let real = Settlement::Refunded {
        order_id: oid(),
        provider: P.into(),
        provider_invoice_id: "inv-1".into(),
        tx_ref: "r2".into(),
        refund_id: Uuid::from_u128(10),
        amount: usd(50),
        at: t(20),
    };
    let done = mm.apply(&overpaid, &real, t(20)).unwrap().order;
    assert_eq!(done.outstanding_excess().unwrap(), usd(0));
}
