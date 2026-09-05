//! Money is tested as a lattice; status is tested as a transition table.
//! Mixing the two into one permutation assertion is how you end up
//! contorting the machine to satisfy a test that was wrong.
//!
//! Order-level money (`observed`/`net`/`refunded`) is a fold over attempts;
//! per-attempt fields (`reversed_total`, `refunded_total`, `finality`,
//! `chargeback_window_ends`, `last_tx_ref`) live on the `Attempt` an event
//! names, not on the order. `att()` reaches the default single-invoice
//! attempt every helper below opens.
//!
//! The final section is a regression per audit finding.

use crate::order::{derive_status, Attempt};
use crate::*;
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
fn m_window(d: Duration) -> OrderMachine<StaticPolicy> {
    OrderMachine::new(StaticPolicy::new(1, Some(d)))
}

/// A `due > 0` order always carries the invoice every helper event below
/// targets: `apply` requires the attempt to already exist, so a bare
/// `Order::new` would make every event in this file `UnknownAttempt`. A
/// `due == 0` order gets none — it never needed one to begin with.
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

/// The lone attempt every `order(due>0)` opens.
fn att(o: &Order) -> &Attempt {
    o.attempt(P, "inv-1").unwrap()
}

fn obs_pi(total: i64, tx: &str, provider: &str, inv: &str, confs: u32, at: i64) -> Settlement {
    Settlement::Observed {
        order_id: oid(),
        provider: provider.into(),
        provider_invoice_id: inv.into(),
        observed_total: usd(total),
        tx_ref: tx.into(),
        finality: Finality::Provisional { confirmations: confs },
        at: t(at),
    }
}

fn obs(total: i64, tx: &str, confs: u32, at: i64) -> Settlement {
    obs_pi(total, tx, P, "inv-1", confs, at)
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

fn refunded(amount: i64, tx: &str, refund_id: Uuid, at: i64) -> Settlement {
    Settlement::Refunded {
        order_id: oid(),
        provider: P.into(),
        provider_invoice_id: "inv-1".into(),
        tx_ref: tx.into(),
        refund_id,
        amount: usd(amount),
        at: t(at),
    }
}

fn disputed(amount: i64, at: i64) -> Settlement {
    Settlement::Disputed {
        order_id: oid(),
        provider: P.into(),
        provider_invoice_id: "inv-1".into(),
        tx_ref: "dis-1".into(),
        amount: usd(amount),
        deadline: t(90_000),
        at: t(at),
    }
}

fn resolved(won: bool, at: i64) -> Settlement {
    Settlement::DisputeResolved {
        order_id: oid(),
        provider: P.into(),
        provider_invoice_id: "inv-1".into(),
        tx_ref: "dis-1".into(),
        won,
        at: t(at),
    }
}

/// Applies every event, unwrapping. A test whose sequence is expected to be
/// rejected partway through does not belong here — call `mm.apply` directly
/// and match on `Err` so a machine bug shows up as a panic, not a silently
/// shorter fold.
fn fold(mm: &OrderMachine<StaticPolicy>, start: Order, evs: &[Settlement]) -> (Order, Vec<Effect>) {
    let mut o = start;
    let mut fx = Vec::new();
    for (i, e) in evs.iter().enumerate() {
        let r = mm.apply(&o, e, t(1000 + i as i64)).unwrap();
        o = r.order;
        fx.extend(r.effects.into_iter().map(|x| x.effect));
    }
    (o, fx)
}

fn refunds(r: &ApplyResult) -> Vec<(Uuid, i64)> {
    r.effects
        .iter()
        .filter_map(|e| match &e.effect {
            Effect::RefundExcess { refund_id, amount, .. } => Some((*refund_id, amount.minor)),
            _ => None,
        })
        .collect()
}

// ===========================================================================
// The money lattice
// ===========================================================================

#[test]
fn stale_observation_cannot_resurrect_returned_funds() {
    let (o, fx) = fold(
        &m(),
        order(100),
        &[
            obs(100, "tx-a", 6, 10),
            rev(100, "ret-a", ReversalKind::AchReturn, 20),
            // Provider's invoice view still reports the original total,
            // re-delivered under a fresh tx_ref (RBF, reconcile snapshot),
            // so the unique index cannot suppress it.
            obs(100, "tx-a-rbf", 6, 30),
        ],
    );
    assert_eq!(o.net().unwrap(), usd(0));
    assert_eq!(o.observed().unwrap(), usd(100));
    assert_eq!(att(&o).reversed_total, usd(100));
    assert_eq!(o.status, OrderStatus::AwaitingPayment);
    assert_eq!(fx.iter().filter(|e| **e == Effect::MayFulfill).count(), 1);
}

#[test]
fn money_is_permutation_invariant() {
    let mm = m();
    // Non-closing reversal, so no permutation reaches a terminal status and
    // every event is accepted from every intermediate state.
    let evs = [
        obs(100, "tx-a", 6, 10),
        rev(40, "ret-a", ReversalKind::AchReturn, 20),
        obs(100, "tx-a-reemit", 6, 30),
    ];
    let perms = [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]];
    let mut seen = Vec::new();
    for p in perms {
        let seq: Vec<Settlement> = p.iter().map(|i| evs[*i].clone()).collect();
        let (o, _) = fold(&mm, order(100), &seq);
        seen.push((o.observed().unwrap(), att(&o).reversed_total.clone(), o.net().unwrap()));
    }
    assert!(seen.windows(2).all(|w| w[0] == w[1]), "{seen:?}");
    assert_eq!(seen[0].2, usd(60));
}

#[test]
fn status_is_path_dependent_by_design() {
    let mm = m();
    let pay = obs(100, "tx-a", 6, 10);
    let cb = rev(100, "cb-a", ReversalKind::Chargeback, 20);
    let (a, _) = fold(&mm, order(100), &[pay.clone(), cb.clone()]);
    let (b, _) = fold(&mm, order(100), &[cb, pay]);
    assert_eq!(a.status, OrderStatus::Reversed);
    assert_eq!(b.status, OrderStatus::Reversed);
    assert_eq!(a.observed().unwrap(), usd(100));
    // The chargeback closed the order before the payment was seen; the
    // funds are still recorded, as UnexpectedFunds.
    assert_eq!(b.observed().unwrap(), usd(100));
    assert!(b.reversal_closed);
}

#[test]
fn observed_join_is_idempotent() {
    let mm = m();
    let ev = obs(100, "tx-a", 6, 10);
    let once = mm.apply(&order(100), &ev, t(50)).unwrap().order;
    let twice = mm.apply(&once, &ev, t(50)).unwrap().order;
    assert_eq!(once, twice);
}

#[test]
fn reversed_sum_is_not_idempotent_and_that_is_the_stores_job() {
    let mm = m();
    let start = mm.apply(&order(100), &obs(100, "tx-a", 6, 10), t(50)).unwrap().order;
    let ev = rev(40, "ret-a", ReversalKind::AchReturn, 20);
    let once = mm.apply(&start, &ev, t(60)).unwrap().order;
    let twice = mm.apply(&once, &ev, t(60)).unwrap().order;
    // Monotone, not idempotent. The unique index on
    // (order_id, provider, kind, provider_invoice_id, tx_ref) is what stops
    // the second call from ever reaching `apply`.
    assert_eq!(att(&once).reversed_total, usd(40));
    assert_eq!(att(&twice).reversed_total, usd(80));
}

// ===========================================================================
// Status transitions
// ===========================================================================

#[test]
fn fulfil_is_sticky_and_gated() {
    let mm = m();
    let paid = mm.apply(&order(100), &obs(100, "tx-a", 6, 10), t(50)).unwrap().order;
    let ful = mm.mark_fulfilled(&paid, t(60)).unwrap().order;
    assert_eq!(ful.status, OrderStatus::Fulfilled);

    let after = mm.apply(&ful, &obs(100, "tx-a2", 12, 70), t(70)).unwrap();
    assert_eq!(after.order.status, OrderStatus::Fulfilled);
    assert!(after.effects.iter().all(|e| e.effect != Effect::MayFulfill));

    let under = mm.apply(&order(100), &obs(30, "tx-b", 6, 10), t(50)).unwrap().order;
    assert!(matches!(
        mm.mark_fulfilled(&under, t(60)),
        Err(MachineError::FulfillRejected { .. })
    ));
}

#[test]
fn dispute_survives_a_confirmation_and_only_resolution_clears_it() {
    let mm = m();
    let paid = mm.apply(&order(100), &obs(100, "tx-a", 6, 10), t(10)).unwrap().order;
    let dis = mm.apply(&paid, &disputed(100, 20), t(20)).unwrap().order;
    assert_eq!(dis.status, OrderStatus::Disputed);

    let deeper = mm.apply(&dis, &obs(100, "tx-a2", 12, 30), t(30)).unwrap();
    assert_eq!(deeper.order.status, OrderStatus::Disputed, "a webhook must not clear a chargeback");
    assert!(deeper.effects.iter().all(|e| e.effect != Effect::MayFulfill));

    let won = mm.apply(&deeper.order, &resolved(true, 40), t(40)).unwrap();
    assert_eq!(won.order.status, OrderStatus::Paid);
}

#[test]
fn dispute_lost_closes_and_zeroes_net() {
    let mm = m();
    let paid = mm.apply(&order(100), &obs(100, "tx-a", 6, 10), t(10)).unwrap().order;
    let dis = mm.apply(&paid, &disputed(100, 20), t(20)).unwrap().order;
    let lost = mm.apply(&dis, &resolved(false, 30), t(30)).unwrap();
    assert_eq!(lost.order.status, OrderStatus::Reversed);
    assert_eq!(lost.order.net().unwrap(), usd(0));
    assert!(lost.order.reversal_closed);
}

#[test]
fn stacked_chargebacks_both_record_loss() {
    let mm = m();
    let paid = mm.apply(&order(100), &obs(100, "tx-a", 6, 10), t(50)).unwrap().order;
    let a = mm.apply(&paid, &rev(60, "cb-1", ReversalKind::Chargeback, 60), t(60)).unwrap();
    let b = mm.apply(&a.order, &rev(40, "cb-2", ReversalKind::Chargeback, 70), t(70)).unwrap();
    assert_eq!(att(&b.order).reversed_total, usd(100));
    assert_eq!(b.order.status, OrderStatus::Reversed);
    assert!(b.effects.iter().any(|e| matches!(e.effect, Effect::RecordLoss { .. })));
}

#[test]
fn chargeback_window_anchors_on_the_completing_observation() {
    let mm = m_window(Duration::days(90));
    let partial = mm.apply(&order(100), &obs(30, "tx-a", 6, 100), t(100)).unwrap().order;
    assert_eq!(partial.status, OrderStatus::Underpaid);
    assert!(att(&partial).chargeback_window_ends.is_none());

    let whole = mm.apply(&partial, &obs(100, "tx-b", 6, 500), t(500)).unwrap().order;
    assert_eq!(att(&whole).chargeback_window_ends, Some(t(500) + Duration::days(90)));
}

#[test]
fn clock_promotes_finality_once_the_window_closes() {
    let mm = m_window(Duration::days(90));
    let paid = mm.apply(&order(100), &obs(100, "tx-a", 6, 100), t(100)).unwrap().order;
    assert!(mm.on_clock(&paid, t(200)).is_none(), "window still open");
    let promoted = mm.on_clock(&paid, t(100) + Duration::days(91)).unwrap();
    assert_eq!(att(&promoted.order).finality, Some(Finality::Final));
    // Idempotent: Final blocks a second promotion.
    assert!(mm.on_clock(&promoted.order, t(100) + Duration::days(92)).is_none());
}

// ===========================================================================
// Attempts: the binding between an event and the invoice it names
// ===========================================================================

#[test]
fn apply_requires_the_attempt_to_already_exist() {
    let mm = m();
    let o = order(100);
    let ev = obs_pi(50, "tx1", P, "inv-other", 6, 10);
    assert!(matches!(mm.apply(&o, &ev, t(10)), Err(MachineError::UnknownAttempt { .. })));
}

#[test]
fn zero_due_orders_carry_no_attempt_and_are_paid_on_arrival() {
    let mm = m();
    let o = order(0);
    assert!(o.attempts.is_empty());
    assert_eq!(derive_status(&o).unwrap(), OrderStatus::Paid);
    assert!(mm.mark_fulfilled(&o, t(10)).is_ok());
}

#[test]
fn two_invoices_on_one_order_are_both_counted() {
    let mm = m();
    let mut o = Order::new(oid(), usd(100), t(0));
    o.open_attempt(Attempt::same_currency(P, "inv-A", usd(100)).unwrap()).unwrap();
    o.open_attempt(Attempt::same_currency(P, "inv-B", usd(100)).unwrap()).unwrap();
    o.status = OrderStatus::AwaitingPayment;

    let a = mm.apply(&o, &obs_pi(40, "a-tx", P, "inv-A", 6, 10), t(10)).unwrap().order;
    let b = mm.apply(&a, &obs_pi(100, "b-tx", P, "inv-B", 6, 20), t(20)).unwrap().order;

    assert_eq!(b.observed().unwrap(), usd(140));
    assert_eq!(b.status, OrderStatus::Overpaid);
}

#[test]
fn disputed_amount_currency_is_checked_against_the_order() {
    let mm = m();
    let paid = mm.apply(&order(100), &obs(100, "tx1", 6, 10), t(10)).unwrap().order;
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

#[test]
fn expiry_of_an_underpaid_order_with_funds_emits_unexpected_funds() {
    let mm = m();
    let mut o = order(100);
    o.expires_at = Some(t(50));
    let partial = mm.apply(&o, &obs(60, "tx1", 6, 10), t(10)).unwrap().order;
    assert_eq!(partial.status, OrderStatus::Underpaid);

    let expired = mm.on_clock(&partial, t(60)).unwrap();
    assert_eq!(expired.order.status, OrderStatus::Expired);
    assert_eq!(expired.order.observed().unwrap(), usd(60), "customer's 60 is on the books");
    assert!(
        expired
            .effects
            .iter()
            .any(|e| matches!(&e.effect, Effect::UnexpectedFunds { observed, .. } if *observed == usd(60))),
        "money stranded by expiry must not vanish silently"
    );
}

// ===========================================================================
// Contract violations by drivers
// ===========================================================================

#[test]
fn currency_mismatch_is_an_error_not_a_comparison() {
    let ev = Settlement::Observed {
        order_id: oid(),
        provider: P.into(),
        provider_invoice_id: "inv-1".into(),
        observed_total: Money::new(100, "BTC"),
        tx_ref: "tx-a".into(),
        finality: Finality::Provisional { confirmations: 6 },
        at: t(10),
    };
    assert!(matches!(
        m().apply(&order(100), &ev, t(10)),
        Err(MachineError::CurrencyMismatch { .. })
    ));
}

#[test]
fn err_leaves_nothing_observable() {
    let before = order(100);
    let bad = Settlement::Observed {
        order_id: oid(),
        provider: P.into(),
        provider_invoice_id: "inv-1".into(),
        observed_total: Money::new(100, "BTC"),
        tx_ref: "tx-a".into(),
        finality: Finality::Provisional { confirmations: 6 },
        at: t(10),
    };
    let after = before.clone();
    assert!(m().apply(&after, &bad, t(10)).is_err());
    // `apply` takes &Order and drafts into a local. Structural, not a
    // convention.
    assert_eq!(before, after);
}

#[test]
fn wrong_order_routing_is_rejected() {
    let mut foreign = order(100);
    foreign.id = Uuid::from_u128(99);
    assert!(matches!(
        m().apply(&foreign, &obs(100, "tx-a", 6, 10), t(10)),
        Err(MachineError::WrongOrder { .. })
    ));
}

#[test]
fn no_arithmetic_panics_at_the_boundary() {
    let mm = m();
    let mut o = order(100);
    o.attempts[0].observed_total = Money::new(i64::MIN, USD);
    let res = mm.apply(&o, &rev(1, "r", ReversalKind::AchReturn, 10), t(10));
    assert!(matches!(res, Err(MachineError::AmountOverflow)) || res.is_ok());
}

// ===========================================================================
// Regressions — one per audit finding
// ===========================================================================

/// CRITICAL: a growing overpay minted a second refund id, so 150 was
/// instructed against a 100 excess.
#[test]
fn regression_growing_overpay_does_not_over_refund() {
    let mm = m();
    let a = mm.apply(&order(100), &obs(150, "tx1", 6, 10), t(10)).unwrap();
    let (id_a, amt_a) = refunds(&a)[0];
    assert_eq!(amt_a, 50);

    // Worker executes it and reports back.
    let paid_back = mm.apply(&a.order, &refunded(50, "rf1", id_a, 15), t(15)).unwrap().order;
    assert_eq!(paid_back.refunded().unwrap(), usd(50));
    assert_eq!(paid_back.outstanding_excess().unwrap(), usd(0));

    // Overpay grows.
    let b = mm.apply(&paid_back, &obs(200, "tx2", 6, 20), t(20)).unwrap();
    let (id_b, amt_b) = refunds(&b)[0];
    assert_ne!(id_a, id_b, "a new refund needs a new id");
    assert_eq!(amt_b, 50, "only the un-refunded part");
    assert_eq!(amt_a + amt_b, 100, "total instructed equals the real excess");
}

/// CRITICAL: recomputing an overpay after a dispute win must reuse the id.
#[test]
fn regression_refund_id_is_stable_across_a_dispute_win() {
    let mm = m();
    let paid = mm.apply(&order(100), &obs(150, "tx1", 6, 10), t(10)).unwrap();
    let first = refunds(&paid)[0].0;
    let dis = mm.apply(&paid.order, &disputed(150, 20), t(20)).unwrap();
    let won = mm.apply(&dis.order, &resolved(true, 30), t(30)).unwrap();
    assert_eq!(first, refunds(&won)[0].0);
}

/// HIGH: a late payment on an expired invoice was silently swallowed —
/// no accumulation, no effect, no dead letter.
#[test]
fn regression_late_payment_on_expired_invoice_is_recorded() {
    let mm = m();
    let mut o = order(100);
    o.expires_at = Some(t(50));
    let expired = mm.on_clock(&o, t(60)).unwrap().order;
    assert_eq!(expired.status, OrderStatus::Expired);

    let late = mm.apply(&expired, &obs(100, "tx-late", 6, 70), t(70)).unwrap();
    assert_eq!(late.order.status, OrderStatus::Expired, "still expired");
    assert_eq!(late.order.observed().unwrap(), usd(100), "but the money is on the books");
    assert_eq!(att(&late.order).observed_total, usd(100), "recorded on the attempt the event named");
    assert!(late
        .effects
        .iter()
        .any(|e| matches!(e.effect, Effect::UnexpectedFunds { .. })));
}

/// HIGH: `provider` is index material; varying it replayed one clawback
/// three times for a total loss of 300 on a 100 payment.
#[test]
fn regression_provider_must_match_the_backend() {
    use crate::store::*;
    // The keys still differ — that is the attack. `ingest` is what closes
    // it, by refusing to hand a mismatched event to the machine at all.
    let a = rev(100, "r1", ReversalKind::AchReturn, 30);
    let spoof = Settlement::Reversed {
        order_id: oid(),
        provider: "btcpay ".into(),
        provider_invoice_id: "inv-1".into(),
        tx_ref: "r1".into(),
        amount: usd(100),
        reason: ReversalReason { kind: ReversalKind::AchReturn, code: "R01".into() },
        at: t(30),
    };
    assert_ne!(a.idempotency_key(), spoof.idempotency_key());

    let store = RecordingStore::default();
    let rt = pollster::block_on(ingest(&m(), &store, P, &[spoof], b"raw", t(30)));
    rt.unwrap();
    assert_eq!(store.dead_lettered(), 1, "mismatched provider never reaches apply");
    assert_eq!(store.committed(), 0);
}

/// MEDIUM: `|`-joined hash input was not injective, so two distinct events
/// shared an outbox row id.
#[test]
fn regression_outbox_ids_do_not_collide_on_adjacent_fields() {
    let k1 = IdempotencyKey {
        order_id: oid(),
        provider: P.into(),
        kind: EventKind::Observed,
        provider_invoice_id: "c|d".into(),
        tx_ref: "t".into(),
    };
    let k2 = IdempotencyKey {
        order_id: oid(),
        provider: P.into(),
        kind: EventKind::Observed,
        provider_invoice_id: "c".into(),
        tx_ref: "d|t".into(),
    };
    assert_ne!(k1, k2);
    assert_ne!(k1.derived_uuid(&["may-fulfill"]), k2.derived_uuid(&["may-fulfill"]));
}

/// MEDIUM: an ACH bounce on a fulfilled order left status `Fulfilled` with
/// net 0, and suppressed the request for repayment.
#[test]
fn regression_ach_bounce_on_a_fulfilled_order_is_recalled() {
    let mm = m();
    let paid = mm.apply(&order(100), &obs(100, "tx1", 6, 10), t(10)).unwrap().order;
    let ful = mm.mark_fulfilled(&paid, t(20)).unwrap().order;
    // Reversed for more than was ever paid — a driver miscount, or a second
    // clawback the invoice's own history can't otherwise explain. The
    // remainder asked of the customer must never exceed what they owe.
    let bounced = mm
        .apply(&ful, &rev(400, "r1", ReversalKind::AchReturn, 30), t(30))
        .unwrap();

    assert_eq!(bounced.order.status, OrderStatus::Recalled, "shipped but unfunded");
    assert!(!bounced.order.reversal_closed, "an ACH return is recoverable");
    let purposes: Vec<_> = bounced.effects.iter().map(|e| e.effect.purpose()).collect();
    assert!(purposes.contains(&"recall"));
    assert!(purposes.contains(&"await"), "the customer can still make it whole");
    let missing = bounced.effects.iter().find_map(|e| match &e.effect {
        Effect::AwaitRemainder { missing } => Some(missing.minor),
        _ => None,
    });
    assert_eq!(missing, Some(100), "never ask for more than due, regardless of the reversal's size");

    // And they do.
    let repaid = mm.apply(&bounced.order, &obs(500, "tx2", 6, 40), t(40)).unwrap().order;
    assert_eq!(repaid.net().unwrap(), usd(100));
    assert_eq!(repaid.status, OrderStatus::Fulfilled);
}

/// MEDIUM: a reversal larger than anything ever observed on an otherwise
/// live order over-billed the customer for the remainder instead of capping
/// the ask at what they actually owe.
#[test]
fn regression_reversal_missing_is_capped_at_due_not_the_reversal_amount() {
    let mm = m();
    let paid = mm.apply(&order(100), &obs(100, "tx1", 6, 10), t(10)).unwrap().order;
    let r = mm.apply(&paid, &rev(400, "r1", ReversalKind::AchReturn, 20), t(20)).unwrap();
    let missing = r.effects.iter().find_map(|e| match &e.effect {
        Effect::AwaitRemainder { missing } => Some(missing.minor),
        _ => None,
    });
    assert_eq!(missing, Some(100), "asks a 100-dollar customer for 100, not 400");
}

/// LOW: oversized identifiers would fail the btree insert at commit and
/// become an endless provider retry loop.
#[test]
fn regression_oversized_identifiers_are_rejected_before_commit() {
    use crate::store::*;
    let long = "x".repeat(MAX_ID_LEN + 1);
    let ev = Settlement::Observed {
        order_id: oid(),
        provider: P.into(),
        provider_invoice_id: "inv-1".into(),
        observed_total: usd(100),
        tx_ref: long,
        finality: Finality::Provisional { confirmations: 6 },
        at: t(10),
    };
    let store = RecordingStore::default();
    pollster::block_on(ingest(&m(), &store, P, &[ev], b"raw", t(10))).unwrap();
    assert_eq!(store.dead_lettered(), 1);
    assert_eq!(store.committed(), 0);
}

#[test]
fn verified_body_rejects_a_bad_mac() {
    assert!(VerifiedBody::from_mac(b"body", b"aaaa", b"bbbb").is_err());
    assert!(VerifiedBody::from_mac(b"body", b"", b"").is_err(), "empty mac is not proof");
    assert!(VerifiedBody::from_mac(b"body", b"aaaa", b"aaa").is_err(), "length differs");
    let ok = VerifiedBody::from_mac(b"body", b"aaaa", b"aaaa").unwrap();
    assert_eq!(ok.as_bytes(), b"body");
}

#[test]
fn derive_status_is_a_function_of_state_alone() {
    let mm = m();
    let paid = mm.apply(&order(100), &obs(100, "tx-a", 6, 10), t(10)).unwrap().order;
    // Recomputing from the same accumulators must reproduce the status.
    assert_eq!(derive_status(&paid).unwrap(), paid.status);
    let ful = mm.mark_fulfilled(&paid, t(20)).unwrap().order;
    assert_eq!(derive_status(&ful).unwrap(), ful.status);
}

/// CRITICAL: a negative delta walked a summing accumulator backwards. One
/// `Reversed { amount: -500 }` against a 100 payment left `reversed_total`
/// at -500 and net at 600, and the next observation turned the difference
/// into a `RefundExcess` for money that never arrived.
#[test]
fn regression_negative_delta_is_rejected_from_every_status() {
    use crate::store::*;
    let mm = m();
    let paid = mm.apply(&order(100), &obs(100, "tx-a", 6, 10), t(10)).unwrap().order;
    for ev in [
        rev(-500, "r1", ReversalKind::Other, 20),
        refunded(-500, "rf1", Uuid::from_u128(7), 20),
        disputed(-500, 20),
    ] {
        assert!(
            matches!(mm.apply(&paid, &ev, t(20)), Err(MachineError::NegativeAmount { .. })),
            "{} accepted a negative amount",
            ev.name()
        );
    }

    // Rejected from a closed status too, so a driver bug is dead-lettered
    // rather than quietly absorbed by the late-event arm.
    let mut closed = paid.clone();
    closed.status = OrderStatus::Expired;
    assert!(matches!(
        mm.apply(&closed, &rev(-500, "r1", ReversalKind::Other, 20), t(20)),
        Err(MachineError::NegativeAmount { .. })
    ));

    let store = RecordingStore::default();
    let ev = [rev(-500, "r1", ReversalKind::Other, 20)];
    pollster::block_on(ingest(&mm, &store, P, &ev, b"raw", t(20))).unwrap();
    assert_eq!(store.dead_lettered(), 1);
    assert_eq!(store.committed(), 0);
}

/// HIGH: `refund_id` was absent from the index and `tx_ref` stood in for it,
/// so a provider reporting an already-executed refund under a fresh
/// transaction reference booked the same 50 twice — erasing the excess still
/// owed to the payer.
#[test]
fn regression_refund_is_keyed_on_refund_id_not_tx_ref() {
    let mm = m();
    let over = mm.apply(&order(100), &obs(150, "tx1", 6, 10), t(10)).unwrap();
    let (rid, amt) = refunds(&over)[0];
    assert_eq!(amt, 50);

    // One refund, two notifications, two transaction references, one row.
    let a = refunded(50, "rf-attempt-1", rid, 15);
    let b = refunded(50, "rf-attempt-2", rid, 16);
    assert_eq!(a.idempotency_key(), b.idempotency_key());
    assert_eq!(a.idempotency_key().tx_ref, rid.to_string(), "keyed on refund_id");

    // A genuinely different refund still gets its own row.
    let c = refunded(50, "rf-attempt-1", Uuid::from_u128(0xdead), 15);
    assert_ne!(a.idempotency_key(), c.idempotency_key());

    // Every other kind is untouched: still keyed on tx_ref.
    assert_eq!(obs(150, "tx1", 6, 10).idempotency_key().tx_ref, "tx1");

    // And the provider's own reference is still recorded on the attempt.
    let after = mm.apply(&over.order, &a, t(15)).unwrap().order;
    assert_eq!(att(&after).last_tx_ref.as_deref(), Some("rf-attempt-1"));
    assert_eq!(after.refunded().unwrap(), usd(50));
    assert_eq!(after.outstanding_excess().unwrap(), usd(0));
}

// ---------------------------------------------------------------------------
// Store double. `pollster::block_on` drives the async port; the crate itself
// stays runtime-agnostic and `unsafe`-free.
// ---------------------------------------------------------------------------

#[derive(Default)]
struct RecordingStore {
    dead: std::sync::atomic::AtomicUsize,
    ok: std::sync::atomic::AtomicUsize,
}

impl RecordingStore {
    fn dead_lettered(&self) -> usize {
        self.dead.load(std::sync::atomic::Ordering::SeqCst)
    }
    fn committed(&self) -> usize {
        self.ok.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl OrderStore for RecordingStore {
    async fn load(&self, _id: Uuid) -> Result<Order, PersistError> {
        // Must carry the `inv-1` attempt, or every event above would fail
        // with `UnknownAttempt` before it ever reached the behaviour under
        // test.
        Ok(order(100))
    }
    async fn commit(&self, _r: ApplyResult) -> Result<CommitResult, PersistError> {
        self.ok.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(CommitResult::Applied)
    }
    async fn dead_letter(
        &self,
        _e: &Settlement,
        _raw: &[u8],
        _why: String,
    ) -> Result<(), PersistError> {
        self.dead.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}
