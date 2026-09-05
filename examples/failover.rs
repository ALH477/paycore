//! Losing a processor mid-order, without losing the order.
//!
//! An order is funded by *attempts* — one invoice on one rail. The order owns
//! `due`; each attempt records what it collected in its own currency and what
//! that is worth against `due`. Adding a rail is opening another attempt, so a
//! processor cutting you off costs you an invoice, not a customer.
//!
//! Run with: `cargo run --example failover`

use paycore::{
    ingest, Attempt, Effect, Finality, MemoryStore, Money, Order, OrderCatalog, OrderMachine,
    OrderStatus, OrderStore, Settlement, StaticPolicy,
};
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

fn usd(cents: i64) -> Money {
    Money::new(cents, "USD")
}
fn at(secs: i64) -> OffsetDateTime {
    OffsetDateTime::UNIX_EPOCH + Duration::seconds(secs)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryStore::new();
    // One confirmation is enough to ship; no chargeback window on these rails.
    let machine = OrderMachine::new(StaticPolicy::new(1, None));
    let order_id = Uuid::from_u128(0x0de1);

    // A $100.00 order. `due` is in the order's own currency and never moves.
    let mut order = Order::try_new(order_id, usd(10_000), at(0))?;

    // Rail one: a card acquirer, invoiced in the order's currency.
    order.open_attempt(Attempt::same_currency("acquirer", "inv-card", usd(10_000))?)?;
    store.insert(order)?;

    // The customer part-pays: $40.00 lands.
    pollster::block_on(ingest(
        &machine,
        &store,
        "acquirer",
        &[Settlement::Observed {
            order_id,
            provider: "acquirer".into(),
            provider_invoice_id: "inv-card".into(),
            observed_total: usd(4_000),
            tx_ref: "auth-1".into(),
            finality: Finality::Provisional { confirmations: 6 },
            at: at(10),
        }],
        b"",
        at(10),
    ))?;

    let o = pollster::block_on(store.load(order_id))?;
    println!("after $40 on the card rail: {:?}, net {}", o.status, o.net()?.minor);
    assert_eq!(o.status, OrderStatus::Underpaid);

    // ---- The acquirer drops the merchant. ---------------------------------
    // The order is untouched: it owns the truth, the processor was a driver.
    // Open a second attempt on another rail for the remaining $60.00, quoted
    // in that rail's own currency. `covers / quoted` is the exchange rate,
    // locked here and never recomputed, so the ledger stays reproducible.
    //
    // This is `OrderCatalog::open_attempt`, not a load-mutate-reinsert:
    // `insert` rejects an existing id, and `commit` only accepts ApplyResult
    // from settlement. Call it when `create_invoice` succeeds.
    pollster::block_on(store.open_attempt(
        order_id,
        Attempt::new(
            "btcpay",
            "inv-btc",
            Money::new(150_000, "BTC"), // 0.0015 BTC, quoted on the rail
            usd(6_000),                 // worth $60.00 against `due`
        )?,
    ))?;

    // The customer pays the Bitcoin invoice in full.
    pollster::block_on(ingest(
        &machine,
        &store,
        "btcpay",
        &[Settlement::Observed {
            order_id,
            provider: "btcpay".into(),
            provider_invoice_id: "inv-btc".into(),
            observed_total: Money::new(150_000, "BTC"),
            tx_ref: "chain-tx-1".into(),
            finality: Finality::Provisional { confirmations: 3 },
            at: at(20),
        }],
        b"",
        at(20),
    ))?;

    let o = pollster::block_on(store.load(order_id))?;
    println!("after 0.0015 BTC on the second rail: {:?}, net {}", o.status, o.net()?.minor);
    assert_eq!(o.status, OrderStatus::Paid, "$40 on one rail plus $60 on another is paid");

    // The machine asks to ship; it never ships by itself.
    let asked_to_ship = store
        .outbox()
        .iter()
        .any(|e| matches!(e.effect, Effect::MayFulfill));
    assert!(asked_to_ship, "a funded order emits MayFulfill");

    // `mark_fulfilled` is gated on status *and* money, so a stale instruction
    // cannot ship an underfunded order.
    let shipped = machine.mark_fulfilled(&o, at(30))?;
    println!("fulfilled: {:?}", shipped.order.status);
    assert_eq!(shipped.order.status, OrderStatus::Fulfilled);

    println!("\ntwo rails, two currencies, one order, one `due`.");
    Ok(())
}
