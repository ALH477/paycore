use paycore::{Finality, Settlement};
use time::{Duration, OffsetDateTime};

fn t(s: i64) -> OffsetDateTime {
    OffsetDateTime::UNIX_EPOCH + Duration::seconds(s)
}
use paycore_btcpay::invoice::{
    decode_webhook, settlement_from_invoice, webhook_invoice_id, webhook_needs_pull,
};

#[test]
fn expired_webhook_does_not_need_http() {
    let body = br#"{
        "type": "InvoiceExpired",
        "invoiceId": "inv-1",
        "timestamp": 1000,
        "metadata": {"orderId": "11111111-1111-1111-1111-111111111111"}
    }"#;
    assert!(!webhook_needs_pull(body).unwrap());
    assert_eq!(webhook_invoice_id(body).unwrap().as_deref(), Some("inv-1"));

    let events = decode_webhook(body, t(9_000)).unwrap();
    assert_eq!(events.len(), 1);
    match &events[0] {
        Settlement::Expired { provider, provider_invoice_id, .. } => {
            assert_eq!(provider, "btcpay");
            assert_eq!(provider_invoice_id, "inv-1");
        }
        other => panic!("expected Expired, got {other:?}"),
    }
}

#[test]
fn payload_provider_field_is_ignored() {
    let body = br#"{
        "type": "InvoiceExpired",
        "invoiceId": "inv-2",
        "provider": "evil-corp",
        "timestamp": 1000,
        "metadata": {"orderId": "11111111-1111-1111-1111-111111111111"}
    }"#;
    let events = decode_webhook(body, t(9_000)).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].provider(), "btcpay");
}

/// F24: a payment webhook carries no trustworthy amount, so it is not
/// decodable on its own. `decode_webhook` used to return `Ok(vec![])` for
/// these, which is indistinguishable from "nothing happened" — so a
/// `BtcPayBackend` wired into the generic `paycore::on_webhook` silently
/// ingested nothing for every payment: no events, no error, no dead letter.
#[test]
fn payment_webhook_refuses_to_decode_without_a_pull() {
    for kind in
        ["InvoiceSettled", "InvoiceReceivedPayment", "InvoicePaymentSettled", "InvoiceProcessing"]
    {
        let body = format!(
            r#"{{"type": "{kind}", "invoiceId": "inv-3", "timestamp": 1000, "metadata": {{"orderId": "11111111-1111-1111-1111-111111111111"}}}}"#
        );
        assert!(webhook_needs_pull(body.as_bytes()).unwrap(), "{kind} should need a pull");
        assert!(
            decode_webhook(body.as_bytes(), t(9_000)).is_err(),
            "{kind} must fail loudly rather than decode to nothing"
        );
    }
}

#[test]
fn get_invoice_settled_is_observed_final() {
    let raw = br#"{
        "id": "inv-4",
        "amount": "0.001",
        "amountPaid": "0.001",
        "currency": "BTC",
        "status": "Settled",
        "createdTime": 1000,
        "metadata": {"orderId": "11111111-1111-1111-1111-111111111111"}
    }"#;
    let settlement = settlement_from_invoice(raw, t(9_000)).unwrap();
    match settlement {
        Settlement::Observed {
            provider, provider_invoice_id, observed_total, finality, ..
        } => {
            assert_eq!(provider, "btcpay");
            assert_eq!(provider_invoice_id, "inv-4");
            assert_eq!(observed_total.minor, 100_000);
            assert_eq!(observed_total.currency, "BTC");
            assert_eq!(finality, Finality::Final);
        }
        other => panic!("expected Observed, got {other:?}"),
    }
}

#[test]
fn overpay_amount_paid_exceeds_amount() {
    let raw = br#"{
        "id": "inv-xyz",
        "amount": "0.001",
        "amountPaid": "0.002",
        "currency": "BTC",
        "status": "Settled",
        "additionalStatus": "PaidOver",
        "metadata": {"orderId": "11111111-1111-1111-1111-111111111111"}
    }"#;
    let settlement = settlement_from_invoice(raw, t(9_000)).unwrap();
    match settlement {
        Settlement::Observed { observed_total, .. } => {
            assert_eq!(observed_total.minor, 200_000);
        }
        other => panic!("expected Observed, got {other:?}"),
    }
}

#[test]
fn missing_order_id_is_error() {
    let body = br#"{
        "type": "InvoiceExpired",
        "invoiceId": "inv-5",
        "timestamp": 1000
    }"#;
    assert!(decode_webhook(body, t(9_000)).is_err());

    let raw = br#"{
        "id": "inv-6",
        "amount": "0.001",
        "currency": "BTC",
        "status": "New"
    }"#;
    assert!(settlement_from_invoice(raw, t(9_000)).is_err());
}

/// F16 (MEDIUM): a missing `createdTime` used to fall back to `UNIX_EPOCH`,
/// which is not a neutral default — `at` anchors the chargeback window, so the
/// window closed decades ago and the next clock tick promoted money that had
/// landed seconds earlier straight to `Final`. The ingest clock is the honest
/// reading of a timestamp the provider did not send.
#[test]
fn a_missing_created_time_does_not_anchor_at_the_epoch() {
    let raw = br#"{
        "id": "inv-7", "amount": "0.001", "amountPaid": "0.001",
        "currency": "BTC", "status": "Settled",
        "metadata": {"orderId": "11111111-1111-1111-1111-111111111111"}
    }"#;
    match settlement_from_invoice(raw, t(9_000)).unwrap() {
        Settlement::Observed { at, .. } => {
            assert_eq!(at, t(9_000), "no createdTime means as-of-now, not 1970");
        }
        other => panic!("expected Observed, got {other:?}"),
    }
}

/// F11: two pulls of one invoice reporting different amounts are two distinct
/// observations and must not collapse onto one idempotency row.
#[test]
fn distinct_invoice_states_get_distinct_idempotency_keys() {
    let at = |paid: &str, status: &str| {
        let raw = format!(
            r#"{{"id":"inv-8","amount":"0.001","amountPaid":"{paid}","currency":"BTC",
                 "status":"{status}","createdTime":10,
                 "metadata":{{"orderId":"11111111-1111-1111-1111-111111111111"}}}}"#
        );
        settlement_from_invoice(raw.as_bytes(), t(9_000)).unwrap().idempotency_key()
    };
    let half = at("0.0005", "Processing");
    let full = at("0.001", "Settled");
    assert_ne!(half, full, "a growing amountPaid is a new observation");
    assert_ne!(at("0.001", "Processing"), full, "so is a status change");
    assert_eq!(full, at("0.001", "Settled"), "but the same state replays onto one row");
}

/// F21: an id that would traverse out of its endpoint is rejected at decode,
/// before it can reach a request path or a btree key.
#[test]
fn a_traversing_id_is_rejected_at_decode() {
    let raw = br#"{
        "id": "../../other", "amount": "0.001", "amountPaid": "0.001",
        "currency": "BTC", "status": "Settled", "createdTime": 10,
        "metadata": {"orderId": "11111111-1111-1111-1111-111111111111"}
    }"#;
    assert!(settlement_from_invoice(raw, t(9_000)).is_err());

    let body = br#"{"type":"InvoiceExpired","invoiceId":"a/b?c",
        "timestamp":1000,"metadata":{"orderId":"11111111-1111-1111-1111-111111111111"}}"#;
    assert!(decode_webhook(body, t(9_000)).is_err());
}
