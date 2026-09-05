use paycore::{Finality, Settlement};
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
    assert_eq!(webhook_needs_pull(body).unwrap(), false);
    assert_eq!(webhook_invoice_id(body).unwrap().as_deref(), Some("inv-1"));

    let events = decode_webhook(body).unwrap();
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
    let events = decode_webhook(body).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].provider(), "btcpay");
}

#[test]
fn payment_webhook_has_no_observed() {
    for kind in [
        "InvoiceSettled",
        "InvoiceReceivedPayment",
        "InvoicePaymentSettled",
        "InvoiceProcessing",
    ] {
        let body = format!(
            r#"{{"type": "{kind}", "invoiceId": "inv-3", "timestamp": 1000, "metadata": {{"orderId": "11111111-1111-1111-1111-111111111111"}}}}"#
        );
        let events = decode_webhook(body.as_bytes()).unwrap();
        assert!(events.is_empty(), "{kind} produced {events:?}");
        assert!(webhook_needs_pull(body.as_bytes()).unwrap(), "{kind} should need a pull");
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
    let settlement = settlement_from_invoice(raw).unwrap();
    match settlement {
        Settlement::Observed { provider, provider_invoice_id, observed_total, finality, .. } => {
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
    let settlement = settlement_from_invoice(raw).unwrap();
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
    assert!(decode_webhook(body).is_err());

    let raw = br#"{
        "id": "inv-6",
        "amount": "0.001",
        "currency": "BTC",
        "status": "New"
    }"#;
    assert!(settlement_from_invoice(raw).is_err());
}
