use paycore::PayError;
use paycore_btcpay::hmac_sig::{hmac_sha256, verify_btcpay_sig};
use paycore_btcpay::secret::WebhookSecret;

#[test]
fn webhook_secret_debug_is_redacted() {
    let s = WebhookSecret::new(b"super-secret-value").unwrap();
    let d = format!("{s:?}");
    assert!(!d.contains("super-secret"), "{d}");
    assert!(d.contains("redacted"), "{d}");
}

#[test]
fn empty_webhook_secret_is_rejected() {
    assert!(WebhookSecret::new(b"").is_err());
}

#[test]
fn rfc4231_case1() {
    let key = [0x0bu8; 20];
    let mac = hmac_sha256(&key, b"Hi There");
    assert_eq!(
        hex::encode(mac),
        "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
    );
}

#[test]
fn btcpay_sig_accepts_good_mac() {
    let secret = b"test-secret";
    let body = b"{\"invoiceId\":\"abc\"}";
    let mac = hmac_sha256(secret, body);
    let header = format!("sha256={}", hex::encode(mac));
    let v = verify_btcpay_sig(secret, &[("BTCPay-Sig".into(), header)], body).unwrap();
    assert_eq!(v.as_bytes(), body);
}

#[test]
fn btcpay_sig_rejects_wrong_mac() {
    assert!(matches!(
        verify_btcpay_sig(
            b"test-secret",
            &[("btcpay-sig".into(), "sha256=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into())],
            b"{}",
        ),
        Err(PayError::BadSignature)
    ));
}

#[test]
fn btcpay_sig_rejects_missing_header() {
    assert!(matches!(
        verify_btcpay_sig(b"test-secret", &[], b"{}"),
        Err(PayError::BadSignature)
    ));
}

#[test]
fn btcpay_sig_does_not_use_from_external_verification() {
    let src = include_str!("../src/hmac_sig.rs");
    assert!(!src.contains("from_external_verification"));
}
