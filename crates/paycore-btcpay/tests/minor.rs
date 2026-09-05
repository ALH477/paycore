use paycore_btcpay::minor::{from_minor, to_minor};

#[test]
fn btc_to_sats() {
    assert_eq!(to_minor("0.00000001", "BTC").unwrap(), 1);
    assert_eq!(to_minor("1.00", "BTC").unwrap(), 100_000_000);
    assert_eq!(to_minor("0.001", "BTC").unwrap(), 100_000);
}

#[test]
fn usd_to_cents() {
    assert_eq!(to_minor("10.50", "USD").unwrap(), 1050);
    assert_eq!(to_minor("10", "USD").unwrap(), 1000);
}

#[test]
fn rejects_more_fractional_digits_than_scale() {
    assert!(to_minor("1.001", "USD").is_err());
}

#[test]
fn rejects_float_poison() {
    assert!(to_minor("1e8", "BTC").is_err());
    assert!(to_minor("NaN", "USD").is_err());
}

#[test]
fn no_f64_in_source() {
    let src = include_str!("../src/minor.rs");
    assert!(!src.contains("f64"));
    assert!(!src.contains("f32"));
    assert!(!src.contains("parse::<f"));
}

#[test]
fn from_minor_btc() {
    assert_eq!(from_minor(100_000, "BTC").unwrap(), "0.001");
    assert_eq!(to_minor(&from_minor(100_000, "BTC").unwrap(), "BTC").unwrap(), 100_000);
}
