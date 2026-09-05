#[test]
fn no_bypass_and_no_pan_words_in_driver() {
    for path in [
        include_str!("../src/backend.rs"),
        include_str!("../src/hmac_sig.rs"),
        include_str!("../src/invoice.rs"),
        include_str!("../src/greenfield.rs"),
        include_str!("../src/secret.rs"),
        include_str!("../src/minor.rs"),
    ] {
        assert!(!path.contains("from_external_verification"));
        for ban in ["PAN", "CVV", "CVC", "card_number", "track2"] {
            assert!(!path.contains(ban), "{ban}");
        }
    }
}
