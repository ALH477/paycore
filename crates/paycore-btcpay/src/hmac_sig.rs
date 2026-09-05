//! HMAC-SHA256 verification of BTCPay Server webhook signatures.
//!
//! BTCPay sends the signature in a `BTCPay-Sig` header as `sha256=<hex>`.
//! The comparison itself lives in `VerifiedBody::from_mac`, which is
//! constant-time; this module only locates and decodes the header.

use hmac::{Hmac, Mac};
use paycore::backend::VerifiedBody;
use paycore::PayError;
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub fn hmac_sha256(key: &[u8], body: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts a key of any length");
    mac.update(body);
    mac.finalize().into_bytes().into()
}

/// Verify a BTCPay Server webhook body against its `BTCPay-Sig` header.
///
/// `headers` is a case-insensitively searched list of `(name, value)`
/// pairs, matching how most HTTP frameworks hand headers to a driver.
pub fn verify_btcpay_sig(
    secret: &[u8],
    headers: &[(String, String)],
    body: &[u8],
) -> Result<VerifiedBody, PayError> {
    if secret.is_empty() {
        return Err(PayError::BadSignature);
    }

    let header_value = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("btcpay-sig"))
        .map(|(_, value)| value.as_str())
        .ok_or(PayError::BadSignature)?;

    let hex_mac = header_value
        .strip_prefix("sha256=")
        .or_else(|| header_value.strip_prefix("SHA256="))
        .ok_or(PayError::BadSignature)?;

    let provided = hex::decode(hex_mac).map_err(|_| PayError::BadSignature)?;
    let expected = hmac_sha256(secret, body);

    VerifiedBody::from_mac(body, &expected, &provided)
}
