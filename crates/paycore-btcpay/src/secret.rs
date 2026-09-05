//! Secret material for the BTCPay driver. Both types zeroize on drop and
//! redact their `Debug` output so a stray `{:?}` in a log line cannot leak
//! the value.

use anyhow::anyhow;
use paycore::PayError;
use zeroize::{Zeroize, ZeroizeOnDrop};

/// The shared secret configured in BTCPay Server for webhook HMAC signing.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct WebhookSecret(Vec<u8>);

impl WebhookSecret {
    pub fn new(bytes: &[u8]) -> Result<Self, PayError> {
        if bytes.is_empty() {
            return Err(PayError::BadSignature);
        }
        Ok(Self(bytes.to_vec()))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Debug for WebhookSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WebhookSecret([redacted])")
    }
}

/// The Greenfield API key used to call the BTCPay Server REST API.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct ApiToken(String);

impl ApiToken {
    pub fn new(token: impl Into<String>) -> Result<Self, PayError> {
        let token = token.into();
        if token.is_empty() {
            return Err(PayError::Other(anyhow!("empty api token")));
        }
        Ok(Self(token))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for ApiToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ApiToken([redacted])")
    }
}
