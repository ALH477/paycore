//! The driver contract.
//!
//! Verification is a type, not a doc comment. `decode` consumes a
//! `VerifiedBody`, and a `VerifiedBody` can only be built by a constructor
//! that either performs the comparison itself or requires the driver to
//! name the external scheme it used. A driver therefore cannot forget to
//! authenticate a webhook — it can only skip it in a way that is visible
//! and greppable.
//!
//! This matters because `Settlement` carries an attacker-chosen
//! `order_id`, `observed_total`, and `finality`. One unauthenticated
//! driver means anyone who can reach the webhook endpoint can mint a
//! fully-paid, `Final` observation against someone else's order.

use async_trait::async_trait;
use serde_json::Value;
use subtle::ConstantTimeEq;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::money::Money;
use crate::settlement::Settlement;

#[derive(Debug, thiserror::Error)]
pub enum PayError {
    #[error("provider unavailable")]
    Unavailable,
    #[error("invalid webhook signature")]
    BadSignature,
    #[error("unknown invoice")]
    UnknownInvoice,
    #[error("refund {0} already processed")]
    RefundIdempotent(Uuid),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Proof that a webhook body was authenticated. Field is private; the only
/// ways in are below.
pub struct VerifiedBody {
    bytes: Vec<u8>,
}

impl VerifiedBody {
    /// Symmetric schemes (BTCPay `BTCPay-Sig`, Stripe `Stripe-Signature`,
    /// most acquirer webhooks). The comparison happens here, in constant
    /// time, so a driver cannot reintroduce the timing leak with `==`.
    pub fn from_mac(body: &[u8], expected: &[u8], provided: &[u8]) -> Result<Self, PayError> {
        // MAC length is not secret, so an early length check leaks nothing.
        if expected.len() != provided.len() || expected.is_empty() {
            return Err(PayError::BadSignature);
        }
        if bool::from(expected.ct_eq(provided)) {
            Ok(Self { bytes: body.to_vec() })
        } else {
            Err(PayError::BadSignature)
        }
    }

    /// Asymmetric schemes (JWS, Ed25519) where the cryptography lives in
    /// the driver's own dependency. Naming the scheme makes bypassing
    /// verification an explicit act that shows up in review and in grep,
    /// rather than an omission that looks like ordinary code.
    pub fn from_external_verification(body: &[u8], scheme: &'static str) -> Self {
        // A real check, not a `debug_assert`: that is compiled out in release,
        // which is the only build where it would have mattered.
        assert!(!scheme.is_empty(), "name the verification scheme");
        Self { bytes: body.to_vec() }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

#[derive(Clone, Debug)]
pub struct CreateInvoice {
    pub order_id: Uuid,
    pub amount: Money,
    pub description: String,
    pub expires_at: OffsetDateTime,
    pub metadata: Value,
}

#[derive(Clone, Debug)]
pub struct Invoice {
    pub order_id: Uuid,
    pub provider: String,
    pub provider_invoice_id: String,
    /// Hosted page, BIP21 URI, BOLT11, X9.150 merchant-presented QR payload.
    pub checkout_url: Option<String>,
    pub raw: Value,
}

#[derive(Clone, Debug)]
pub struct RefundRequest {
    /// Deterministic and order-scoped. Retrying `apply` after a failed
    /// `commit`, or recomputing an overpay after a dispute win, yields the
    /// same id, so the driver's own idempotency check fires.
    pub refund_id: Uuid,
    pub provider: String,
    pub provider_invoice_id: String,
    pub amount: Money,
    pub reason: Option<String>,
}

/// A payment rail. Translates. Does not own order state.
///
/// PCI: implementations MUST NOT accept, log, persist, or return PAN,
/// magnetic-stripe data, or CVC. Hosted fields, network tokens, or a
/// redirect only. A driver that takes raw card data is out of contract and
/// puts every deployment of this crate outside SAQ-A.
#[async_trait]
pub trait PaymentBackend: Send + Sync {
    /// The value that must appear in every `Settlement::provider` this
    /// driver emits. `ingest` enforces the match.
    fn name(&self) -> &'static str;

    async fn create_invoice(&self, req: CreateInvoice) -> Result<Invoice, PayError>;

    /// Authenticate. Returning `Ok` asserts the body is from the provider.
    async fn verify(
        &self,
        headers: &[(String, String)],
        body: &[u8],
    ) -> Result<VerifiedBody, PayError>;

    /// Pure translation of an authenticated body. May yield several events.
    /// Not responsible for idempotency — that is the store's unique index.
    fn decode(&self, body: &VerifiedBody) -> Result<Vec<Settlement>, PayError>;

    async fn refund(&self, req: RefundRequest) -> Result<(), PayError>;

    /// Pull what the provider believes happened since `since`. Webhooks are
    /// rumours; this is how you find out the truth.
    async fn fetch_settlements(
        &self,
        since: OffsetDateTime,
    ) -> Result<Vec<Settlement>, PayError>;
}
