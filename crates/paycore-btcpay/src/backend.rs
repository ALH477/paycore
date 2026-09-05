//! BTCPay [`PaymentBackend`]: HMAC verify, webhook decode, Greenfield pull/create/refund.
//!
//! The API token is held so a real HTTP client can attach it; this module
//! never puts it in a JSON body or query string.

use std::fmt;

use async_trait::async_trait;
use serde_json::Value;
use time::OffsetDateTime;

use crate::greenfield::Greenfield;
use crate::hmac_sig::verify_btcpay_sig;
use crate::invoice::{
    decode_webhook, safe_invoice_id, settlement_from_invoice, webhook_invoice_id,
    webhook_needs_pull,
};
use crate::minor::from_minor;
use crate::secret::{ApiToken, WebhookSecret};
use paycore::{
    ingest, CreateInvoice, FulfillmentPolicy, Invoice, OrderMachine, OrderStore, PayError,
    PaymentBackend, RefundRequest, Settlement, VerifiedBody,
};

pub struct BtcPayBackend<G: Greenfield> {
    pub store_id: String,
    secret: WebhookSecret,
    token: ApiToken,
    pub http: G,
}

impl<G: Greenfield> BtcPayBackend<G> {
    pub fn new(
        store_id: impl Into<String>,
        secret: WebhookSecret,
        token: ApiToken,
        http: G,
    ) -> Self {
        Self { store_id: store_id.into(), secret, token, http }
    }

    pub fn name(&self) -> &'static str {
        "btcpay"
    }
}

impl<G: Greenfield> BtcPayBackend<G> {
    /// Whether a pull payment named `marker` already exists on this store.
    /// Archived ones count: a completed refund is still a refund.
    async fn pull_payment_exists(&self, marker: &str) -> Result<bool, PayError> {
        let path = format!("/api/v1/stores/{}/pull-payments?includeArchived=true", self.store_id);
        let raw = self.http.get(&path).await?;
        let items: Vec<Value> = serde_json::from_slice(&raw).map_err(parse_err)?;
        Ok(items.iter().any(|i| i.get("name").and_then(|v| v.as_str()) == Some(marker)))
    }
}

impl<G: Greenfield> fmt::Debug for BtcPayBackend<G> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BtcPayBackend")
            .field("store_id", &self.store_id)
            .field("secret", &self.secret)
            .field("token", &self.token)
            .finish_non_exhaustive()
    }
}

fn parse_err(e: impl std::fmt::Display) -> PayError {
    PayError::Other(anyhow::anyhow!("bad btcpay payload: {e}"))
}

#[async_trait]
impl<G: Greenfield> PaymentBackend for BtcPayBackend<G> {
    fn name(&self) -> &'static str {
        BtcPayBackend::name(self)
    }

    async fn create_invoice(&self, req: CreateInvoice) -> Result<Invoice, PayError> {
        let amount = from_minor(req.amount.minor, &req.amount.currency)?;
        // Greenfield has no top-level `expiry`. It takes
        // `checkout.expirationMinutes`, in *minutes from now* — a unix
        // timestamp there was silently ignored and every invoice quietly got
        // the store default (15 minutes) instead of `req.expires_at`.
        let minutes = (req.expires_at - OffsetDateTime::now_utc()).whole_minutes().max(1);
        let payload = serde_json::json!({
            "amount": amount,
            "currency": req.amount.currency,
            "metadata": { "orderId": req.order_id },
            "checkout": { "expirationMinutes": minutes },
        });
        let body = serde_json::to_vec(&payload).map_err(parse_err)?;
        let path = format!("/api/v1/stores/{}/invoices", self.store_id);
        let raw = self.http.post(&path, &body).await?;
        let parsed: Value = serde_json::from_slice(&raw).map_err(parse_err)?;
        let id = parsed.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
        if id.is_empty() {
            return Err(PayError::Other(anyhow::anyhow!("empty invoice id")));
        }
        let checkout_url = parsed.get("checkoutLink").and_then(|v| v.as_str()).map(str::to_string);
        Ok(Invoice {
            order_id: req.order_id,
            provider: "btcpay".into(),
            provider_invoice_id: id,
            checkout_url,
            raw: parsed,
        })
    }

    async fn verify(
        &self,
        headers: &[(String, String)],
        body: &[u8],
    ) -> Result<VerifiedBody, PayError> {
        verify_btcpay_sig(self.secret.as_bytes(), headers, body)
    }

    /// Only the endings decode standalone. Every payment event needs the
    /// Greenfield pull that [`on_btcpay_webhook`] performs, and `decode_webhook`
    /// returns an error for those rather than an empty list, so wiring this
    /// backend into the generic `paycore::on_webhook` fails loudly instead of
    /// silently ingesting nothing.
    fn decode(&self, body: &VerifiedBody) -> Result<Vec<Settlement>, PayError> {
        decode_webhook(body.as_bytes(), OffsetDateTime::now_utc())
    }

    /// Idempotent on `refund_id`, which the core requires: `refund_excess_id`
    /// is deterministic and deliberately reissues the same id across distinct
    /// outbox rows (an observation and a later dispute win compute the same
    /// one), and `drain_once` calls this once per row.
    ///
    /// A Greenfield refund creates a *pull payment* carrying the `name` we set,
    /// so the store's pull-payment list is the record of what has already been
    /// refunded. Checking it first is what keeps two rows carrying one id from
    /// becoming two payouts.
    async fn refund(&self, req: RefundRequest) -> Result<(), PayError> {
        let invoice_id = safe_invoice_id(&req.provider_invoice_id)?;
        let marker = req.refund_id.to_string();

        if self.pull_payment_exists(&marker).await? {
            return Err(PayError::RefundIdempotent(req.refund_id));
        }

        let amount = from_minor(req.amount.minor, &req.amount.currency)?;
        // `refundVariant` is the only required field, and a custom amount is
        // carried by `customAmount`/`customCurrency`. The previous body was
        // `{"name", "amount"}` — no `refundVariant`, and `amount` is not a
        // field of this endpoint, so every refund was a 400.
        let payload = serde_json::json!({
            "refundVariant": "Custom",
            "customAmount": amount,
            "customCurrency": req.amount.currency,
            "name": marker,
            "description": req.reason.clone().unwrap_or_else(|| "excess".to_string()),
        });
        let body = serde_json::to_vec(&payload).map_err(parse_err)?;
        // Not under /stores/{id}: the refund endpoint is invoice-scoped.
        let path = format!("/api/v1/invoices/{invoice_id}/refund");
        self.http.post(&path, &body).await?;
        Ok(())
    }

    /// Paged, and tolerant of a row it cannot read.
    ///
    /// This is the mechanism that is supposed to find out the truth after the
    /// webhooks have lied, so neither silently truncating it at one page nor
    /// letting a single undecodable invoice abort the whole sweep is
    /// acceptable. `ingest` already takes the same line for events; this now
    /// matches it.
    async fn fetch_settlements(&self, since: OffsetDateTime) -> Result<Vec<Settlement>, PayError> {
        const PAGE: usize = 100;
        let now = OffsetDateTime::now_utc();
        let mut out = Vec::new();
        let mut skip = 0usize;

        loop {
            let path = format!(
                "/api/v1/stores/{}/invoices?startDate={}&skip={}&take={}",
                self.store_id,
                since.unix_timestamp(),
                skip,
                PAGE
            );
            let raw = self.http.get(&path).await?;
            let items: Vec<Value> = serde_json::from_slice(&raw).map_err(parse_err)?;
            let received = items.len();

            for item in items {
                let bytes = match serde_json::to_vec(&item) {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                // An invoice this driver cannot read — no orderId, a currency
                // it has no scale for — is not this sweep's problem. Skip it;
                // the rest of the history still reconciles.
                if let Ok(s) = settlement_from_invoice(&bytes, now) {
                    out.push(s);
                }
            }

            if received < PAGE {
                return Ok(out);
            }
            skip += received;
        }
    }
}

/// Authenticate a BTCPay webhook, decode it, and GET the invoice when the
/// event type has no trustworthy amount. HTTP failures from that pull
/// propagate — they must not look like a successful ingest.
pub async fn on_btcpay_webhook<G, P, S>(
    backend: &BtcPayBackend<G>,
    machine: &OrderMachine<P>,
    store: &S,
    headers: &[(String, String)],
    body: &[u8],
    now: OffsetDateTime,
) -> Result<(), PayError>
where
    G: Greenfield,
    P: FulfillmentPolicy,
    S: OrderStore,
{
    let verified = backend.verify(headers, body).await?;
    let needs_pull = webhook_needs_pull(verified.as_bytes())?;
    // `decode` rejects the pull-required types outright, so ask it only for
    // the endings it can actually translate.
    let mut events =
        if needs_pull { Vec::new() } else { decode_webhook(verified.as_bytes(), now)? };
    if needs_pull {
        if let Some(id) = webhook_invoice_id(verified.as_bytes())? {
            let id = safe_invoice_id(&id)?;
            let path = format!("/api/v1/stores/{}/invoices/{}", backend.store_id, id);
            let raw = backend.http.get(&path).await?;
            events.push(settlement_from_invoice(&raw, now)?);
        }
    }
    ingest(machine, store, backend.name(), &events, body, now)
        .await
        .map_err(|e| PayError::Other(e.into()))
}
