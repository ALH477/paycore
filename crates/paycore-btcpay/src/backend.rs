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
    decode_webhook, settlement_from_invoice, webhook_invoice_id, webhook_needs_pull,
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
    pub fn new(store_id: impl Into<String>, secret: WebhookSecret, token: ApiToken, http: G) -> Self {
        Self {
            store_id: store_id.into(),
            secret,
            token,
            http,
        }
    }

    pub fn name(&self) -> &'static str {
        "btcpay"
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
        let expiry = req.expires_at.unix_timestamp().max(1);
        let payload = serde_json::json!({
            "amount": amount,
            "currency": req.amount.currency,
            "metadata": { "orderId": req.order_id },
            "expiry": expiry,
        });
        let body = serde_json::to_vec(&payload).map_err(parse_err)?;
        let path = format!("/api/v1/stores/{}/invoices", self.store_id);
        let raw = self.http.post(&path, &body).await?;
        let parsed: Value = serde_json::from_slice(&raw).map_err(parse_err)?;
        let id = parsed
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if id.is_empty() {
            return Err(PayError::Other(anyhow::anyhow!("empty invoice id")));
        }
        let checkout_url = parsed
            .get("checkoutLink")
            .and_then(|v| v.as_str())
            .map(str::to_string);
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

    fn decode(&self, body: &VerifiedBody) -> Result<Vec<Settlement>, PayError> {
        decode_webhook(body.as_bytes())
    }

    async fn refund(&self, req: RefundRequest) -> Result<(), PayError> {
        let amount = from_minor(req.amount.minor, &req.amount.currency)?;
        let payload = serde_json::json!({
            "name": req.refund_id.to_string(),
            "amount": amount,
        });
        let body = serde_json::to_vec(&payload).map_err(parse_err)?;
        let path = format!(
            "/api/v1/stores/{}/invoices/{}/refund",
            self.store_id, req.provider_invoice_id
        );
        match self.http.post(&path, &body).await {
            Ok(_) => Ok(()),
            Err(PayError::RefundIdempotent(id)) => Err(PayError::RefundIdempotent(id)),
            Err(PayError::Unavailable) => Err(PayError::Unavailable),
            Err(e) => Err(e),
        }
    }

    async fn fetch_settlements(
        &self,
        since: OffsetDateTime,
    ) -> Result<Vec<Settlement>, PayError> {
        let path = format!(
            "/api/v1/stores/{}/invoices?startDate={}",
            self.store_id,
            since.unix_timestamp()
        );
        let raw = self.http.get(&path).await?;
        let items: Vec<Value> = serde_json::from_slice(&raw).map_err(parse_err)?;
        let mut out = Vec::new();
        for item in items {
            let bytes = serde_json::to_vec(&item).map_err(parse_err)?;
            match settlement_from_invoice(&bytes) {
                Ok(s) => out.push(s),
                Err(PayError::Other(e)) if e.to_string().contains("missing metadata.orderId") => {
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        Ok(out)
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
    let mut events = backend.decode(&verified)?;
    if let Some(id) = webhook_invoice_id(verified.as_bytes())? {
        if webhook_needs_pull(verified.as_bytes())? {
            let path = format!("/api/v1/stores/{}/invoices/{}", backend.store_id, id);
            let raw = backend.http.get(&path).await?;
            events.push(settlement_from_invoice(&raw)?);
        }
    }
    ingest(machine, store, backend.name(), &events, body, now)
        .await
        .map_err(|e| PayError::Other(e.into()))
}
