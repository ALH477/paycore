//! Mapping between BTCPay Greenfield JSON and [`Settlement`].
//!
//! `provider` is never read out of a payload — it is always the literal
//! `"btcpay"`. [`Settlement::provider`] is part of the unique index, and a
//! payload-derived value would let an attacker vary it to defeat duplicate
//! suppression, so any `provider` field present in the JSON is ignored.

use serde::Deserialize;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::minor::to_minor;
use paycore::{Finality, Money, PayError, Settlement};

const NEEDS_PULL: &[&str] = &[
    "InvoiceSettled",
    "InvoiceReceivedPayment",
    "InvoicePaymentSettled",
    "InvoiceProcessing",
];

#[derive(Deserialize)]
struct Metadata {
    #[serde(rename = "orderId")]
    order_id: Option<Uuid>,
}

#[derive(Deserialize)]
struct WebhookBody {
    #[serde(rename = "type")]
    kind: String,
    #[serde(rename = "invoiceId")]
    invoice_id: String,
    timestamp: Option<i64>,
    metadata: Option<Metadata>,
}

#[derive(Deserialize)]
struct InvoiceIdOnly {
    #[serde(rename = "invoiceId")]
    invoice_id: Option<String>,
}

#[derive(Deserialize)]
struct EventType {
    #[serde(rename = "type")]
    kind: String,
}

#[derive(Deserialize)]
struct InvoiceBody {
    id: String,
    amount: String,
    #[serde(rename = "amountPaid")]
    amount_paid: Option<String>,
    currency: String,
    status: String,
    #[serde(rename = "createdTime")]
    created_time: Option<i64>,
    metadata: Option<Metadata>,
}

fn parse_err(e: impl std::fmt::Display) -> PayError {
    PayError::Other(anyhow::anyhow!("bad btcpay payload: {e}"))
}

fn require_order_id(metadata: &Option<Metadata>) -> Result<Uuid, PayError> {
    metadata
        .as_ref()
        .and_then(|m| m.order_id)
        .ok_or_else(|| PayError::Other(anyhow::anyhow!("missing metadata.orderId")))
}

fn at_from_unix(timestamp: Option<i64>) -> OffsetDateTime {
    timestamp
        .and_then(|t| OffsetDateTime::from_unix_timestamp(t).ok())
        .unwrap_or(OffsetDateTime::UNIX_EPOCH)
}

/// Translate a BTCPay webhook body into zero or more settlements.
///
/// Only `InvoiceExpired` and `InvoiceInvalid` yield a settlement here — the
/// others in [`NEEDS_PULL`] carry an amount that must be fetched from the
/// Greenfield API, not trusted from the webhook, so [`webhook_needs_pull`]
/// is how a caller learns to go do that instead.
pub fn decode_webhook(body: &[u8]) -> Result<Vec<Settlement>, PayError> {
    let parsed: WebhookBody = serde_json::from_slice(body).map_err(parse_err)?;
    let at = at_from_unix(parsed.timestamp);

    match parsed.kind.as_str() {
        "InvoiceExpired" => {
            let order_id = require_order_id(&parsed.metadata)?;
            Ok(vec![Settlement::Expired {
                order_id,
                provider: "btcpay".into(),
                provider_invoice_id: parsed.invoice_id,
                at,
            }])
        }
        "InvoiceInvalid" => {
            let order_id = require_order_id(&parsed.metadata)?;
            Ok(vec![Settlement::Failed {
                order_id,
                provider: "btcpay".into(),
                provider_invoice_id: parsed.invoice_id,
                code: "invalid".into(),
                at,
            }])
        }
        _ => Ok(vec![]),
    }
}

/// Whether a webhook of this type carries no trustworthy amount and must be
/// followed by a `GET /invoices/{id}` before it can become an `Observed`.
pub fn webhook_needs_pull(body: &[u8]) -> Result<bool, PayError> {
    let parsed: EventType = serde_json::from_slice(body).map_err(parse_err)?;
    Ok(NEEDS_PULL.contains(&parsed.kind.as_str()))
}

/// The `invoiceId` a webhook body refers to, if any.
pub fn webhook_invoice_id(body: &[u8]) -> Result<Option<String>, PayError> {
    let parsed: InvoiceIdOnly = serde_json::from_slice(body).map_err(parse_err)?;
    Ok(parsed.invoice_id)
}

/// Translate a fetched `GET /invoices/{id}` body into an `Observed`
/// settlement.
///
/// `amountPaid` is authoritative when present, including when it exceeds
/// `amount` (an overpay). Absent `amountPaid` on a non-`Settled` invoice
/// means nothing has been paid yet.
pub fn settlement_from_invoice(raw: &[u8]) -> Result<Settlement, PayError> {
    let parsed: InvoiceBody = serde_json::from_slice(raw).map_err(parse_err)?;
    let order_id = require_order_id(&parsed.metadata)?;

    let paid = if let Some(amount_paid) = &parsed.amount_paid {
        to_minor(amount_paid, &parsed.currency)?
    } else if parsed.status == "Settled" {
        to_minor(&parsed.amount, &parsed.currency)?
    } else {
        0
    };

    let finality = if parsed.status == "Settled" {
        Finality::Final
    } else {
        Finality::Provisional { confirmations: 0 }
    };

    Ok(Settlement::Observed {
        order_id,
        provider: "btcpay".into(),
        provider_invoice_id: parsed.id.clone(),
        observed_total: Money::new(paid, parsed.currency),
        tx_ref: parsed.id,
        finality,
        at: at_from_unix(parsed.created_time),
    })
}
