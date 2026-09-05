//! Mapping between BTCPay Greenfield JSON and [`Settlement`].
//!
//! `provider` is never read out of a payload — it is always the literal
//! `"btcpay"`. [`Settlement::provider`] is part of the unique index, and a
//! payload-derived value would let an attacker vary it to defeat duplicate
//! suppression, so any `provider` field present in the JSON is ignored.

use serde::Deserialize;
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::minor::to_minor;
use paycore::{Finality, Money, PayError, Settlement, MAX_ID_LEN};

/// A provider identifier that is about to be interpolated into a request path
/// and stored in a btree key. Anything outside this set is rejected rather than
/// escaped: `MAX_ID_LEN` already establishes that provider identifiers are
/// validated before they become infrastructure, and an id carrying `/`, `?`,
/// `#`, or `..` retargets the request to a different endpoint entirely.
pub fn safe_invoice_id(id: &str) -> Result<&str, PayError> {
    if id.is_empty() || id.len() > MAX_ID_LEN {
        return Err(PayError::Other(anyhow::anyhow!("invoice id has an implausible length")));
    }
    if !id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_') {
        return Err(PayError::Other(anyhow::anyhow!("invoice id has unexpected characters")));
    }
    Ok(id)
}

/// Names one *observation* of an invoice, not the invoice itself.
///
/// `tx_ref` used to be the invoice id, which is constant across every pull. All
/// five components of the idempotency key were therefore identical for every
/// observation of one invoice, so the first commit won and every later one was
/// suppressed as a `Duplicate`: an invoice paid in two payments never left
/// `Underpaid`, and the `Final` finality that only `InvoiceSettled` carries was
/// dropped with it.
///
/// Length-prefixed, for the reason `paycore`'s own `canonical` is: `status` is
/// a provider-controlled string, and a separator join is not injective.
fn observation_ref(id: &str, status: &str, paid_minor: i64) -> String {
    let paid = paid_minor.to_string();
    let mut buf = Vec::new();
    for part in [id, status, paid.as_str()] {
        buf.extend_from_slice(&(part.len() as u64).to_be_bytes());
        buf.extend_from_slice(part.as_bytes());
    }
    hex::encode(Sha256::digest(&buf))
}

const NEEDS_PULL: &[&str] =
    &["InvoiceSettled", "InvoiceReceivedPayment", "InvoicePaymentSettled", "InvoiceProcessing"];

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

/// A provider timestamp, falling back to the ingest clock.
///
/// The fallback used to be `UNIX_EPOCH`, which is not a neutral default: `at`
/// anchors the chargeback window, so a missing `createdTime` produced a window
/// that closed decades ago, and the next clock tick promoted money that landed
/// seconds earlier straight to `Final`. "As of now" is the honest reading of a
/// timestamp the provider did not send, and it errs towards keeping funds
/// reversible for longer rather than shorter.
fn at_from_unix(timestamp: Option<i64>, fallback: OffsetDateTime) -> OffsetDateTime {
    timestamp.and_then(|t| OffsetDateTime::from_unix_timestamp(t).ok()).unwrap_or(fallback)
}

/// Translate a BTCPay webhook body into zero or more settlements.
///
/// Only `InvoiceExpired` and `InvoiceInvalid` yield a settlement here — the
/// others in [`NEEDS_PULL`] carry an amount that must be fetched from the
/// Greenfield API, not trusted from the webhook, so [`webhook_needs_pull`]
/// is how a caller learns to go do that instead.
pub fn decode_webhook(body: &[u8], now: OffsetDateTime) -> Result<Vec<Settlement>, PayError> {
    let parsed: WebhookBody = serde_json::from_slice(body).map_err(parse_err)?;
    let at = at_from_unix(parsed.timestamp, now);
    let invoice_id = safe_invoice_id(&parsed.invoice_id)?.to_string();

    match parsed.kind.as_str() {
        "InvoiceExpired" => {
            let order_id = require_order_id(&parsed.metadata)?;
            Ok(vec![Settlement::Expired {
                order_id,
                provider: "btcpay".into(),
                provider_invoice_id: invoice_id,
                at,
            }])
        }
        "InvoiceInvalid" => {
            let order_id = require_order_id(&parsed.metadata)?;
            Ok(vec![Settlement::Failed {
                order_id,
                provider: "btcpay".into(),
                provider_invoice_id: invoice_id,
                code: "invalid".into(),
                at,
            }])
        }
        // These carry no trustworthy amount, so they are not decodable on
        // their own — the caller has to pull the invoice. Returning `Ok(vec![])`
        // made that indistinguishable from "nothing happened", so a
        // `BtcPayBackend` wired into the generic `paycore::on_webhook` silently
        // ingested nothing for every payment: no events, no error, no dead
        // letter. Fail loudly instead; `on_btcpay_webhook` handles the pull.
        k if NEEDS_PULL.contains(&k) => Err(PayError::Other(anyhow::anyhow!(
            "{k} carries no trustworthy amount: use on_btcpay_webhook, which pulls the invoice"
        ))),
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
pub fn settlement_from_invoice(raw: &[u8], now: OffsetDateTime) -> Result<Settlement, PayError> {
    let parsed: InvoiceBody = serde_json::from_slice(raw).map_err(parse_err)?;
    let order_id = require_order_id(&parsed.metadata)?;
    safe_invoice_id(&parsed.id)?;

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

    let tx_ref = observation_ref(&parsed.id, &parsed.status, paid);
    Ok(Settlement::Observed {
        order_id,
        provider: "btcpay".into(),
        provider_invoice_id: parsed.id,
        observed_total: Money::new(paid, parsed.currency),
        tx_ref,
        finality,
        at: at_from_unix(parsed.created_time, now),
    })
}
