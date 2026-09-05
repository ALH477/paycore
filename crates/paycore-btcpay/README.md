# paycore-btcpay

BTCPay Greenfield driver for paycore. Crypto, JSON, and HTTP are separate
modules so a webhook cannot be decoded without a `VerifiedBody`.

## Do not use `paycore::on_webhook`

BTCPay payment webhooks do not include a cumulative paid total.
`PaymentBackend::decode` therefore emits only `Expired` / `Failed`.
Observed money comes from `GET /api/v1/stores/{storeId}/invoices/{id}`
after HMAC verify. Call `on_btcpay_webhook`. Using `on_webhook` will
2xx BTCPay and never book the payment.

## Secrets

Construct explicitly. The library does not read the environment.

- `WebhookSecret::new(bytes)` — HMAC key for `BTCPay-Sig`
- `ApiToken::new(string)` — Greenfield API token (Authorization is the
  `Greenfield` impl's job, never a query string)

`Debug` prints `[redacted]`.

## Verify

`BTCPay-Sig: sha256=<hex>` compared via `VerifiedBody::from_mac` on the
raw MAC bytes. There is no `from_external_verification` in this crate.

`name()` is always `"btcpay"`. Payload `provider` is ignored.
