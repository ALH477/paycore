use std::collections::HashMap;
use std::sync::Mutex;

use paycore::{
    Attempt, CreateInvoice, Finality, MemoryStore, Money, Order, OrderMachine, OrderStatus,
    OrderStore, PayError, PaymentBackend, RefundRequest, StaticPolicy,
};
use paycore_btcpay::greenfield::Greenfield;
use paycore_btcpay::hmac_sig::hmac_sha256;
use paycore_btcpay::secret::{ApiToken, WebhookSecret};
use paycore_btcpay::{on_btcpay_webhook, BtcPayBackend};
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

const SECRET: &[u8] = b"test-secret";
const P: &str = "btcpay";
const TOKEN: &str = "tok_secret";

struct FakeHttp {
    gets: Mutex<HashMap<String, Vec<u8>>>,
    posts: Mutex<Vec<(String, Vec<u8>)>>,
    post_resp: Vec<u8>,
    post_err: Option<String>,
    get_panic: bool,
}

impl FakeHttp {
    fn new() -> Self {
        Self {
            gets: Mutex::new(HashMap::new()),
            posts: Mutex::new(Vec::new()),
            post_resp: br#"{"id":"inv-new","checkoutLink":"https://pay.example/i/inv-new"}"#
                .to_vec(),
            post_err: None,
            get_panic: false,
        }
    }
}

#[async_trait::async_trait]
impl Greenfield for FakeHttp {
    async fn get(&self, path: &str) -> Result<Vec<u8>, PayError> {
        if self.get_panic {
            panic!("unexpected GET {path}");
        }
        self.gets.lock().unwrap().get(path).cloned().ok_or(PayError::Unavailable)
    }

    async fn post(&self, path: &str, json_body: &[u8]) -> Result<Vec<u8>, PayError> {
        self.posts.lock().unwrap().push((path.to_string(), json_body.to_vec()));
        if let Some(err) = &self.post_err {
            if err == "unavailable" {
                return Err(PayError::Unavailable);
            }
            if err == "already" {
                return Err(PayError::RefundIdempotent(Uuid::nil()));
            }
            return Err(PayError::Other(anyhow::anyhow!("{err}")));
        }
        Ok(self.post_resp.clone())
    }
}

fn oid() -> Uuid {
    Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap()
}

fn t(s: i64) -> OffsetDateTime {
    OffsetDateTime::UNIX_EPOCH + Duration::seconds(s)
}

fn signed(body: &[u8]) -> (Vec<(String, String)>, &[u8]) {
    let mac = hmac_sha256(SECRET, body);
    let h = vec![("BTCPay-Sig".into(), format!("sha256={}", hex::encode(mac)))];
    (h, body)
}

fn seed_order() -> Order {
    let mut o = Order::new(oid(), Money::new(100_000, "BTC"), t(0));
    o.open_attempt(
        Attempt::same_currency("btcpay", "inv-xyz", Money::new(100_000, "BTC")).unwrap(),
    )
    .unwrap();
    o.status = OrderStatus::AwaitingPayment;
    o
}

fn backend(http: FakeHttp) -> BtcPayBackend<FakeHttp> {
    BtcPayBackend::new(
        "s1",
        WebhookSecret::new(SECRET).unwrap(),
        ApiToken::new(TOKEN).unwrap(),
        http,
    )
}

fn machine() -> OrderMachine<StaticPolicy> {
    OrderMachine::new(StaticPolicy::new(1, None))
}

#[test]
fn bad_sig_does_not_get_invoice() {
    let mut http = FakeHttp::new();
    http.get_panic = true;
    let backend = backend(http);
    let store = MemoryStore::new();
    store.insert(seed_order()).unwrap();
    let body = br#"{"type":"InvoiceSettled","invoiceId":"inv-xyz"}"#;
    let err = pollster::block_on(on_btcpay_webhook(&backend, &machine(), &store, &[], body, t(10)));
    assert!(matches!(err, Err(PayError::BadSignature)), "{err:?}");
}

#[test]
fn invoice_settled_pulls_observed() {
    let http = FakeHttp::new();
    http.gets.lock().unwrap().insert(
        "/api/v1/stores/s1/invoices/inv-xyz".into(),
        br#"{
            "id": "inv-xyz",
            "amount": "0.001",
            "amountPaid": "0.001",
            "currency": "BTC",
            "status": "Settled",
            "createdTime": 10,
            "metadata": {"orderId": "11111111-1111-1111-1111-111111111111"}
        }"#
        .to_vec(),
    );
    let backend = backend(http);
    let store = MemoryStore::new();
    store.insert(seed_order()).unwrap();
    let body = br#"{"type":"InvoiceSettled","invoiceId":"inv-xyz","timestamp":10}"#;
    let (headers, body) = signed(body);
    pollster::block_on(on_btcpay_webhook(&backend, &machine(), &store, &headers, body, t(10)))
        .unwrap();
    let order = pollster::block_on(store.load(oid())).unwrap();
    assert_eq!(order.net().unwrap().minor, 100_000);
    assert_eq!(order.status, OrderStatus::Paid);
}

#[test]
fn decode_ignores_payload_provider() {
    let http = FakeHttp::new();
    http.gets.lock().unwrap().insert(
        "/api/v1/stores/s1/invoices/inv-xyz".into(),
        br#"{
            "id": "inv-xyz",
            "amount": "0.001",
            "amountPaid": "0.001",
            "currency": "BTC",
            "status": "Settled",
            "metadata": {"orderId": "11111111-1111-1111-1111-111111111111"}
        }"#
        .to_vec(),
    );
    let backend = backend(http);
    let store = MemoryStore::new();
    store.insert(seed_order()).unwrap();
    let body = br#"{
        "type": "InvoiceSettled",
        "invoiceId": "inv-xyz",
        "provider": "btcpay ",
        "timestamp": 10
    }"#;
    let (headers, body) = signed(body);
    pollster::block_on(on_btcpay_webhook(&backend, &machine(), &store, &headers, body, t(10)))
        .unwrap();
    let order = pollster::block_on(store.load(oid())).unwrap();
    assert_eq!(order.status, OrderStatus::Paid);
    assert_eq!(order.attempts[0].provider, P);
}

#[test]
fn create_invoice_posts_order_id_not_token() {
    let backend = backend(FakeHttp::new());
    let req = CreateInvoice {
        order_id: oid(),
        amount: Money::new(100_000, "BTC"),
        description: "order".into(),
        expires_at: OffsetDateTime::UNIX_EPOCH + Duration::seconds(600),
        metadata: serde_json::json!({}),
    };
    let invoice = pollster::block_on(backend.create_invoice(req)).unwrap();
    assert_eq!(invoice.provider, P);
    assert_eq!(invoice.provider_invoice_id, "inv-new");
    assert_eq!(invoice.checkout_url.as_deref(), Some("https://pay.example/i/inv-new"));

    let posts = backend.http.posts.lock().unwrap();
    assert_eq!(posts.len(), 1);
    assert_eq!(posts[0].0, "/api/v1/stores/s1/invoices");
    let body = std::str::from_utf8(&posts[0].1).unwrap();
    assert!(!body.contains(TOKEN), "{body}");
    let json: serde_json::Value = serde_json::from_slice(&posts[0].1).unwrap();
    assert_eq!(json["metadata"]["orderId"].as_str(), Some("11111111-1111-1111-1111-111111111111"));
}

#[test]
fn backend_debug_redacts_secrets() {
    let backend = backend(FakeHttp::new());
    let d = format!("{backend:?}");
    assert!(!d.contains("test-secret"), "{d}");
    assert!(!d.contains("tok_secret"), "{d}");
}

/// F11 (CRITICAL): `tx_ref` was the invoice id, which is constant across every
/// pull, so all five components of the idempotency key were identical for every
/// observation of one invoice. The first commit won and every later one was
/// suppressed as a `Duplicate`.
///
/// This is the ordinary path for an on-chain invoice paid in two payments:
/// `InvoiceReceivedPayment` for the first half, `InvoiceSettled` for the rest.
/// Before the fix the order stays `Underpaid` forever, and the `Final` finality
/// that only `InvoiceSettled` carries is dropped with it, so it never ships.
#[test]
fn an_invoice_paid_in_two_payments_reaches_paid() {
    let http = FakeHttp::new();
    let put = |body: &str| {
        http.gets
            .lock()
            .unwrap()
            .insert("/api/v1/stores/s1/invoices/inv-xyz".into(), body.as_bytes().to_vec());
    };

    put(r#"{
        "id": "inv-xyz", "amount": "0.001", "amountPaid": "0.0005",
        "currency": "BTC", "status": "Processing", "createdTime": 10,
        "metadata": {"orderId": "11111111-1111-1111-1111-111111111111"}
    }"#);
    let backend = backend(http);
    let store = MemoryStore::new();
    store.insert(seed_order()).unwrap();

    let (headers, body) =
        signed(br#"{"type":"InvoiceReceivedPayment","invoiceId":"inv-xyz","timestamp":10}"#);
    pollster::block_on(on_btcpay_webhook(&backend, &machine(), &store, &headers, body, t(10)))
        .unwrap();
    let half = pollster::block_on(store.load(oid())).unwrap();
    assert_eq!(half.net().unwrap().minor, 50_000);
    assert_eq!(half.status, OrderStatus::Underpaid);

    // The rest arrives. A second pull of the same invoice reports the new
    // cumulative total and a Settled status.
    backend.http.gets.lock().unwrap().insert(
        "/api/v1/stores/s1/invoices/inv-xyz".into(),
        br#"{
            "id": "inv-xyz", "amount": "0.001", "amountPaid": "0.001",
            "currency": "BTC", "status": "Settled", "createdTime": 10,
            "metadata": {"orderId": "11111111-1111-1111-1111-111111111111"}
        }"#
        .to_vec(),
    );
    let (headers, body) =
        signed(br#"{"type":"InvoiceSettled","invoiceId":"inv-xyz","timestamp":20}"#);
    pollster::block_on(on_btcpay_webhook(&backend, &machine(), &store, &headers, body, t(20)))
        .unwrap();

    let full = pollster::block_on(store.load(oid())).unwrap();
    assert_eq!(full.net().unwrap().minor, 100_000, "the second payment must land");
    assert_eq!(full.status, OrderStatus::Paid);
    assert_eq!(
        full.attempts[0].finality,
        Some(Finality::Final),
        "InvoiceSettled's Final must not be dropped with the observation carrying it"
    );
    assert_eq!(store.dead_letters().len(), 0);
}

/// F11: a genuine replay of the *same* invoice state still collapses onto one
/// row — the fix must not trade duplicate suppression away to get here.
#[test]
fn replaying_one_invoice_state_is_still_idempotent() {
    let http = FakeHttp::new();
    http.gets.lock().unwrap().insert(
        "/api/v1/stores/s1/invoices/inv-xyz".into(),
        br#"{
            "id": "inv-xyz", "amount": "0.001", "amountPaid": "0.001",
            "currency": "BTC", "status": "Settled", "createdTime": 10,
            "metadata": {"orderId": "11111111-1111-1111-1111-111111111111"}
        }"#
        .to_vec(),
    );
    let backend = backend(http);
    let store = MemoryStore::new();
    store.insert(seed_order()).unwrap();
    let (headers, body) =
        signed(br#"{"type":"InvoiceSettled","invoiceId":"inv-xyz","timestamp":10}"#);
    for _ in 0..3 {
        pollster::block_on(on_btcpay_webhook(&backend, &machine(), &store, &headers, body, t(10)))
            .unwrap();
    }
    let o = pollster::block_on(store.load(oid())).unwrap();
    assert_eq!(o.observed().unwrap().minor, 100_000);
    assert_eq!(o.status, OrderStatus::Paid);
}

/// F14 (HIGH): the core reissues one `refund_id` across distinct outbox rows,
/// so driver-side idempotency is load-bearing. Nothing consulted `refund_id`,
/// so two rows meant two payouts. A Greenfield refund creates a pull payment
/// carrying the name we set, which is the record to check first.
#[test]
fn refund_is_idempotent_on_refund_id() {
    let refund_id = Uuid::from_u128(0xabc);
    let http = FakeHttp::new();
    // No pull payments yet.
    http.gets
        .lock()
        .unwrap()
        .insert("/api/v1/stores/s1/pull-payments?includeArchived=true".into(), b"[]".to_vec());
    let backend = backend(http);

    let req = || RefundRequest {
        refund_id,
        provider: "btcpay".into(),
        provider_invoice_id: "inv-xyz".into(),
        amount: Money::new(50_000, "BTC"),
        reason: Some("excess".into()),
    };

    pollster::block_on(backend.refund(req())).unwrap();
    let posts = backend.http.posts.lock().unwrap().clone();
    assert_eq!(posts.len(), 1);
    assert_eq!(
        posts[0].0, "/api/v1/invoices/inv-xyz/refund",
        "the refund endpoint is invoice-scoped, not under /stores/{{id}}"
    );
    let sent: serde_json::Value = serde_json::from_slice(&posts[0].1).unwrap();
    assert_eq!(sent["refundVariant"], "Custom", "refundVariant is the one required field");
    assert_eq!(sent["customAmount"], "0.0005");
    assert_eq!(sent["customCurrency"], "BTC");
    assert_eq!(sent["name"], refund_id.to_string());

    // The rail now reports that pull payment. A second drain of the same
    // refund_id must not create another one.
    backend.http.gets.lock().unwrap().insert(
        "/api/v1/stores/s1/pull-payments?includeArchived=true".into(),
        format!(r#"[{{"id":"pp1","name":"{refund_id}"}}]"#).into_bytes(),
    );
    let again = pollster::block_on(backend.refund(req()));
    assert!(matches!(again, Err(PayError::RefundIdempotent(id)) if id == refund_id), "{again:?}");
    assert_eq!(
        backend.http.posts.lock().unwrap().len(),
        1,
        "one refund_id must never become two payouts"
    );
}

/// F21 (MEDIUM): provider ids were interpolated into request paths unencoded
/// and unvalidated, so one carrying `/` or `..` retargets the request.
#[test]
fn a_traversing_invoice_id_is_rejected_before_it_reaches_a_path() {
    let http = FakeHttp::new();
    http.gets
        .lock()
        .unwrap()
        .insert("/api/v1/stores/s1/pull-payments?includeArchived=true".into(), b"[]".to_vec());
    let backend = backend(http);
    let bad = pollster::block_on(backend.refund(RefundRequest {
        refund_id: Uuid::from_u128(1),
        provider: "btcpay".into(),
        provider_invoice_id: "../../other-store/invoices/x".into(),
        amount: Money::new(1, "BTC"),
        reason: None,
    }));
    assert!(bad.is_err(), "a traversing id must not be turned into a request path");
    assert!(backend.http.posts.lock().unwrap().is_empty());
}

/// F23 (MEDIUM) + F22 (MEDIUM): reconcile is the mechanism that finds out the
/// truth after the webhooks have lied. It must not stop at one page, and one
/// invoice it cannot read must not abort the sweep.
#[test]
fn reconcile_pages_and_skips_unreadable_rows() {
    fn invoice(n: usize) -> String {
        format!(
            r#"{{"id":"inv-{n}","amount":"0.001","amountPaid":"0.001","currency":"BTC",
                 "status":"Settled","createdTime":10,
                 "metadata":{{"orderId":"11111111-1111-1111-1111-111111111111"}}}}"#
        )
    }
    let page1: Vec<String> = (0..100).map(invoice).collect();
    let http = FakeHttp::new();
    http.gets.lock().unwrap().insert(
        "/api/v1/stores/s1/invoices?startDate=0&skip=0&take=100".into(),
        format!("[{}]", page1.join(",")).into_bytes(),
    );
    // Second page: one good row, one with no orderId, one in a currency this
    // driver has no scale for.
    http.gets.lock().unwrap().insert(
        "/api/v1/stores/s1/invoices?startDate=0&skip=100&take=100".into(),
        format!(
            r#"[{},{{"id":"inv-x","amount":"1","currency":"BTC","status":"Settled"}},
                 {{"id":"inv-y","amount":"1","amountPaid":"1","currency":"XYZ","status":"Settled",
                   "metadata":{{"orderId":"11111111-1111-1111-1111-111111111111"}}}}]"#,
            invoice(100)
        )
        .into_bytes(),
    );
    let backend = backend(http);
    let got = pollster::block_on(backend.fetch_settlements(OffsetDateTime::UNIX_EPOCH)).unwrap();
    assert_eq!(got.len(), 101, "both pages, minus the two rows that cannot be read");
}

/// F31 (MEDIUM): Greenfield has no top-level `expiry`. It takes
/// `checkout.expirationMinutes`, in minutes from now. A unix timestamp under
/// the wrong key was accepted and ignored, so every invoice quietly got the
/// store's default 15-minute expiry instead of the one the caller asked for.
#[test]
fn create_invoice_sets_expiry_where_greenfield_reads_it() {
    let backend = backend(FakeHttp::new());
    let req = CreateInvoice {
        order_id: oid(),
        amount: Money::new(100_000, "BTC"),
        description: "order".into(),
        expires_at: OffsetDateTime::now_utc() + Duration::minutes(30),
        metadata: serde_json::json!({}),
    };
    pollster::block_on(backend.create_invoice(req)).unwrap();
    let posts = backend.http.posts.lock().unwrap();
    let json: serde_json::Value = serde_json::from_slice(&posts[0].1).unwrap();
    assert!(json.get("expiry").is_none(), "there is no such Greenfield field");
    let minutes = json["checkout"]["expirationMinutes"]
        .as_i64()
        .expect("expirationMinutes must be set, as a number of minutes");
    assert!((29..=30).contains(&minutes), "expected ~30 minutes, got {minutes}");
}
