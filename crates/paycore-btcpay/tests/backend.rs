use std::collections::HashMap;
use std::sync::Mutex;

use paycore::{
    Attempt, CreateInvoice, MemoryStore, Money, Order, OrderMachine, OrderStatus, OrderStore,
    PayError, PaymentBackend, StaticPolicy,
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
            post_resp: br#"{"id":"inv-new","checkoutLink":"https://pay.example/i/inv-new"}"#.to_vec(),
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
        self.gets
            .lock()
            .unwrap()
            .get(path)
            .cloned()
            .ok_or(PayError::Unavailable)
    }

    async fn post(&self, path: &str, json_body: &[u8]) -> Result<Vec<u8>, PayError> {
        self.posts
            .lock()
            .unwrap()
            .push((path.to_string(), json_body.to_vec()));
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
    let h = vec![(
        "BTCPay-Sig".into(),
        format!("sha256={}", hex::encode(mac)),
    )];
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
    let err = pollster::block_on(on_btcpay_webhook(
        &backend,
        &machine(),
        &store,
        &[],
        body,
        t(10),
    ));
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
    pollster::block_on(on_btcpay_webhook(
        &backend,
        &machine(),
        &store,
        &headers,
        body,
        t(10),
    ))
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
    pollster::block_on(on_btcpay_webhook(
        &backend,
        &machine(),
        &store,
        &headers,
        body,
        t(10),
    ))
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
    assert_eq!(
        invoice.checkout_url.as_deref(),
        Some("https://pay.example/i/inv-new")
    );

    let posts = backend.http.posts.lock().unwrap();
    assert_eq!(posts.len(), 1);
    assert_eq!(posts[0].0, "/api/v1/stores/s1/invoices");
    let body = std::str::from_utf8(&posts[0].1).unwrap();
    assert!(!body.contains(TOKEN), "{body}");
    let json: serde_json::Value = serde_json::from_slice(&posts[0].1).unwrap();
    assert_eq!(
        json["metadata"]["orderId"].as_str(),
        Some("11111111-1111-1111-1111-111111111111")
    );
}

#[test]
fn backend_debug_redacts_secrets() {
    let backend = backend(FakeHttp::new());
    let d = format!("{backend:?}");
    assert!(!d.contains("test-secret"), "{d}");
    assert!(!d.contains("tok_secret"), "{d}");
}
