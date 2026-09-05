#![forbid(unsafe_code)]
//! BTCPay Greenfield driver for paycore. HMAC in `hmac_sig`; secrets in `secret`.

pub mod backend;
pub mod greenfield;
pub mod hmac_sig;
pub mod invoice;
pub mod minor;
pub mod secret;

pub use backend::{on_btcpay_webhook, BtcPayBackend};
