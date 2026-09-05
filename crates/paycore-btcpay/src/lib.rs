#![forbid(unsafe_code)]
//! BTCPay Greenfield driver for paycore. HMAC in `hmac_sig`; secrets in `secret`.

pub mod hmac_sig;
pub mod minor;
pub mod secret;
