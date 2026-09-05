//! HTTP surface a BTCPay Greenfield client must provide. `invoice` decodes
//! what comes back; this trait only says how to get bytes in and out.

use async_trait::async_trait;
use paycore::PayError;

#[async_trait]
pub trait Greenfield: Send + Sync {
    async fn get(&self, path: &str) -> Result<Vec<u8>, PayError>;
    async fn post(&self, path: &str, json_body: &[u8]) -> Result<Vec<u8>, PayError>;
}
