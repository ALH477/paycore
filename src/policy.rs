//! Confirmation depth and chargeback windows are per-rail. Lightning 0,
//! on-chain BTC 6, ACH by calendar day, cards never from webhook finality.

use time::{Duration, OffsetDateTime};

use crate::settlement::Finality;

pub trait FulfillmentPolicy {
    fn may_fulfill(&self, provider: &str, finality: &Finality) -> bool;

    /// When the funding becomes irreversible for this rail, measured from
    /// the observation that first made the invoice whole.
    fn chargeback_window(
        &self,
        provider: &str,
        funded_at: OffsetDateTime,
    ) -> Option<OffsetDateTime>;
}

/// Uniform policy. Useful in tests and for single-rail deployments.
pub struct StaticPolicy {
    pub min_confirmations: u32,
    pub window: Option<Duration>,
}

impl StaticPolicy {
    pub fn new(min_confirmations: u32, window: Option<Duration>) -> Self {
        Self { min_confirmations, window }
    }
}

impl FulfillmentPolicy for StaticPolicy {
    fn may_fulfill(&self, _provider: &str, finality: &Finality) -> bool {
        match finality {
            Finality::Final => true,
            Finality::Provisional { confirmations } => *confirmations >= self.min_confirmations,
        }
    }

    fn chargeback_window(
        &self,
        _provider: &str,
        funded_at: OffsetDateTime,
    ) -> Option<OffsetDateTime> {
        self.window.map(|d| funded_at + d)
    }
}
