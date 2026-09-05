//! Integer minor units with checked arithmetic and a monotone join.

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

use crate::machine::MachineError;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Money {
    /// Integer minor units. Never floats. May be negative for a net.
    pub minor: i64,
    /// ISO-4217 or crypto ticker: "USD" | "BTC" | "XMR" | ...
    pub currency: String,
}

impl Money {
    pub fn new(minor: i64, currency: impl Into<String>) -> Self {
        Self { minor, currency: currency.into() }
    }

    pub fn zero(currency: impl Into<String>) -> Self {
        Self::new(0, currency)
    }

    fn same_currency(&self, other: &Money) -> Result<(), MachineError> {
        if self.currency != other.currency {
            return Err(MachineError::CurrencyMismatch {
                expected: self.currency.clone(),
                actual: other.currency.clone(),
            });
        }
        Ok(())
    }

    /// True subtraction. May go negative — a net is below zero when a
    /// reversal outruns what was observed, which happens when reconcile
    /// starts mid-history.
    pub fn sub(&self, other: &Money) -> Result<Money, MachineError> {
        self.same_currency(other)?;
        let minor = self.minor.checked_sub(other.minor).ok_or(MachineError::AmountOverflow)?;
        Ok(Money::new(minor, self.currency.clone()))
    }

    pub fn add(&self, other: &Money) -> Result<Money, MachineError> {
        self.same_currency(other)?;
        let minor = self.minor.checked_add(other.minor).ok_or(MachineError::AmountOverflow)?;
        Ok(Money::new(minor, self.currency.clone()))
    }

    /// Monotone join for cumulative observations. Out-of-order delivery
    /// cannot walk the total backwards.
    pub fn max_join(&self, incoming: &Money) -> Result<Money, MachineError> {
        self.same_currency(incoming)?;
        Ok(Money::new(self.minor.max(incoming.minor), self.currency.clone()))
    }

    pub fn cmp_amount(&self, other: &Money) -> Result<Ordering, MachineError> {
        self.same_currency(other)?;
        Ok(self.minor.cmp(&other.minor))
    }

    /// `self * numer / denom`, floored, denominated in `numer`'s currency.
    ///
    /// This is how a rail-currency amount crosses into the order's currency
    /// and back: `numer / denom` is a locked exchange rate expressed as the
    /// ratio of two amounts, never a float. The intermediate is 128-bit, so
    /// the multiply cannot wrap before the divide; the result is then
    /// range-checked back into `i64`.
    ///
    /// Floored rather than truncated, so the rounding direction does not
    /// flip with the sign: a clawed-back amount rounds further from zero,
    /// never towards it, and the ledger never credits a fraction it did not
    /// receive.
    pub fn scale_to(&self, numer: &Money, denom: &Money) -> Result<Money, MachineError> {
        self.same_currency(denom)?;
        if denom.minor <= 0 {
            return Err(MachineError::InvalidAttempt { why: "scale denominator must be positive" });
        }
        let scaled = (self.minor as i128)
            .checked_mul(numer.minor as i128)
            .ok_or(MachineError::AmountOverflow)?
            .div_euclid(denom.minor as i128);
        let minor = i64::try_from(scaled).map_err(|_| MachineError::AmountOverflow)?;
        Ok(Money::new(minor, numer.currency.clone()))
    }

    /// The lesser of two amounts. Currency-checked, so it cannot silently
    /// compare across rails.
    pub fn min_of(&self, other: &Money) -> Result<Money, MachineError> {
        self.same_currency(other)?;
        Ok(if self.minor <= other.minor { self.clone() } else { other.clone() })
    }

    pub fn clamp_zero(&self) -> Money {
        Money::new(self.minor.max(0), self.currency.clone())
    }

    pub fn is_positive(&self) -> bool {
        self.minor > 0
    }
}
