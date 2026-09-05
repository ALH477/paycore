pub fn to_minor(s: &str, currency: &str) -> Result<i64, paycore::PayError> {
    let err = || paycore::PayError::Other(anyhow::anyhow!("bad amount {s} {currency}"));
    let scale: usize = match currency {
        "BTC" => 8,
        "SATS" => 0,
        "USD" | "EUR" => 2,
        _ => return Err(err()),
    };
    if s.is_empty() || s.starts_with('-') || s.starts_with('+') {
        return Err(err());
    }
    let (whole, frac) = match s.split_once('.') {
        Some((w, f)) => (w, f),
        None => (s, ""),
    };
    if whole.is_empty() || !whole.bytes().all(|b| b.is_ascii_digit()) {
        return Err(err());
    }
    if !frac.bytes().all(|b| b.is_ascii_digit()) {
        return Err(err());
    }
    if frac.len() > scale {
        return Err(err());
    }
    let mut digits = whole.to_string();
    digits.push_str(frac);
    for _ in 0..(scale - frac.len()) {
        digits.push('0');
    }
    digits.parse::<i64>().map_err(|_| err())
}

pub fn from_minor(n: i64, currency: &str) -> Result<String, paycore::PayError> {
    let err = || paycore::PayError::Other(anyhow::anyhow!("bad amount {n} {currency}"));
    if n < 0 {
        return Err(err());
    }
    let scale: usize = match currency {
        "BTC" => 8,
        "SATS" => 0,
        "USD" | "EUR" => 2,
        _ => return Err(err()),
    };
    if scale == 0 {
        return Ok(n.to_string());
    }
    let mut digits = n.to_string();
    while digits.len() <= scale {
        digits.insert(0, '0');
    }
    let split = digits.len() - scale;
    let whole = &digits[..split];
    let mut frac = digits[split..].to_string();
    while frac.ends_with('0') {
        frac.pop();
    }
    if frac.is_empty() {
        Ok(whole.to_string())
    } else {
        Ok(format!("{whole}.{frac}"))
    }
}
