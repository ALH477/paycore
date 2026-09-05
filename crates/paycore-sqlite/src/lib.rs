//! SQLite `OrderStore`. Same one-transaction commit protocol as
//! `MemoryStore`, backed by a real unique index instead of a `HashSet`.
//!
//! One connection behind a `std::sync::Mutex`. Every async method locks it
//! and does its work with blocking `rusqlite` calls — there is no pool and
//! no separate async runtime for SQLite here, so this is fine.

use std::sync::{Mutex, MutexGuard};

use async_trait::async_trait;
use rusqlite::{params, Connection};
use time::OffsetDateTime;
use uuid::Uuid;

use paycore::{
    ApplyResult, Attempt, CommitResult, Effect, EventKind, Finality, IdempotencyKey, Money,
    Order, OrderCatalog, OrderStatus, OrderStore, OutboxEntry, PersistError, Settlement,
};

const SCHEMA: &str = "
CREATE TABLE orders (
  id TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  currency TEXT NOT NULL,
  due_minor INTEGER NOT NULL,
  saw_closing_reversal INTEGER NOT NULL DEFAULT 0,
  reversal_closed INTEGER NOT NULL DEFAULT 0,
  dispute_open INTEGER NOT NULL DEFAULT 0,
  expires_at INTEGER,
  fulfilled_at INTEGER,
  updated_at INTEGER NOT NULL
);
CREATE TABLE attempts (
  order_id TEXT NOT NULL REFERENCES orders(id),
  seq INTEGER NOT NULL,
  provider TEXT NOT NULL,
  provider_invoice_id TEXT NOT NULL,
  quoted_minor INTEGER NOT NULL,
  quoted_currency TEXT NOT NULL,
  covers_minor INTEGER NOT NULL,
  covers_currency TEXT NOT NULL,
  observed_total_minor INTEGER NOT NULL DEFAULT 0,
  reversed_total_minor INTEGER NOT NULL DEFAULT 0,
  refunded_total_minor INTEGER NOT NULL DEFAULT 0,
  finality TEXT,
  last_tx_ref TEXT,
  expires_at INTEGER,
  chargeback_window_ends INTEGER,
  PRIMARY KEY (order_id, provider, provider_invoice_id)
);
CREATE TABLE processed_events (
  order_id TEXT NOT NULL REFERENCES orders(id),
  provider TEXT NOT NULL,
  kind TEXT NOT NULL,
  provider_invoice_id TEXT NOT NULL,
  tx_ref TEXT NOT NULL,
  applied_at INTEGER NOT NULL,
  PRIMARY KEY (order_id, provider, kind, provider_invoice_id, tx_ref)
);
CREATE TABLE outbox (
  id TEXT PRIMARY KEY,
  order_id TEXT NOT NULL REFERENCES orders(id),
  effect TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  drained_at INTEGER
);
CREATE TABLE dead_letters (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  order_id TEXT,
  event TEXT NOT NULL,
  raw BLOB,
  why TEXT NOT NULL,
  created_at INTEGER NOT NULL
);
";

/// Maps any error into the store's error type. Kept as one function so every
/// call site reads the same, rather than repeating the closure everywhere.
fn other<E: Into<anyhow::Error>>(e: E) -> PersistError {
    PersistError::Other(e.into())
}

fn to_unix(t: OffsetDateTime) -> i64 {
    t.unix_timestamp()
}

fn from_unix(t: i64) -> Result<OffsetDateTime, PersistError> {
    OffsetDateTime::from_unix_timestamp(t).map_err(other)
}

pub struct SqliteStore {
    conn: Mutex<Connection>,
}

impl SqliteStore {
    pub fn open(path: &str) -> anyhow::Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self { conn: Mutex::new(conn) })
    }

    pub fn open_in_memory() -> anyhow::Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self { conn: Mutex::new(conn) })
    }

    fn lock(&self) -> MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Number of rows ever dead-lettered. Test-only observation point —
    /// production code drains the store, not this counter.
    pub fn dead_letter_count(&self) -> usize {
        let conn = self.lock();
        conn.query_row("SELECT COUNT(*) FROM dead_letters", [], |r| r.get::<_, i64>(0))
            .unwrap_or(0) as usize
    }
}

fn load_order(conn: &Connection, id: Uuid) -> Result<Order, PersistError> {
    let id_s = id.to_string();
    let row = conn.query_row(
        "SELECT status, currency, due_minor, saw_closing_reversal, reversal_closed, \
         dispute_open, expires_at, fulfilled_at, updated_at FROM orders WHERE id = ?1",
        params![id_s],
        |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, i64>(3)?,
                r.get::<_, i64>(4)?,
                r.get::<_, i64>(5)?,
                r.get::<_, Option<i64>>(6)?,
                r.get::<_, Option<i64>>(7)?,
                r.get::<_, i64>(8)?,
            ))
        },
    );
    let (
        status_s,
        currency,
        due_minor,
        saw_closing_reversal,
        reversal_closed,
        dispute_open,
        expires_at,
        fulfilled_at,
        updated_at,
    ) = match row {
        Ok(v) => v,
        Err(rusqlite::Error::QueryReturnedNoRows) => return Err(PersistError::UnknownOrder(id)),
        Err(e) => return Err(other(e)),
    };

    let status: OrderStatus = serde_json::from_str(&status_s).map_err(other)?;
    let attempts = load_attempts(conn, &id_s)?;

    Ok(Order {
        id,
        status,
        due: Money::new(due_minor, currency),
        attempts,
        saw_closing_reversal: saw_closing_reversal != 0,
        reversal_closed: reversal_closed != 0,
        dispute_open: dispute_open != 0,
        expires_at: expires_at.map(from_unix).transpose()?,
        fulfilled_at: fulfilled_at.map(from_unix).transpose()?,
        updated_at: from_unix(updated_at)?,
    })
}

fn load_attempts(conn: &Connection, order_id: &str) -> Result<Vec<Attempt>, PersistError> {
    let mut stmt = conn
        .prepare(
            "SELECT provider, provider_invoice_id, quoted_minor, quoted_currency, \
             covers_minor, covers_currency, observed_total_minor, reversed_total_minor, \
             refunded_total_minor, finality, last_tx_ref, expires_at, chargeback_window_ends \
             FROM attempts WHERE order_id = ?1 ORDER BY seq ASC",
        )
        .map_err(other)?;
    let rows = stmt
        .query_map(params![order_id], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, i64>(4)?,
                r.get::<_, String>(5)?,
                r.get::<_, i64>(6)?,
                r.get::<_, i64>(7)?,
                r.get::<_, i64>(8)?,
                r.get::<_, Option<String>>(9)?,
                r.get::<_, Option<String>>(10)?,
                r.get::<_, Option<i64>>(11)?,
                r.get::<_, Option<i64>>(12)?,
            ))
        })
        .map_err(other)?;

    let mut out = Vec::new();
    for row in rows {
        let (
            provider,
            provider_invoice_id,
            quoted_minor,
            quoted_currency,
            covers_minor,
            covers_currency,
            observed_total_minor,
            reversed_total_minor,
            refunded_total_minor,
            finality_s,
            last_tx_ref,
            expires_at,
            chargeback_window_ends,
        ) = row.map_err(other)?;

        let finality: Option<Finality> =
            finality_s.map(|s| serde_json::from_str(&s)).transpose().map_err(other)?;

        out.push(Attempt {
            provider,
            provider_invoice_id,
            quoted: Money::new(quoted_minor, quoted_currency.clone()),
            covers: Money::new(covers_minor, covers_currency),
            observed_total: Money::new(observed_total_minor, quoted_currency.clone()),
            reversed_total: Money::new(reversed_total_minor, quoted_currency.clone()),
            refunded_total: Money::new(refunded_total_minor, quoted_currency),
            finality,
            last_tx_ref,
            expires_at: expires_at.map(from_unix).transpose()?,
            chargeback_window_ends: chargeback_window_ends.map(from_unix).transpose()?,
        });
    }
    Ok(out)
}

fn insert_order_row(conn: &Connection, order: &Order) -> Result<(), PersistError> {
    let status_s = serde_json::to_string(&order.status).map_err(other)?;
    conn.execute(
        "INSERT INTO orders (id, status, currency, due_minor, saw_closing_reversal, \
         reversal_closed, dispute_open, expires_at, fulfilled_at, updated_at) \
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
        params![
            order.id.to_string(),
            status_s,
            order.due.currency,
            order.due.minor,
            order.saw_closing_reversal as i64,
            order.reversal_closed as i64,
            order.dispute_open as i64,
            order.expires_at.map(to_unix),
            order.fulfilled_at.map(to_unix),
            to_unix(order.updated_at),
        ],
    )
    .map_err(other)?;
    Ok(())
}

fn update_order_row(conn: &Connection, order: &Order) -> Result<usize, PersistError> {
    let status_s = serde_json::to_string(&order.status).map_err(other)?;
    conn.execute(
        "UPDATE orders SET status=?1, currency=?2, due_minor=?3, saw_closing_reversal=?4, \
         reversal_closed=?5, dispute_open=?6, expires_at=?7, fulfilled_at=?8, updated_at=?9 \
         WHERE id=?10",
        params![
            status_s,
            order.due.currency,
            order.due.minor,
            order.saw_closing_reversal as i64,
            order.reversal_closed as i64,
            order.dispute_open as i64,
            order.expires_at.map(to_unix),
            order.fulfilled_at.map(to_unix),
            to_unix(order.updated_at),
            order.id.to_string(),
        ],
    )
    .map_err(other)
}

fn replace_attempts(
    conn: &Connection,
    order_id: &str,
    attempts: &[Attempt],
) -> Result<(), PersistError> {
    conn.execute("DELETE FROM attempts WHERE order_id = ?1", params![order_id]).map_err(other)?;
    for (seq, a) in attempts.iter().enumerate() {
        let finality_s = a.finality.as_ref().map(serde_json::to_string).transpose().map_err(other)?;
        conn.execute(
            "INSERT INTO attempts (order_id, seq, provider, provider_invoice_id, quoted_minor, \
             quoted_currency, covers_minor, covers_currency, observed_total_minor, \
             reversed_total_minor, refunded_total_minor, finality, last_tx_ref, expires_at, \
             chargeback_window_ends) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)",
            params![
                order_id,
                seq as i64,
                a.provider,
                a.provider_invoice_id,
                a.quoted.minor,
                a.quoted.currency,
                a.covers.minor,
                a.covers.currency,
                a.observed_total.minor,
                a.reversed_total.minor,
                a.refunded_total.minor,
                finality_s,
                a.last_tx_ref,
                a.expires_at.map(to_unix),
                a.chargeback_window_ends.map(to_unix),
            ],
        )
        .map_err(other)?;
    }
    Ok(())
}

/// `claim_outbox` reconstructs an `OutboxEntry`, but the worker only reads
/// its `id`, `order_id`, and `effect` — `idempotency_key` was consumed the
/// moment `commit` derived the row's id, so a fresh empty one here is
/// harmless filler, not a lossy roundtrip.
fn dummy_idempotency_key(order_id: Uuid) -> IdempotencyKey {
    IdempotencyKey {
        order_id,
        provider: String::new(),
        kind: EventKind::Observed,
        provider_invoice_id: String::new(),
        tx_ref: String::new(),
    }
}

fn is_constraint_violation(e: &rusqlite::Error) -> bool {
    matches!(
        e,
        rusqlite::Error::SqliteFailure(err, _)
            if err.code == rusqlite::ErrorCode::ConstraintViolation
    )
}

#[async_trait]
impl OrderStore for SqliteStore {
    async fn load(&self, id: Uuid) -> Result<Order, PersistError> {
        let conn = self.lock();
        load_order(&conn, id)
    }

    async fn commit(&self, result: ApplyResult) -> Result<CommitResult, PersistError> {
        let mut conn = self.lock();
        let tx = conn.transaction().map_err(other)?;

        let insert_res = tx.execute(
            "INSERT INTO processed_events (order_id, provider, kind, provider_invoice_id, \
             tx_ref, applied_at) VALUES (?1,?2,?3,?4,?5,?6)",
            params![
                result.key.order_id.to_string(),
                result.key.provider,
                result.key.kind.as_str(),
                result.key.provider_invoice_id,
                result.key.tx_ref,
                to_unix(result.order.updated_at),
            ],
        );
        match insert_res {
            Ok(_) => {}
            Err(e) if is_constraint_violation(&e) => {
                // Dropping `tx` here rolls the whole attempt back: the order
                // row and the outbox are exactly as they were before.
                return Ok(CommitResult::Duplicate);
            }
            Err(e) => return Err(other(e)),
        }

        let updated = update_order_row(&tx, &result.order)?;
        if updated == 0 {
            return Err(PersistError::UnknownOrder(result.order.id));
        }

        let order_id_s = result.order.id.to_string();
        replace_attempts(&tx, &order_id_s, &result.order.attempts)?;

        for entry in &result.effects {
            let effect_json = serde_json::to_string(&entry.effect).map_err(other)?;
            tx.execute(
                "INSERT INTO outbox (id, order_id, effect, created_at, drained_at) \
                 VALUES (?1,?2,?3,?4,NULL)",
                params![
                    entry.id.to_string(),
                    entry.order_id.to_string(),
                    effect_json,
                    to_unix(result.order.updated_at),
                ],
            )
            .map_err(other)?;
        }

        tx.commit().map_err(other)?;
        Ok(CommitResult::Applied)
    }

    async fn dead_letter(
        &self,
        event: &Settlement,
        raw: &[u8],
        why: String,
    ) -> Result<(), PersistError> {
        let conn = self.lock();
        let event_json = serde_json::to_string(event).map_err(other)?;
        conn.execute(
            "INSERT INTO dead_letters (order_id, event, raw, why, created_at) \
             VALUES (?1,?2,?3,?4,?5)",
            params![
                event.order_id().to_string(),
                event_json,
                raw,
                why,
                to_unix(OffsetDateTime::now_utc()),
            ],
        )
        .map_err(other)?;
        Ok(())
    }
}

#[async_trait]
impl OrderCatalog for SqliteStore {
    async fn insert(&self, order: Order) -> Result<(), PersistError> {
        let conn = self.lock();
        insert_order_row(&conn, &order)?;
        replace_attempts(&conn, &order.id.to_string(), &order.attempts)?;
        Ok(())
    }

    async fn claim_outbox(&self, limit: usize) -> Result<Vec<OutboxEntry>, PersistError> {
        let conn = self.lock();
        let mut stmt = conn
            .prepare(
                "SELECT id, order_id, effect FROM outbox WHERE drained_at IS NULL \
                 ORDER BY created_at ASC LIMIT ?1",
            )
            .map_err(other)?;
        let rows = stmt
            .query_map(params![limit as i64], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?))
            })
            .map_err(other)?;

        let mut out = Vec::new();
        for row in rows {
            let (id_s, order_id_s, effect_s) = row.map_err(other)?;
            let id = Uuid::parse_str(&id_s).map_err(other)?;
            let order_id = Uuid::parse_str(&order_id_s).map_err(other)?;
            let effect: Effect = serde_json::from_str(&effect_s).map_err(other)?;
            out.push(OutboxEntry {
                id,
                order_id,
                idempotency_key: dummy_idempotency_key(order_id),
                effect,
            });
        }
        Ok(out)
    }

    async fn mark_drained(&self, id: Uuid, at: OffsetDateTime) -> Result<(), PersistError> {
        let conn = self.lock();
        conn.execute(
            "UPDATE outbox SET drained_at=?1 WHERE id=?2",
            params![to_unix(at), id.to_string()],
        )
        .map_err(other)?;
        Ok(())
    }

    async fn ids_needing_clock(&self, now: OffsetDateTime) -> Result<Vec<Uuid>, PersistError> {
        let conn = self.lock();
        let mut stmt = conn.prepare("SELECT id FROM orders").map_err(other)?;
        let ids: Vec<String> = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .map_err(other)?
            .collect::<Result<_, _>>()
            .map_err(other)?;
        drop(stmt);

        let mut out = Vec::new();
        for id_s in ids {
            let id = Uuid::parse_str(&id_s).map_err(other)?;
            let order = load_order(&conn, id)?;
            let expired_unpaid = matches!(
                order.status,
                OrderStatus::Pending | OrderStatus::AwaitingPayment | OrderStatus::Underpaid
            ) && order.expires_at.is_some_and(|end| end <= now);
            let chargeback_due = order.attempts.iter().any(|a| {
                a.chargeback_window_ends.is_some_and(|end| end <= now)
                    && !matches!(a.finality, Some(Finality::Final))
            });
            if expired_unpaid || chargeback_due {
                out.push(id);
            }
        }
        Ok(out)
    }
}
