# paycore

Provider-agnostic payment orchestration. Your order state machine owns
truth; processors are disposable drivers behind one trait. Swapping a
processor is a config change, not a rewrite.

This crate is the contract and the machine. Drivers stay out. Persistence
is `MemoryStore` (the commit protocol, in process) or your own `OrderStore`
(the same protocol, on a database). A driver must not become load-bearing
for correctness.

## Layout

| file | what it holds |
|---|---|
| `money.rs` | minor units, checked arithmetic, monotone join |
| `settlement.rs` | driver-reported events, finality lattice, canonical keys |
| `order.rs` | the aggregate, `derive_status`, effects |
| `backend.rs` | `PaymentBackend`, `VerifiedBody` |
| `policy.rs` | per-rail confirmation depth and chargeback windows |
| `machine.rs` | `apply`, `mark_fulfilled`, `on_clock` |
| `store.rs` | `OrderStore`, `OrderCatalog`, `ingest`, `on_webhook`, `reconcile` |
| `memory.rs` | `MemoryStore` — unique index + one-transaction commit |
| `crates/paycore-sqlite` | SQLite `OrderStore` + `OrderCatalog` (file or in-memory) |
| `crates/paycore-worker` | `drain_once` (outbox) and `tick_clock` |

## Invariants

1. **PCI.** PAN, track data, and CVV never cross the driver boundary.
   Hosted fields, network tokens, or redirect only. Every deployment stays
   in SAQ-A scope; a driver that breaks this is out of contract.
2. **Drivers observe, they do not classify.** A driver reports
   `observed_total`; under/exact/over is computed here from `due`.
3. **Authentication is a type.** `decode` consumes a `VerifiedBody`, which
   can only be built by a constructor that does a constant-time comparison
   itself or requires the driver to name the external scheme.
4. **`apply` is pure.** No clock, no RNG, no I/O. Takes `&Order`, returns a
   new one, so an error cannot publish a partial mutation. Derived ids are
   UUIDv5 over canonical, length-prefixed input.
5. **Idempotency is a unique index**, written in the same transaction as
   the order row and the outbox rows.
6. **Money is three monotone accumulators**, never one field moving both
   ways.
7. **Effects are outbox rows**, committed with the order.

### What commutes

The money accumulators are a lattice: permuting any set of accepted events
yields the same totals. `observed_total` is also idempotent;
`reversed_total` and `refunded_total` are not — summing is monotone only
because the deltas are magnitudes, and it is not idempotent, and duplicate suppression belongs to the unique index. Folding
it into `apply` would put a seen-set on the aggregate, which is an
in-process `HashSet` with extra steps.

Status is a fold over an ordered log and is path-dependent by design. Do
not assert permutation-invariance on it.

## Schema

Money, finality, and the chargeback clock are per invoice. Putting
`observed_total` on `orders` max-joins two invoices into one number and
discards the smaller payment. `seq` is the refund-allocation order;
`load` must return attempts sorted by it.

```sql
CREATE TABLE orders (
  id                      uuid PRIMARY KEY,
  status                  text        NOT NULL,
  currency                text        NOT NULL,
  due_minor               bigint      NOT NULL,
  saw_closing_reversal    boolean     NOT NULL DEFAULT false,
  reversal_closed         boolean     NOT NULL DEFAULT false,
  dispute_open            boolean     NOT NULL DEFAULT false,
  expires_at              timestamptz,
  fulfilled_at            timestamptz,
  updated_at              timestamptz NOT NULL
);

CREATE TABLE attempts (
  order_id                uuid        NOT NULL REFERENCES orders(id),
  seq                     integer     NOT NULL,  -- creation order; do not shuffle
  provider                text        NOT NULL,
  provider_invoice_id     text        NOT NULL,
  quoted_minor            bigint      NOT NULL,  -- rail currency
  quoted_currency         text        NOT NULL,
  covers_minor            bigint      NOT NULL,  -- order currency
  covers_currency         text        NOT NULL,
  observed_total_minor    bigint      NOT NULL DEFAULT 0,  -- max-joined
  reversed_total_minor    bigint      NOT NULL DEFAULT 0,  -- summed
  refunded_total_minor    bigint      NOT NULL DEFAULT 0,  -- summed
  finality                jsonb,
  last_tx_ref             text,
  expires_at              timestamptz,
  chargeback_window_ends  timestamptz,
  PRIMARY KEY (order_id, provider, provider_invoice_id)
);

-- Idempotency. order_id leads because clock and fulfil events have no
-- provider fields before the first observation; without it every
-- pre-observation expiry in the system collapses onto one row.
-- `tx_ref` holds `Settlement::key_ref()`, not `Settlement::tx_ref()`. They
-- differ for exactly one kind: a `Refunded` row is keyed on its `refund_id`,
-- because that is the identity the driver's own idempotency is keyed on and
-- a replayed refund often carries a fresh transaction reference.
CREATE TABLE processed_events (
  order_id            uuid NOT NULL REFERENCES orders(id),
  provider            text NOT NULL,
  kind                text NOT NULL,
  provider_invoice_id text NOT NULL,
  tx_ref              text NOT NULL,
  applied_at          timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (order_id, provider, kind, provider_invoice_id, tx_ref)
);

CREATE TABLE outbox (
  id          uuid PRIMARY KEY,   -- derived; safe to retry
  order_id    uuid NOT NULL REFERENCES orders(id),
  effect      jsonb NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now(),
  drained_at  timestamptz
);

CREATE TABLE dead_letters (
  id         bigserial PRIMARY KEY,
  order_id   uuid,
  event      jsonb NOT NULL,
  raw        bytea,              -- original transport bytes
  why        text  NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
```

No running-sum column in `processed_events`. `observed_total` on an event
is a snapshot the driver reports; the aggregate's is the join. A second
copy is a second source of truth that a driver can disagree with.

`commit` is one transaction: insert `processed_events` (conflict →
`Duplicate`, roll back), update `orders`, replace that order's `attempts`
rows in `seq` order, insert `outbox`. `MemoryStore` is the executable
form of this. `paycore-sqlite` is the same protocol on a real unique
index (`SqliteStore::open` / `open_in_memory`). A SQL store that updates
`orders` without rewriting `attempts` is wrong. `processed_events.kind`
is `EventKind::as_str()` (`Observed`, `Clock`, `Fulfill`, …).

Construct orders with `Order::try_new` (or `Order::new`, which panics on
negative `due`). `due.minor < 0` is `InvalidAttempt`.

## Worker

`paycore-worker::drain_once` claims undrained outbox rows:

- `MayFulfill` → `mark_fulfilled` then commit. `FulfillRejected` drains
  the row without shipping (stale instruction after a reversal).
- `RefundExcess` → `PaymentBackend::refund`, then `ingest` of
  `Settlement::Refunded`. `Unavailable` leaves the row for retry.
  `RefundIdempotent` still ingests, so a crash after the rail refunded
  but before drain still converges.
- `UnexpectedFunds` is never auto-drained. A human has to look.
- Other effects are informational and get marked drained.

`tick_clock` loads `ids_needing_clock` and applies `on_clock`.

## Writing a driver

- `name()` is the only source of `Settlement::provider`. Never read it out
  of the payload — it is index material, and `reversed_total` sums, so a
  payload-controlled value lets an attacker replay clawbacks. `ingest`
  rejects mismatches, but the driver should not create the situation.
- `verify` before `decode`. Prefer `VerifiedBody::from_mac`, which does the
  constant-time compare for you.
- Report `observed_total` as the provider's **cumulative** total for the
  invoice, not the amount of `tx_ref`. `tx_ref` is key material only.
- `Reversed.amount` and `Refunded.amount` are deltas, and both are
  magnitudes: never signed corrections. `apply` rejects a negative one
  outright — it would walk a summing accumulator backwards, and once `net`
  exceeds anything ever observed the next observation refunds the
  difference. Reverse a reversal by observing again, not by negating.
- Emit `Settlement::Refunded` after a successful refund, or
  `refunded_total` never advances and a growing overpay will be refunded
  twice.
- `refund` must be idempotent on `refund_id`, and `Settlement::Refunded`
  must carry the `refund_id` the instruction named. It, not `tx_ref`, is
  what the unique index collapses duplicate refund reports onto.
- Never accept, log, or persist PAN.

## Outbox worker

`MayFulfill` is an instruction to ship, not a fact. Call
`mark_fulfilled` — it is gated on status *and* money, so a stale
`MayFulfill` cannot mark an underfunded order shipped.

`RefundExcess` carries a deterministic `refund_id`. Call
`PaymentBackend::refund`, then feed `Settlement::Refunded` back through
`ingest`.

`UnexpectedFunds` means money arrived against an order that had already
ended — a late on-chain payment past invoice expiry, typically. It needs a
human.

## Legal

Accepting payment for your own goods keeps you a user, not a money
transmitter, under FinCEN's 2019 CVC guidance. Running this multi-tenant
for other merchants makes you a CVC payment processor, which *is* money
transmission — the processor exemption does not apply, because CVC does not
settle through a BSA-regulated clearing system. That is an MSB registration
plus state MTLs. OFAC screening applies either way, at any size, strict
liability.
