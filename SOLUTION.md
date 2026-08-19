# SOLUTION.md

## What was broken, and why

### 1. Race condition in dedup — TOCTOU vulnerability

The `Ingest` method used a check-then-act pattern: `EventExists()` followed by
`InsertEvent()`. Two concurrent deliveries of the same `event_id` could both
pass the existence check before either insert landed, creating duplicate event
rows. The `events` table had only a non-unique index on `event_id`, not a
`UNIQUE` constraint, so Postgres never rejected the second insert.

**Impact:** Duplicate call records in the dashboard and inflated
`account_stats.call_count` / `total_duration_sec` — every duplicate delivery
added another +1 / +duration.

### 2. In-memory stats cache — missing mutex on writes

`Cache.Get()` correctly acquired `RLock`, but `Cache.Record()` read and wrote
the underlying map with no lock at all. Under concurrent webhook deliveries
this produced:

- **Lost increments** (`CallCount++` is a non-atomic read-modify-write)
- **Potential crash** (Go's runtime detects concurrent map access)

The existing test (`TestCacheRecordAccumulates`) only exercised serial
access and therefore passed.

### 3. Recording processing — cancelled context and silent error

The recording goroutine received the HTTP request's `context.Context`. By the
time the goroutine reached `MarkRecordingProcessed`, the handler had already
written a 200 and the context was cancelled. The DB update failed with
`context.Canceled`, but the error was caught by a bare `// TODO: handle`
comment and never logged — matching the symptom "nothing in the logs about it."

### 4. In-flight goroutines lost on deploy

Recording goroutines were fire-and-forget (`go func() { … }()`). The server
had graceful HTTP shutdown (`srv.Shutdown`), but nothing tracked or waited for
background goroutines. On `SIGTERM` the process exited immediately, killing
any goroutine mid-work — "whatever was in flight seems to just disappear."

---

## Deduplication strategy — and alternatives I considered

I chose a **two-layer approach**: Redis `SET NX` as a fast path, backed by a
Postgres `UNIQUE` constraint as the durable guarantee.

### Why Redis SET NX (Layer 1)

- **Speed**: Redeliveries are rejected in ~0.1 ms with a single Redis
  round-trip, never touching Postgres.
- **TTL**: The key expires after 24 hours, which comfortably covers the
  provider's retry window without unbounded memory growth.
- **Graceful degradation**: If Redis is down, the code logs a warning and
  falls through to the Postgres layer — availability is not sacrificed.

### Why Postgres UNIQUE + ON CONFLICT (Layer 2)

- **Durability**: Redis is volatile. If it restarts (or the 24 h TTL elapses
  and the provider retries very late), the UNIQUE constraint is the last line
  of defence.
- **Atomicity**: `INSERT … ON CONFLICT (event_id) DO NOTHING` returns
  `RowsAffected() == 0` for duplicates, so the caller knows whether to
  proceed with stats — no TOCTOU window.

### Alternatives considered

| Approach | Why I didn't choose it |
|---|---|
| **Postgres-only dedup** (UNIQUE constraint, no Redis) | Every redelivery still executes an INSERT round-trip. Under high retry rates this adds unnecessary Postgres load. Redis eliminates those round-trips. |
| **Redis-only dedup** | Not durable. A Redis restart or key expiry silently re-opens the dedup window. Acceptable only if losing a few events is tolerable — it isn't here, because stats must be exact. |
| **Separate idempotency table** | Same durability as the UNIQUE constraint but adds schema complexity and a second write per event. The constraint on `events` already does the job. |
| **Application-level distributed lock** | Overkill for single-key dedup and adds latency. SET NX is effectively a distributed lock scoped to one event_id. |

---

## What I would change for 10,000 webhooks/second

1. **Batch inserts with a write buffer.**  
   Buffer incoming events in memory (bounded channel) and flush to Postgres
   in batches of e.g. 500 rows using `COPY` or multi-row `INSERT`. This
   amortises per-row overhead and reduces connection contention.

2. **Message queue for recording processing.**  
   Replace fire-and-forget goroutines with a durable queue (Redis Streams,
   Kafka, or SQS). This decouples ingestion throughput from downstream
   processing latency, provides automatic retries, and survives restarts
   without a `WaitGroup`.

3. **Connection pooling and tuning.**  
   Increase `pgxpool` max connections, tune `idle_in_transaction_session_timeout`,
   and consider PgBouncer in transaction mode in front of Postgres.

4. **Redis Cluster for dedup.**  
   A single Redis node becomes a bottleneck and a single point of failure.
   Redis Cluster shards keys across nodes and provides replication.

5. **Partitioned events table.**  
   Partition `events` by `received_at` (e.g. daily) so that the UNIQUE index
   stays small and old data can be archived cheaply with `DROP PARTITION`.

6. **Horizontal scaling.**  
   Run multiple service replicas behind a load balancer. The dedup layers
   (Redis + Postgres UNIQUE) are already safe under concurrent access from
   multiple processes.

7. **Rate limiting and back-pressure.**  
   Add a token-bucket rate limiter at the HTTP layer to protect Postgres from
   bursts, and return `429 Too Many Requests` so the provider backs off.
