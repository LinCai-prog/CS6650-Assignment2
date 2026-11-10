# Architecture: distributed ingestion and real leaderboards

## Components and topology

```text
HTTP + WebSocket clients
          |
          v
     AWS ALB :80
          |
   +------+------+------+------+
   |             Four ingest hosts       |
   |     HTTP servlet + real WebSocket   |
   +------+------+------+------+
          |                 ^
          v                 | per-host copy
  RabbitMQ skitrak.exchange  | skitrak.leaderboard.updates
          |                 |
  resort.1 ... resort.10     |
          |                 |
          v                 |
  Processing consumer ---- outbox dispatcher
          |
          v
        SQLite
          ^
          |
  Snapshot responder <---- snapshot requests from ingest hosts
```

RabbitMQ runs on a separate EC2 instance. Four Tomcat ingest instances are the
ALB targets. Each owns its accepted WebSocket connections and its own exclusive
backplane queue. The processing EC2 instance owns SQLite, the ordered consumers,
the outbox dispatcher and the snapshot responder. The shared `leaderboard-common`
module defines the versioned leaderboard payload used by both applications.

## Swipe-to-leaderboard sequence

1. The HTTP client submits a swipe with a stable `X-Event-ID` through the ALB.
2. The ingest servlet validates it, looks up the fixed lift vertical, borrows a
   RabbitMQ channel, publishes to `skitrak.exchange` with `resort.{id}`, waits for
   confirmation and returns the channel to its pool.
3. The resort's serial executor validates and admits the event to a bounded,
   round-robin database writer, preserving FIFO order within each resort.
4. One bounded SQLite batch transaction records each event ID, accumulates vertical, compares
   ordered top-10 entries before and after, and, if changed, stores an immutable
   versioned notification in the outbox. Duplicate events change nothing.
5. After the whole batch commits, the consumer acknowledges only contiguous
   committed delivery tags on that resort's channel; size and timer limits flush ACKs.
6. A separate dispatcher publishes pending notifications, waits for broker
   confirmation and removes their outbox rows. Failed publication remains pending.
7. The topic exchange copies each notification to every ingest host's bound queue.
   Each host sends it only to local subscribers of the matching resort and day.

## Connection locality and authoritative snapshots

This design chooses a broadcast backplane. The consumer does not hold browser
connections and never attempts to send directly to a remote server's sessions.
Every accepting host has its own notification queue; hosts do not compete for a
single queue. SQLite is not copied to the ingest hosts.

A new subscription is registered before requesting a snapshot from
`skitrak.leaderboard.snapshots`. The processing responder reads the version and
standings atomically and replies to the requesting host's exclusive callback
queue. Correlation IDs and five-second request expiry bound outstanding requests.
Changes racing snapshot creation are buffered; only versions newer than the
snapshot are subsequently sent. On broker recovery, existing subscriptions are
resynchronized. Initial synchronization supplies current state, not offline
history of every intermediate change.

This adds a bounded request/reply dependency for initial state while keeping the
high-volume live path asynchronous. A processing-host failure prevents fresh
snapshots, but it cannot silently substitute synthetic or empty local standings.

## Concurrency, recovery and deployment

The publisher reuses a connection and pooled channels with exclusive borrowing
and connection recovery. Consumer worker count is configurable; a single serial
executor per resort preserves normal application order. Current worker count is
fixed during a run; dynamic scaling remains outstanding.

A batch waits at most five milliseconds for collection, then applies each event
and captures every intermediate top-10 change before committing. A failed batch
rolls back and is retried ahead of subsequent batches. The measured configuration uses a 256 MiB page cache and
background PASSIVE checkpoints; FULL durability and an
automatic checkpoint fallback remain enabled. Batch execution and I/O can add
latency beyond the collection wait. See the consumer tuning guide for configuration.

Aggregation and the pending update commit together. A crash between broker
confirmation and outbox deletion may republish a version; ingest hosts discard
versions already accepted by each subscription. WebSocket sends are serialized
with bounded per-connection queues. Slow subscribers reconnect for a snapshot.
Protocol pings keep idle connections active without synthetic leaderboard pushes.

The ALB forwards both HTTP requests and WebSocket upgrades to port 8080. Target
stickiness, a 120-second idle timeout and health checks at `/server-v2/health`
are the intended settings in the deployment script. Live routing and upgrades
were observed; the final tests did not export all AWS attributes or prove failover. Health is 503 while the
backplane/snapshot service is unavailable. `serverId` identifies the accepting
host in health and WebSocket responses. Systemd restarts failed applications.

SQLite remains a centralized write and snapshot bottleneck. Multiple independent
outbox dispatchers or unordered consumers of one resort are not supported.
End-to-end verification must compare HTTP acceptance, unique database events,
actual leaderboard changes and subscriber delivery, alongside queue/lag metrics.
