> Superseded for deployment topology by [INGEST_WEBSOCKETS.md](INGEST_WEBSOCKETS.md). Change detection and outbox semantics below remain applicable; initial snapshots are now requested over RabbitMQ by the ingest hosts.

# Conditional leaderboard updates

## Behavior

For each new event, SQLite updates skier totals and compares the ordered top 10
before and after the update. Membership, ordering and score changes count as a
leaderboard change. A below-cutoff score change is still saved but creates no
notification. Duplicate event IDs change neither totals nor notifications.

Each changed leaderboard is saved as an immutable, versioned payload in
`leaderboard_outbox` in the same transaction as the event and totals. One
`LeaderboardOutboxDispatcher` per aggregation database publishes pending payloads
in insertion order, waits for RabbitMQ confirms, and then removes the pending row.
Publication failures leave the row pending. The dispatcher recreates its channel
when necessary. A crash after publish but before deleting a pending row can
republish the same version; WebSocket servers suppress already-sent versions per
connection and resort/day.

Each WebSocket process declares its own exclusive, auto-delete queue bound to
`skitrak.leaderboard.updates` with `leaderboard.*.*`. The topic exchange sends a
copy to each bound queue. Initial snapshots read standings and version from a
single SQLite snapshot. Older queued changes cannot move a client backward.
Snapshots and delivery are current-state synchronization, not an offline event
history: a disconnected subscriber may recover the latest state rather than every
intermediate leaderboard.

## Upgrade

- Build and upgrade the consumer and WebSocket applications together. Old
  notifications without captured standings are rejected by the new subscriber.
- Stop both old services before installing the replacement JAR. Preserve
  `data/leaderboard.db`; the new tables are added automatically and existing
  event IDs and skier totals remain intact.
- Start the new WebSocket service, then the consumer service, then subscribers.
- Inspect the old `skitrak.websocket.updates` queue after the upgrade. It is no
  longer consumed, and its old binding would keep accumulating notifications.
  After confirming that no old service needs it, remove its binding or retire the
  queue in RabbitMQ. The application does not delete existing queues for you.
- Rebuild the client before testing. Message counts are no longer expected to
  match swipe counts. It reports distinct changed standings, invalid payloads,
  and duplicate/older versions separately.

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `LEADERBOARD_DB_PATH` | `data/leaderboard.db` | Authoritative SQLite file used by both applications |
| `WS_BIND_HOST` | `127.0.0.1` | Set appropriately when exposing the service to an ALB |
| `WS_PORT` | `8081` | WebSocket listener port |
| `HTTP_BASE_URL` | Existing ingest ALB URL | Client HTTP endpoint |
| `WS_BASE_URL` | `ws://127.0.0.1:18081` | Client WebSocket endpoint; set to the ALB endpoint for ALB testing |

The current authoritative snapshot implementation reads the same local SQLite
file as the consumer. Multiple WebSocket processes on the processing host must
use that same file. Although live updates are now copied to each WebSocket queue,
deploying WebSocket servers on other hosts also needs an authoritative snapshot
API or shared database implementation. Independent empty local SQLite files are
not a replacement for the authoritative snapshot source.

Do not run multiple outbox dispatchers against the same database: dispatch
ownership is currently single-process. Resort queues likewise retain one ordered
consumer each. This change does not implement dynamic consumer scaling or modify
AWS resources. ALB routing, health checks, stickiness, timeout, failover and the
assignment's large benchmark matrix still need deployment configuration and
verification.

## Measurements and local verification

The consumer logs processed unique swipes, duplicate swipes, actual leaderboard
changes, and source-to-aggregation-start lag every five seconds. These counters
are per-process and reset on restart. Lag samples require parseable ISO-8601
timestamps with a timezone and synchronized machine clocks. `serverTimestamp`
continues to measure WebSocket delivery; `sourceTimestamp` and
`aggregatedTimestamp` are retained separately.

Run consumer tests with Java 11 and Maven (`cd consumer; mvn test`). If Mockito
cannot attach its test agent in your environment, supply its cached Byte Buddy
agent explicitly with Maven's `argLine` option. No real broker is used by these
tests; RabbitMQ channels and WebSocket connections are mocked, while SQLite tests
use temporary databases.

The dependency-free client checks can be run with:

```sh
cd client-part2
mvn test-compile
java -ea -cp target/classes:target/test-classes websocket.LeaderboardUpdateTrackerTest
```

The deterministic pipeline test processes twelve unique swipes plus one replay:
ten initial leaders, one below-cutoff skier, and one score increase for the leader.
It verifies eleven leaderboard deliveries, correct totals, and no replay update.
