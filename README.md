# CS6650 Assignment 2: real distributed leaderboards

## Data flow

```text
Client -> ALB -> [four ingest servers] -> RabbitMQ -> Consumer
                                                        |
                                       Aggregation + outbox
                                       (memory or SQLite)
                                                        |
                                        RabbitMQ broadcast backplane
                                                        |
                                    Ingest-local WebSocket subscribers
```

The client sends swipes and initiates WebSocket connections through the ALB.
Each HTTP request goes to one ingest host. A WebSocket stays with the ingest host
that accepted its upgrade. Changed standings travel from the consumer through
the backplane to each ingest host, then back through the ALB to its subscribers.

## Modules

| Module | Responsibility |
|---|---|
| `leaderboard-common` | Immutable versioned leaderboard payload and snapshot serialization |
| `server-v2` | HTTP ingestion, pooled RabbitMQ publishing, real WebSocket endpoint and per-host backplane |
| `consumer` | Ordered aggregation, event ID deduplication, conditional notifications, outbox retries and authoritative snapshots |
| `client-part2` | HTTP load generation, ALB WebSocket subscriptions and metrics |
| `deployment` | Deployment, rollback and ALB configuration procedures |
| `monitoring` | Broker and host sampling, queue guard and verification workflow |

## Build

Use Java 11 and Maven from this repository root:

```sh
mvn clean package
java -ea -cp client-part2/target/classes:client-part2/target/test-classes websocket.LeaderboardUpdateTrackerTest
```

Build artifacts:

- `server-v2/target/ski-servlet-1.0-SNAPSHOT.war` (deploy as `server-v2.war`).
- `consumer/target/consumer-1.0-SNAPSHOT.jar` (shaded executable consumer).

Subset builds must include the shared module, for example
`mvn -pl server-v2 -am package`. Environments where Mockito cannot attach its test
agent can provide the cached Byte Buddy agent with Maven's `argLine` option.

## Endpoints and client

- HTTP: `/server-v2/skiers/{resort}/seasons/{season}/days/{day}/skiers/{skier}`.
- WebSocket: `/server-v2/leaderboard` with `subscribe` / `unsubscribe` JSON.
- Health: `/server-v2/health`; readiness includes the real leaderboard backplane.

The client's defaults use the existing ALB for both HTTP and WebSocket traffic.
Override them with `HTTP_BASE_URL` and `WS_BASE_URL` if needed.

```sh
java -cp client-part2/target/classes LoadClient smoke 30
```

Only the small smoke workload is shown here. Large benchmarks require monitoring
and deliberate scheduling.

## Correctness and deployment

The Consumer selects its storage at startup. Set `LEADERBOARD_STORAGE=memory`
for A2's in-memory aggregation or `LEADERBOARD_STORAGE=sqlite` for persistent
storage. SQLite is the default for existing deployments.

```sh
# With the RabbitMQ connection variables set and the resort queues declared
LEADERBOARD_STORAGE=memory java -jar consumer/target/consumer-1.0-SNAPSHOT.jar
LEADERBOARD_STORAGE=sqlite LEADERBOARD_DB_PATH=data/leaderboard.db \
  java -jar consumer/target/consumer-1.0-SNAPSHOT.jar
```

Run a single processing Consumer for these shared queues. Change modes between
runs after the queues and leaderboard publications drain. The memory mode
starts with empty standings and keeps totals, event IDs and pending changes for
the lifetime of that process. Restart the ingest applications or reconnect
subscribers after changing modes or restarting a memory Consumer to establish
the new version baseline. SQLite history lives in its database and is available
when that file is opened again. Swipes processed in memory are acknowledged
after in-memory aggregation and are absent from that file. Use the
`server-v2/leaderboard` WebSocket endpoint with either mode.

SQLite records event IDs, skier totals and pending leaderboard changes in one
transaction. Memory applies the same changes under a lock before ACK. Only
changes to the ordered top 10 generate notifications. A leader's
score increase counts as a change even if rank is unchanged. A below-cutoff
increase changes the total without a broadcast. Replayed event IDs change neither.

Every ingest host receives its own copy of changed standings. A new subscription
requests a current snapshot from the processing host through RabbitMQ, using a
correlation ID and an expiring request. Snapshot/update races and duplicate
versions are handled locally; no ingest host copies SQLite or fabricates rankings.

The four ingest hosts run the real endpoint. The old processing-host WebSocket
service is retired; the consumer and snapshot responder remain active. Existing
RabbitMQ connection pooling, remote credentials and service configuration are
preserved. Credentials and private keys must stay outside the repository.

See [the current deployment guide](deployment/INGEST_WEBSOCKETS.md),
[architecture](architecture/ARCHITECTURE.md), and
[the AWS in-memory benchmark results](performance/aws-memory-20261113/README.md).

## Current validation

- [Assignment 2 submission PDF](docs/CS6650_Assignment2_Final_Report_Lin_Cai.pdf)
- [AWS in-memory test matrix and evidence](performance/aws-memory-20261113/README.md)
- [AI usage reflection](AI_USAGE.md)

The AWS in-memory suite completed 500K swipes with one, two and four ingest
hosts, and 1M with four. The 1M run sustained 2,883.09 requests/s in the
unpaced main phase. HTTP 201, RabbitMQ ACK and unique aggregation counts each
matched the expected workload. Earlier rate-controlled SQLite results are in
[monitoring/RESULTS.md](monitoring/RESULTS.md). The
[monitoring guide](monitoring/README.md) describes broker and host sampling and
the guarded load client.

The ALB script documents the intended health checks, idle timeout and cookie
settings. The AWS in-memory suite verified routing, WebSocket affinity and final
leaderboard agreement. An [earlier SQLite functional probe](performance/aws-memory-20261113/historical-alb-failover.json)
observed failover and a 75-second idle connection. Live export of the exact
ALB attributes was outside the evidence. Consumer worker count is configured at
startup, and automatic load-driven worker scaling remains unimplemented.

## Consumer and client tuning

Consumer batching, prefetch, worker count, ACK batching and SQLite checkpoint/cache
settings are configurable; the publisher channel pool is configurable per ingest.
See [the current tuning environment](deployment/consumer-tuning-current.env) and [architecture](architecture/ARCHITECTURE.md) for the measured configuration and failure behavior.
`LoadClient full 120 128 32` runs the fixed 500K workload with 128 main threads and
32 warmup threads. `TARGET_RPS` limits the main phase; optional
`WARMUP_TARGET_RPS` overrides only the warmup and otherwise inherits `TARGET_RPS`.
`LOAD_TOTAL_EVENTS` changes the total while retaining the selected mode's warmup
count. The request CSV is saved after HTTP completion, before WebSocket drain.
Use the monitored runner for larger workloads.

Set `LOAD_DAY_ID` to use a fresh leaderboard partition for a benchmark. The
load client uses the same day for its swipe requests and WebSocket subscriptions.
The default day is 1.
