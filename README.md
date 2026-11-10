# CS6650 Assignment 2: real distributed leaderboards

## Data flow

```text
Client -> ALB -> [four ingest servers] -> RabbitMQ -> Consumer
                                                        |
                                       SQLite aggregation + outbox
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
| `consumer` | Ordered aggregation, SQLite deduplication, conditional notifications, outbox retries and authoritative snapshots |
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

SQLite records event IDs, skier totals and pending leaderboard changes in one
transaction. Only changes to the ordered top 10 generate notifications. A leader's
score increase counts as a change even if rank is unchanged. A below-cutoff
increase is persisted without a broadcast. Replayed event IDs change neither.

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
[the benchmark results](monitoring/RESULTS.md).
The earlier performance report contains historical measurements from the former
deployment and must not be treated as a benchmark of this revised code.

## Current validation

The Java 11 Maven build and 83 automated tests passed before submission. Four
client self-checks cover allocation, shared workload, pacing and payload tracking.
The measured 500K and 1M results are in [monitoring/RESULTS.md](monitoring/RESULTS.md).
Use [the monitoring scripts](monitoring/README.md) for broker and host sampling
and to guard a load client. Raw evidence remains separate from source control.

The ALB script documents the intended health checks, idle timeout and cookie
settings. Routing and live WebSockets were observed; an authenticated export of
all ALB settings and controlled failover testing were not part of the final runs.
Consumer worker count is configurable, but there is no automatic load-driven
worker scaling. The measured rates do not establish unlimited-input capacity.

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
