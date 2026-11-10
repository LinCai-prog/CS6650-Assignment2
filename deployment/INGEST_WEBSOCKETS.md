# Real WebSockets on every ingest host

## Deployed roles

- Four Tomcat ingest hosts accept both HTTP swipes and WebSocket connections.
- The processing host owns SQLite and runs the consumer, outbox publisher and
  read-only snapshot responder. It does not serve the client WebSocket endpoint.
- RabbitMQ transports swipe events, changed leaderboards and snapshot requests.

Client endpoint:
`ws://cs6650-ingest-alb-972956563.us-west-2.elb.amazonaws.com/server-v2/leaderboard`

The WebSocket JSON protocol remains `subscribe` / `unsubscribe` with string
`resortId` and `dayId`. Responses include the accepting host's `serverId` so tests
can verify routing, affinity and delivery to all four hosts. The old synthetic
`/leaderboard/{resortID}` endpoint is replaced by `/leaderboard`.

## Connection locality and initial state

Each ingest application creates its own exclusive queue bound to the topic
exchange `skitrak.leaderboard.updates` with `leaderboard.*.*`. Each host therefore
receives a copy of every changed leaderboard and sends it only to its local
matching subscriptions. No ingest host reads or copies the processing database.

Initial subscriptions request an authoritative `(resort, day)` snapshot from
`skitrak.leaderboard.snapshots`. The processing consumer reads standings and
version from one SQLite read transaction and replies to the ingest application's
exclusive reply queue using a correlation ID. Requests expire after five seconds.
Replies with no pending correlation ID are ignored.

A subscription is registered before requesting its snapshot. Changes arriving
while the snapshot is loading are buffered as the latest state; after the
snapshot, only a newer version can be sent. This initialization synchronizes
current state, rather than providing every historical intermediate change.

Each local connection tracks the last accepted version per subscription. Retries
or old replies cannot regress its standings. A broker reconnection recreates both
queues and refreshes existing subscriptions from the authoritative snapshot
service. Snapshot failures are retried while the connection remains subscribed.

Asynchronous WebSocket writes are serialized per connection. The queue is bounded
at 256 messages; slow or failed subscribers are disconnected and can reconnect
for current state. Protocol pings every 25 seconds keep idle connections active
without inventing leaderboard updates.

## Build and test

Build from the repository root so the shared protocol module is included:

```sh
mvn package
java -ea -cp client-part2/target/classes:client-part2/target/test-classes websocket.LeaderboardUpdateTrackerTest
```

The shared `leaderboard-common` module is packaged in both the consumer JAR and
server WAR. To build a subset, use `mvn -pl server-v2 -am package` or
`mvn -pl consumer -am package` from the root. On environments where Mockito cannot
attach its agent, supply the cached Byte Buddy agent through Maven's `argLine`.

## Rollout and rollback

1. Save the current processing JAR, each ingest WAR and the ALB attributes.
2. Deploy the processing JAR first. Preserve SQLite and verify the snapshot
   responder has a consumer before changing the ingest hosts.
3. Deploy the identical WAR to one ingest host at a time under the existing
   `server-v2.war` context. Wait for its `/server-v2/health` response to report
   `leaderboardReady: true` before proceeding to the next host.
4. Verify snapshots and live changes on all four hosts through the ALB.
5. Stop and disable the old `cs6650-websocket` service only after the ingest
   endpoints have passed verification. The consumer must remain running.
6. Rebuild the load client; its default WebSocket endpoint now uses the ALB.

On failure, restore that host's saved WAR and restart Tomcat. Preserve SQLite.
The processing JAR remains compatible with the preceding dedicated WebSocket
service, so it can be restarted for a temporary rollback if necessary.

`/server-v2/health` returns 503 until the backplane and snapshot responder are
available. It returns a unique `serverId` when ready. Existing RabbitMQ publisher
connection/channel pooling remains separate from the long-lived backplane
subscriber connection.

## ALB requirements

ALB natively upgrades HTTP/1.1 connections to WebSockets; no separate WebSocket
listener protocol is needed. The existing HTTP target group forwards the upgrade
to Tomcat on port 8080.

Use duration-based target-group stickiness (300 seconds), an ALB idle timeout of
120 seconds, and health checks at `/server-v2/health` with interval 30 seconds,
timeout 5 seconds, healthy threshold 2 and unhealthy threshold 3. The path has the
WAR context prefix; the servlet itself maps to `/health`.

Set `EXPECTED_TARGET_IDS` to the four space-separated ingest instance IDs and
run `deployment/configure-alb.sh` from an authenticated AWS CLI session or AWS
CloudShell. It checks the target membership, saves the old configuration and
applies those attributes. It never creates access keys or changes IAM/security
permissions. Each load-client WebSocket subscriber has its own cookie jar, so
reconnect affinity does not pin every subscriber to the same host.

A persistent upgraded connection remains on its accepting target. Cookie affinity
on reconnect and failover must be tested separately. A successful smoke test alone
does not prove all ALB attributes, full failure recovery or benchmark performance.

## Measured results and remaining limits

See [benchmark results](../monitoring/RESULTS.md) and [monitoring scripts](../monitoring/README.md).
SQLite aggregation and snapshot reads remain centralized on the processing host.
There is one ordered consumer per resort and one writer/outbox dispatcher per
aggregation database. The worker pool is configurable; automatic scaling and
controlled failover are outside the measured benchmark coverage.
