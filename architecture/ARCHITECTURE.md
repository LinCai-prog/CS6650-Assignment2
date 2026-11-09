# CS6650 Assignment 2 — System Architecture

**Project:** Message Distribution and the Real Leaderboard  
**Course:** CS6650 — Building Scalable Distributed Systems

## 1. System Overview

SkiTrak is a distributed, event-driven ski tracking system deployed on AWS EC2. It separates HTTP request ingestion from asynchronous processing and real-time leaderboard delivery.

The system uses four Tomcat ingest servers behind an Application Load Balancer (ALB), RabbitMQ for message distribution, a Java consumer for aggregation, SQLite for persistence, and a separate WebSocket application for notifications.

### Architecture Diagram

```text
                 Load Testing Client
                 HTTP + WS Subscribers
                          |
                  HTTP requests
                          |
                          v
                 AWS Application
                  Load Balancer
                          |
              +-----------+-----------+
              |           |           |
           Ingest 1    Ingest 2    Ingest 3/4
              |           |           |
              +-----------+-----------+
                          |
                          v
                    RabbitMQ Broker
                   skitrak.exchange
                          |
                 resort.1 ... resort.10
                          |
                          v
                Consumer Application
                          |
                Validation / Deduplication
                Per-Resort Aggregation
                          |
                          v
                    SQLite Database
                          |
                   Leaderboard Updates
                          |
                          v
                    RabbitMQ Backplane
                          |
                          v
                  WebSocket Application
                          |
                          v
                 Connected Subscribers
```

The HTTP ALB distributes ingest traffic. In the tested deployment, WebSocket connections are handled by a separate application on the processing EC2 instance, rather than by the four Tomcat ingest servers.

## 2. Swipe-to-Leaderboard Sequence

1. **Request ingestion:** The client sends a lift-ride request through the ALB to an available Tomcat instance.

2. **Validation and enrichment:** The ingest servlet validates the request, determines the lift's vertical drop, and creates a swipe event containing an `eventId`.

3. **Message publishing:** The producer borrows a RabbitMQ channel from its thread-safe pool and publishes the event to `skitrak.exchange` using a resort-specific routing key.

4. **Message consumption:** The corresponding resort queue delivers the event to the consumer.

5. **Aggregation:** The consumer validates the event and uses its event ID to prevent duplicate application. It updates skier totals and persistent leaderboard state in SQLite.

6. **Notification:** The consumer publishes leaderboard updates through RabbitMQ. The separate WebSocket application consumes these updates and sends them to relevant locally connected subscribers.

7. **Acknowledgment:** The consumer acknowledges processed messages according to its successful-processing and error-handling logic.

HTTP acceptance, database persistence, and WebSocket notification are distinct asynchronous stages. A successful HTTP response does not necessarily mean the leaderboard has already been updated.

## 3. RabbitMQ Queue Topology

The ingest layer publishes to the topic exchange `skitrak.exchange`.

Events use routing keys of the form `resort.{resortID}` and are distributed across ten resort-specific queues (`resort.1` through `resort.10`).

The deployment also includes:

- `skitrak.websocket.updates` for leaderboard notification delivery.
- `skitrak.deadletter.queue` for dead-letter handling.

Separating resort queues allows independent processing of different resorts and provides a basis for preserving per-resort order.

RabbitMQ channels are pooled because channels must not be concurrently shared across publishing threads. Reusing channels also avoids the cost of creating a new channel for every request.

## 4. Consumer Concurrency and Data Consistency

The consumer uses a configurable concurrent processing architecture with resort-specific serialization controls.

`ResortSerialExecutor` coordinates processing order within a resort, while independent resorts can be processed concurrently. `TaskAdmissionGate` helps coordinate acceptance of work during shutdown.

Persistent event-ID deduplication protects aggregation against duplicate RabbitMQ deliveries. SQLite stores processed-event information and leaderboard state.

At-least-once messaging requires the consumer to tolerate redelivery. Therefore, correct aggregation depends on idempotent database operations rather than assuming RabbitMQ delivers every event exactly once.

## 5. WebSocket Connection Locality

A WebSocket connection belongs to the server process that accepted it. Consequently, a consumer cannot send notifications directly to subscribers that may be connected to another process or host.

The project addresses this separation using RabbitMQ as a notification backplane between the processing logic and the WebSocket application.

For the current deployment, one WebSocket application instance receives leaderboard updates and broadcasts to its local subscribers.

**Scalability trade-off:** This decouples aggregation from WebSocket delivery, but a single shared notification queue is not automatically a broadcast mechanism for multiple WebSocket servers. Scaling to multiple WebSocket hosts requires each host to receive relevant updates, for example through a fanout exchange with a separate queue for each host.

This distinction is important because RabbitMQ's competing-consumer behavior would otherwise deliver each notification to only one consumer.

## 6. Load Balancing and Failure Handling

The AWS ALB distributes HTTP traffic across four Tomcat ingest instances. The ALB health checks help identify unhealthy targets and prevent new traffic from being directed to them.

RabbitMQ absorbs temporary differences between ingestion and processing rates, but sustained queue growth indicates a downstream bottleneck.

The consumer applies validation, acknowledgment, and persistent deduplication to support recovery from message redelivery. The processing EC2 instance runs the consumer and WebSocket application as separate systemd services configured for automatic startup.

End-to-end reliability must be validated using HTTP response counts, RabbitMQ backlog, SQLite processed-event counts, and WebSocket delivery metrics.

## 7. Design Trade-offs

The architecture provides horizontal scalability at the HTTP ingestion layer and decouples request handling from leaderboard computation.

However, increasing ingest instances does not necessarily increase total system throughput proportionally. RabbitMQ processing capacity, SQLite write contention, consumer concurrency, and WebSocket delivery may become bottlenecks.

The architecture therefore prioritizes message-processing correctness and measurable end-to-end behavior over HTTP throughput alone.