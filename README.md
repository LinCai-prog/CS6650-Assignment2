# CS6650 Assignment 2 — SkiTrak Distributed System

## Overview

This project implements a distributed ski tracking and real-time leaderboard system for CS6650: Building Scalable Distributed Systems at Northeastern University.

The system replaces the simulated backend from Assignment 1 with a real asynchronous processing pipeline using RabbitMQ, Java consumers, SQLite, and WebSocket notifications. Multiple HTTP ingest servers run behind an AWS Application Load Balancer.

### Key Features

- Horizontally scalable HTTP ingestion using AWS Application Load Balancer
- RabbitMQ-based asynchronous event publishing and consumption
- Connection and channel management for RabbitMQ publishers
- Per-resort event processing and leaderboard aggregation
- SQLite persistence with event-ID-based deduplication
- Real-time leaderboard notifications through WebSocket
- Multi-threaded load testing and performance measurement
- Error handling, reconnection, and automated tests

## System Architecture

```text
                       Load Testing Client
                       HTTP + WebSocket
                                |
                                v
                    AWS Application Load Balancer
                                |
             +------------------+------------------+
             |                  |                  |
             v                  v                  v
        Ingest #1          Ingest #2          Ingest #3/#4
             |                  |                  |
             +------------------+------------------+
                                |
                                v
                         RabbitMQ Broker
                         skitrak.exchange
                                |
                     Resort-specific Queues
                         resort.1–resort.10
                                |
                                v
                         Consumer Service
                                |
                     Event Validation
                     Deduplication
                     Aggregation
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
                         WebSocket Server
                                |
                                v
                       Subscribed Clients
```

The HTTP ingestion layer and asynchronous processing layer can operate independently. The RabbitMQ broker buffers events between producers and consumers, while the WebSocket service delivers leaderboard updates to subscribers.

## Repository Structure

```text
CS6650-Assignment2/
├── server-v2/
│   ├── pom.xml
│   ├── src/main/java/
│   └── web/
├── consumer/
│   ├── pom.xml
│   ├── src/main/java/
│   └── src/test/java/
├── client-part2/
│   ├── pom.xml
│   └── src/main/java/
└── .gitignore
```

Additional deployment, monitoring, architecture, and AI usage documentation will be included with the final submission.

## Components

### 1. Ingest Server (`server-v2`)

The ingest server is a Java Servlet application deployed on Apache Tomcat 9.

Its primary responsibilities include:

- Receiving and validating skier lift-ride requests
- Looking up lift vertical-drop information
- Constructing swipe-event messages
- Publishing events to RabbitMQ
- Managing RabbitMQ connections and pooled channels
- Providing an HTTP health-check endpoint

Important classes include `SkierServlet`, `SwipeEventMessage`, `RabbitMQManager`, `ChannelPool`, `SwipePublisher`, and `RabbitMQTopology`.

### 2. Consumer and Leaderboard Service (`consumer`)

The processing service consumes resort-specific events from RabbitMQ, validates events, updates persistent state, and publishes leaderboard notifications.

Its primary responsibilities include:

- Handling asynchronous RabbitMQ messages
- Preserving the required per-resort processing order
- Preventing duplicate aggregation through event IDs
- Updating skier totals and leaderboard state
- Persisting results to SQLite
- Publishing and broadcasting leaderboard updates
- Managing consumer concurrency and shutdown

Important classes include `RabbitMQConsumerManager`, `ResortConsumer`, `ResortSerialExecutor`, `LeaderboardAggregator`, `SqliteLeaderboardStore`, and `LeaderboardWebSocketServer`.

### 3. Load Testing Client (`client-part2`)

The load testing client generates simulated lift-ride requests and measures the system's performance.

It supports multiple workloads and collects:

- HTTP request throughput
- Successful and failed requests
- Request latency (mean, median, P95, P99)
- Per-resort throughput
- WebSocket connection statistics
- Leaderboard delivery and push-latency statistics
- CSV performance output

## Technologies

| Layer | Technology |
|---|---|
| Programming language | Java 11 |
| HTTP application server | Apache Tomcat 9 |
| Messaging | RabbitMQ |
| Database | SQLite |
| Real-time communication | WebSocket |
| Build system | Apache Maven |
| Cloud infrastructure | AWS EC2 |
| Load balancing | AWS Application Load Balancer |
| Performance testing | Custom multithreaded Java client |

## Building the Project

Prerequisites:

- Java 11
- Apache Maven
- RabbitMQ for integration and deployment
- Apache Tomcat 9 for the servlet application

Build each module independently:

```bash
cd server-v2
mvn clean package

cd ../consumer
mvn clean package

cd ../client-part2
mvn clean package
```

Run the available consumer tests:

```bash
cd consumer
mvn test
```

These commands describe the intended build workflow. A fresh-clone build and test run must still be performed as part of the final submission verification.

## Runtime Configuration

RabbitMQ connection settings are provided through environment variables rather than hardcoded credentials.

The ingest server and consumer use `RABBITMQ_PASSWORD` from the process environment. The ingest deployment also uses RabbitMQ host, port, and username configuration.

Do not commit production credentials, private SSH keys, or environment files containing secrets.

Deployment-specific service configuration, database paths, and connection instructions will be documented separately.

## AWS Deployment

The tested deployment uses:

- Four EC2 ingest instances running Tomcat 9
- One AWS Application Load Balancer for HTTP ingestion
- One RabbitMQ EC2 instance
- One processing EC2 instance hosting the consumer, SQLite, and WebSocket services

The processing service and WebSocket service run as separate systemd-managed applications.

The application exposes:

```text
GET /server-v2/health
POST /server-v2/skiers/...
```

The RabbitMQ broker uses resort-specific queues (`resort.1` through `resort.10`) and a separate leaderboard notification queue.

## Performance Testing

The performance evaluation includes:

| Configuration | Requests | Status |
|---|---:|---|
| Single ingest server | 500,000 | Completed and verified |
| Two ingest servers | 500,000 | Completed and verified |
| Four ingest servers | 500,000 | Pending final result |
| Four ingest servers — stress test | 1,000,000 | Pending |

### Verified 500K Benchmark Results

| Metric | 1 Ingest | 2 Ingest |
|---|---:|---:|
| Successful HTTP requests | 500,000 | 500,000 |
| Failed HTTP requests | 0 | 0 |
| Overall throughput | 676.45 req/s | 781.87 req/s |
| Main-phase throughput | 869.08 req/s | 825.23 req/s |
| Mean HTTP latency | 119.17 ms | 116.21 ms |
| P95 HTTP latency | 301.22 ms | 214.52 ms |
| P99 HTTP latency | 505.84 ms | 521.46 ms |

Both completed configurations successfully persisted 500,000 new events to SQLite and drained their RabbitMQ business queues.

The two-instance configuration achieved higher overall throughput and lower HTTP P95 latency. However, main-phase throughput did not scale linearly with the number of ingest servers.

WebSocket push latency also increased substantially during the larger tests, indicating that the notification path requires separate performance analysis.

### Running the Load Client

In IntelliJ IDEA, run `LoadClient` with one of the supported arguments:

```text
smoke 30
medium 30
full 120
```

The `full 120` configuration performs 500,000 HTTP requests, with 32,000 warmup requests and 468,000 main-phase requests, followed by a 120-second WebSocket observation period.

The HTTP endpoint and WebSocket connection settings must match the deployment environment.

## Reliability and Data Integrity

The system uses event IDs and persistent deduplication to prevent repeated processing from increasing skier totals more than once.

The completed 500K tests were validated using:

1. HTTP response counts and status distributions
2. CSV metrics output
3. SQLite `processed_events` record counts
4. RabbitMQ queue drain verification
5. WebSocket connection and delivery statistics

Additional reliability tests, including shutdown-race verification, must be reported according to their actual test outcomes.

## Security Considerations

- RabbitMQ credentials are supplied through environment variables.
- SSH private keys and production secrets are excluded from version control.
- SQLite runtime files and large generated performance CSV files are excluded from the source repository.
- Public deployment IP addresses should not be treated as permanent service identifiers.

## Assignment Documentation

The final submission will include:

- Architecture diagram and message-processing sequence diagram
- RabbitMQ queue topology and connection-locality design
- Load-balancer configuration and performance evidence
- Performance results and scalability analysis
- Deployment and monitoring instructions
- AI usage reflection in `AI_USAGE.md`

## Repository

https://github.com/LinCai-prog/CS6650-Assignment2
