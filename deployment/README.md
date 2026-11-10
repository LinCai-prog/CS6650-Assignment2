> Current deployment instructions: [INGEST_WEBSOCKETS.md](INGEST_WEBSOCKETS.md). The separate processing-host WebSocket service described below is the previous architecture and is retained only as historical deployment/rollback information.

# AWS Deployment Guide

## 1. Architecture Overview

CS6650 Assignment 2 is deployed on AWS EC2 using a distributed architecture consisting of:

- Four HTTP ingest servers behind an AWS Application Load Balancer (ALB)
- A RabbitMQ message broker running on a separate EC2 instance
- A processing EC2 instance hosting the consumer and WebSocket services
- SQLite for persistent leaderboard aggregation

### Event Flow

```text
Load Testing Client
        |
        v
AWS Application Load Balancer
        |
        v
Four Tomcat Ingest Instances
        |
        v
RabbitMQ Topic Exchange
        |
        v
Resort-Specific Queues
        |
        v
Consumer Application
        |
        v
SQLite Database
        |
        v
Leaderboard Update Messaging
        |
        v
WebSocket Application
        |
        v
Subscribed Clients
```

## 2. Deployment Components

| Component | Technology | Description |
|---|---|---|
| Load Balancer | AWS ALB | Distributes HTTP requests across ingest instances |
| Ingest Servers | EC2 + Tomcat 9 | Validate requests and publish swipe events |
| Message Broker | RabbitMQ | Buffers and routes resort-specific events |
| Consumer | Java 11 | Processes messages and updates leaderboard state |
| Database | SQLite | Persists processed events and skier aggregation |
| WebSocket Server | Java | Delivers real-time leaderboard updates |

## 3. RabbitMQ Configuration

RabbitMQ runs on a separate Amazon Linux 2023 EC2 instance.

Configuration:

- AMQP port: `5672`
- Management port: `15672`
- Topic exchange: `skitrak.exchange`
- Resort queues: `resort.1` through `resort.10`
- Leaderboard update queue: `skitrak.websocket.updates`
- Dead-letter queue: `skitrak.deadletter.queue`

The ingest application publishes messages using resort-specific routing keys. The consumer application processes messages asynchronously and updates persistent state.

RabbitMQ credentials are configured through environment variables and must not be committed to the repository.

## 4. HTTP Ingest Deployment

Each ingest instance runs Apache Tomcat 9 on Amazon Linux 2023.

The application is packaged as:

```text
server-v2.war
```

and deployed to:

```text
/opt/tomcat9/webapps/server-v2.war
```

Four ingest instances are registered with the ALB target group:

```text
cs6650-ingest-tg
```

The target group forwards traffic to Tomcat port `8080`.

During the recorded four-instance smoke test, all four targets were healthy.

The exact EC2 provisioning, Tomcat installation, and security group configuration should be reproduced and verified separately before describing this as a fully automated deployment.

## 5. Consumer and WebSocket Deployment

Both applications run on the processing EC2 instance.

The deployed JAR is:

```text
/opt/cs6650/processing/consumer-1.0-SNAPSHOT.jar
```

The consumer and WebSocket applications use separate Java entry points.

### Consumer Service

```text
Service: cs6650-consumer.service
Service file: /etc/systemd/system/cs6650-consumer.service
Working directory: /opt/cs6650/processing
User: ec2-user
```

### WebSocket Service

```text
Service: cs6650-websocket.service
Service file: /etc/systemd/system/cs6650-websocket.service
Main class: app.WebSocketApplication
```

The exact full `ExecStart` arguments and environment configuration must be checked against the deployed systemd unit files when reproducing this deployment.

### Verify Services

```bash
systemctl is-active cs6650-consumer cs6650-websocket
```

Expected output for healthy running services:

```text
active
active
```

Both services were verified as `active (running)` on November 8, 2026.

The services were also confirmed to be enabled through systemd.

### Inspect Service Logs

```bash
sudo journalctl -u cs6650-consumer -n 50 --no-pager
```

```bash
sudo journalctl -u cs6650-websocket -n 50 --no-pager
```

Review logs locally for sensitive information before sharing or committing them.

## 6. SQLite Storage

The processing application uses a local SQLite database:

```text
/opt/cs6650/processing/data/leaderboard.db
```

SQLite persistence supports event-ID-based deduplication and leaderboard aggregation.

The runtime database is excluded from Git because it contains generated application state.

For verification, database record counts can be compared before and after a controlled load test.

## 7. Health and Operational Checks

### Check Consumer and WebSocket Services

```bash
systemctl is-active cs6650-consumer cs6650-websocket
```

### Check RabbitMQ Status

On the RabbitMQ EC2 instance:

```bash
sudo rabbitmqctl status
```

### Inspect Queue Status

```bash
sudo rabbitmqctl list_queues name messages_ready messages_unacknowledged consumers
```

### Inspect Ingest Health

Use the deployed application health endpoint or the AWS ALB target group health status.

The ALB target group should report all intended ingest instances as healthy before performance testing.

## 8. Recovery Considerations

The deployment uses separate systemd services for the processing and WebSocket applications.

RabbitMQ message acknowledgment and persistent deduplication support recovery from certain processing failures.

However, the complete recovery behavior depends on the precise acknowledgment, retry, transaction, and shutdown implementation.

A fresh-environment deployment, broker restart test, and complete shutdown-race verification should be performed before claiming these scenarios have been fully validated.

## 9. Security

- Store RabbitMQ credentials outside source control.
- Never commit SSH private keys.
- Restrict AMQP and management ports through AWS security groups.
- Avoid exposing SQLite database files.
- Rotate credentials if they have been exposed.
- Do not commit service environment files containing secrets.

## 10. Deployment Verification Status

| Check | Status |
|---|---|
| Four ingest targets healthy | Verified during testing |
| RabbitMQ event integration | Verified during completed load tests |
| Consumer service running | Verified |
| WebSocket service running | Verified |
| SQLite event persistence | Verified during completed load tests |
| Fresh-environment deployment | Not yet verified |
| Complete shutdown-race validation | Not yet verified |

This document describes the recorded deployment architecture and operational checks. It is not yet a fully automated deployment procedure.