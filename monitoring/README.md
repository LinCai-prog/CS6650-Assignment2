# Monitoring and Performance Validation

## Overview

This directory documents the monitoring methodology used to evaluate the CS6650 Assignment 2 distributed system.

The monitoring process covers:

- HTTP throughput and latency
- RabbitMQ queue depth and processing rates
- Consumer and WebSocket service health
- SQLite persistence and deduplication
- AWS load-balancer distribution
- EC2 CPU, memory, and network utilization

## 1. Client Performance Metrics

The custom Java load-testing client collects:

- Total requests
- Successful and failed requests
- Warmup and main-phase throughput
- Overall throughput
- HTTP mean, median, P95, and P99 latency
- WebSocket notification counts and push latency

The client can export performance data to CSV files. Raw CSV files are excluded from Git by default.

## 2. RabbitMQ Monitoring

Check current queue depth:

```bash
sudo rabbitmqctl list_queues name messages_ready messages_unacknowledged consumers
```

The RabbitMQ management interface can be used to inspect:

- Publish rate
- Deliver and acknowledgment rate
- Ready messages
- Unacknowledged messages
- Queue depth over time
- Consumer connections

Record screenshots during the test rather than only after all queues drain.

Queue depth at the end of a test does not establish peak backlog or queue stability.

## 3. EC2 Resource Monitoring

On each relevant EC2 instance, resource usage can be examined with:

```bash
top
```

Memory:

```bash
free -h
```

Disk:

```bash
df -h
```

Network and CPU metrics can also be inspected through Amazon CloudWatch.

For the final performance report, collect metrics for the same observation period as the load test.

## 4. ALB Monitoring

AWS Application Load Balancer metrics should be used to evaluate traffic distribution across ingest targets.

Relevant evidence includes:

- Healthy target count
- Request count by target or target group, where available
- Target response time
- HTTP error counts
- Connection behavior

The four-ingest smoke test confirmed application functionality, but a successful smoke test alone does not establish even traffic distribution at production-scale load.

## 5. SQLite Verification

Record the processed-event count immediately before and after each isolated load test.

The difference should match the number of unique events successfully processed.

Compare database counts with:

- Successful HTTP requests
- RabbitMQ pending-message counts
- Dead-letter queue state
- WebSocket delivery results

Ensure that overlapping or restarted runs do not contaminate the baseline.

## 6. Verified Benchmark Summary

| Metric | 1 Ingest / 500K | 2 Ingest / 500K |
|---|---:|---:|
| Successful requests | 500,000 | 500,000 |
| Failed requests | 0 | 0 |
| Overall throughput | 676.45 req/s | 781.87 req/s |
| Mean HTTP latency | 119.17 ms | 116.21 ms |
| HTTP P95 | 301.22 ms | 214.52 ms |
| HTTP P99 | 505.84 ms | 521.46 ms |
| New SQLite processed events | 500,000 | 500,000 |
| Final resort-queue backlog | 0 | 0 |

The two-ingest configuration improved overall throughput compared with the single-ingest configuration.

This comparison does not by itself establish linear scalability or identify the system's maximum sustainable throughput.

## 7. Pending Performance Evidence

- Four-ingest 500K complete benchmark
- Four-ingest 1M stress test
- RabbitMQ queue-depth time series
- Publish and consumption rate screenshots
- ALB request distribution evidence
- CPU, memory, and network utilization during load
- Verification of WebSocket real-time delivery under stress
- Final analysis of identified bottlenecks

## 8. Interpretation

A complete distributed-system performance analysis should distinguish HTTP acceptance, asynchronous event processing, database persistence, and notification delivery.

Successful HTTP responses do not automatically prove that all events have been durably aggregated or delivered to WebSocket subscribers.

All results should be supported by captured logs, measured counters, or monitoring screenshots.