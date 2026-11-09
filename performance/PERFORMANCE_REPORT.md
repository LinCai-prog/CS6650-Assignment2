# CS6650 Assignment 2 — Performance Evaluation Report

**Course:** CS6650 — Building Scalable Distributed Systems  
**Project:** Message Distribution and the Real Leaderboard

## 1. Objective

The objective of this evaluation is to measure the performance and scalability of a distributed ski-tracking system that uses multiple HTTP ingest servers, RabbitMQ, asynchronous consumers, SQLite persistence, and real-time WebSocket notifications.

The evaluation focuses on:

- HTTP request throughput and latency
- Scalability from one to multiple ingest servers
- Message processing and queue-drain behavior
- Database persistence and event integrity
- WebSocket notification delivery
- Identification of potential bottlenecks

## 2. Test Environment

The system was deployed on AWS in the `us-west-2` region.

| Component | Configuration |
|---|---|
| HTTP ingestion | Java Servlet application on Tomcat 9 |
| Load balancing | AWS Application Load Balancer |
| Ingest instances | 1, 2, or 4 EC2 instances |
| Message broker | RabbitMQ on a separate EC2 instance |
| Consumers | Java processing application |
| Database | SQLite |
| Notifications | RabbitMQ-backed WebSocket application |
| Load generator | Custom multithreaded Java client |

The ingest layer publishes lift-ride events to RabbitMQ. Consumers validate, deduplicate, and aggregate messages before updating SQLite and generating leaderboard notifications.

## 3. Benchmark Methodology

For the standard 500,000-request workload:

- Warmup phase: 32,000 HTTP requests
- Main phase: 468,000 HTTP requests
- Total: 500,000 HTTP requests
- WebSocket observation period: 120 seconds after HTTP execution

The HTTP client records successful and failed requests, throughput, and response latency.

The asynchronous pipeline is separately evaluated through SQLite processed-event counts, RabbitMQ queue state, and WebSocket delivery metrics.

The overall throughput is calculated from the HTTP execution phases. The WebSocket observation period is not included in this throughput denominator.

### Performance Metrics

**Throughput**

\[
\text{Throughput} = \frac{\text{Completed HTTP Requests}}{\text{HTTP Execution Time}}
\]

**HTTP latency**

Mean, median, P95, and P99 request latencies describe the HTTP response distribution.

**Database consistency**

The increase in unique processed-event records should match the number of unique events successfully processed during an isolated test.

**Message backlog**

RabbitMQ ready and unacknowledged counts are examined during and after testing.

## 4. Benchmark Results

### 4.1 HTTP Performance Comparison

| Metric | 1 Ingest | 2 Ingest | 4 Ingest |
|---|---:|---:|---:|
| Total requests | 500,000 | 500,000 | TBD |
| Successful requests | 500,000 | 500,000 | TBD |
| Failed requests | 0 | 0 | TBD |
| Warmup runtime | 200.65 s | TBD | TBD |
| Main-phase runtime | 538.50 s | TBD | TBD |
| Total HTTP runtime | 739.15 s | TBD | TBD |
| Warmup throughput | 159.49 req/s | 442.12 req/s | TBD |
| Main throughput | 869.08 req/s | 825.23 req/s | TBD |
| Overall throughput | 676.45 req/s | 781.87 req/s | TBD |
| Mean HTTP latency | 119.17 ms | 116.21 ms | TBD |
| HTTP P95 | 301.22 ms | 214.52 ms | TBD |
| HTTP P99 | 505.84 ms | 521.46 ms | TBD |

The single- and double-ingest configurations successfully completed all 500,000 HTTP requests without recorded HTTP failures.

### 4.2 Throughput Analysis

The single-ingest configuration achieved an overall throughput of 676.45 requests per second.

The two-ingest configuration achieved 781.87 requests per second, representing approximately 15.6% higher overall throughput.

However, the main-phase throughput decreased from 869.08 to 825.23 requests per second.

This difference suggests that the overall improvement should not be interpreted as evidence of linear scaling.

Possible contributing factors include workload phase behavior, client-side concurrency, asynchronous publishing overhead, shared infrastructure contention, or differences between individual benchmark runs.

Additional controlled tests and resource monitoring are necessary to isolate the cause.

### 4.3 HTTP Latency Analysis

The mean HTTP latency decreased from 119.17 ms to 116.21 ms when moving from one to two ingest servers.

P95 latency decreased from 301.22 ms to 214.52 ms, an improvement of approximately 28.8%.

However, P99 latency increased from 505.84 ms to 521.46 ms.

This indicates that the two-ingest configuration improved the 95th-percentile response time but did not eliminate the longest observed request delays.

A complete explanation requires additional information about per-instance load, network behavior, and shared downstream resources.

## 5. RabbitMQ Message Processing

The application uses resort-specific queues to distribute lift-ride events to consumers.

### Validation Results

| Validation | 1 Ingest | 2 Ingest | 4 Ingest |
|---|---|---|---|
| Resort queues drained after test | Yes | Yes | TBD |
| 500K new processed events | Yes | Yes | TBD |
| Peak queue depth | Not recorded | Not recorded | TBD |
| Average publish rate | Not recorded | Not recorded | TBD |
| Average consumer rate | Not recorded | Not recorded | TBD |
| Queue stability under peak load | Not established | Not established | TBD |

The completed benchmarks showed that the resort queues eventually drained.

However, a final queue depth of zero does not establish that queue backlog remained low or stable during the test.

Time-series measurements are required to evaluate whether consumers can continuously keep pace with producers.

## 6. SQLite Persistence and Idempotency

SQLite provides persistent aggregation and event-ID-based deduplication.

### Single-Ingest Test

Database processed-event count:

- Before: 513,027
- After: 1,013,027
- Difference: 500,000

### Double-Ingest Test

Database processed-event count:

- Before: 13,027
- After: 513,027
- Difference: 500,000

Both completed tests produced database count increases matching the expected workload size.

These results provide stronger end-to-end evidence than HTTP response counts alone.

## 7. WebSocket Performance

WebSocket notifications were measured independently from HTTP acceptance.

| Metric | 1 Ingest | 2 Ingest |
|---|---:|---:|
| Observed notifications after drain | 401,949 | 410,487 |
| P95 push latency | 77,917 ms | 19,324 ms |
| Observation period | 120 s | 120 s |

The two-ingest run showed lower WebSocket P95 push latency than the single-ingest run.

However, the high push latency observed during the single-ingest run indicates significant end-to-end delay in the notification pipeline.

The notification counts should not be directly interpreted as unique processed events without verifying subscription fanout and message-counting semantics.

The persistence and WebSocket metrics therefore represent different aspects of the system.

## 8. Four-Ingest Smoke Test

A separate 1,000-request smoke test was completed against the four-ingest deployment.

Observed results:

- HTTP requests: 1,000
- Successful responses: 1,000
- Failed responses: 0
- WebSocket notifications: 1,000
- WebSocket P95 push latency: 73 ms
- SQLite processed-event increase: 1,000
- Final RabbitMQ resort queue backlog: zero

This confirms successful functional operation for the smoke-test workload.

However, the smoke-test result cannot substitute for the required 500K benchmark.

## 9. Four-Ingest 500K Benchmark

**Status: Pending complete benchmark evidence**

| Metric | Result |
|---|---|
| Successful requests | TBD |
| Failed requests | TBD |
| Overall throughput | TBD |
| Mean latency | TBD |
| HTTP P95 / P99 | TBD |
| RabbitMQ peak depth | TBD |
| SQLite processed-event increase | TBD |
| WebSocket push latency | TBD |

The final outcome must be validated independently, especially if an earlier load test was interrupted or restarted.

## 10. Four-Ingest 1M Stress Test

**Status: Not yet verified**

The stress test is intended to evaluate whether the system can maintain acceptable performance under a larger workload.

Metrics to collect:

- Successful and failed requests
- Overall throughput
- Latency distribution
- RabbitMQ peak queue depth
- Message consumption and publish rates
- SQLite persistence correctness
- WebSocket push latency
- EC2 CPU, memory, and network utilization

No stress-test results are claimed in this report until supported by actual execution evidence.

## 11. Bottleneck Analysis

The benchmark results suggest several areas for further investigation.

**HTTP ingress capacity:** Increasing the number of ingest servers improved overall throughput but did not result in proportional scaling.

**RabbitMQ:** Without time-series queue measurements, it is not yet possible to determine whether sustained backlog occurred during peak load.

**SQLite:** Database persistence introduces a shared resource that may constrain aggregation throughput as producer capacity increases.

**WebSocket:** The observed push-latency results demonstrate that notification timeliness must be measured separately from HTTP and database correctness.

These are potential explanations, not confirmed root causes.

## 12. Conclusion

The completed 500K tests demonstrate successful HTTP ingestion, asynchronous event processing, and persistent event aggregation for one and two ingest servers.

Moving from one to two ingest servers improved overall HTTP throughput by approximately 15.6% and reduced HTTP P95 latency by approximately 28.8%.

Nevertheless, main-phase throughput did not improve, P99 latency increased slightly, and WebSocket delivery remained substantially slower than HTTP acceptance.

These observations demonstrate the importance of evaluating a distributed system end-to-end rather than relying exclusively on HTTP throughput.

The remaining four-ingest benchmarks, resource monitoring, RabbitMQ queue profiles, and 1M stress test are necessary to complete the required scalability evaluation.