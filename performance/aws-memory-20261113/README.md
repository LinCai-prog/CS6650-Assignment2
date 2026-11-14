# AWS in-memory performance evaluation

[Download the Assignment 2 submission PDF](../../docs/CS6650_Assignment2_Final_Report_Lin_Cai.pdf)

Four completed November 13, 2026 runs in AWS us-west-2 used the same Consumer in-memory mode, a 32,000-swipe warmup paced at 300/s, and 128 unpaced main-phase HTTP threads. The load client ran on the RabbitMQ EC2 host. Each run used a fresh day partition and subscribed to all ten resorts. The single ingest used direct VPC traffic. The two- and four-ingest tests used the ALB.

| Workload | Entry path | HTTP window (PST) | Run evidence |
|---|---|---|---|
| 1 ingest / 500K | direct VPC | 15:58:15 to 16:03:11 | [Results](../aws-single-500k-memory-unpaced-20261113/README.md) · [Client](../aws-single-500k-memory-unpaced-20261113/client-output.txt) · [Queue](../aws-single-500k-memory-unpaced-20261113/depth-series.csv) · [Rates](../aws-single-500k-memory-unpaced-20261113/message-rate-series.csv) · [JSON](../aws-single-500k-memory-unpaced-20261113/summary.json) |
| 2 ingests / 500K | ALB | 16:08:48 to 16:13:21 | [Results](../aws-two-500k-memory-unpaced-20261113/README.md) · [Client](../aws-two-500k-memory-unpaced-20261113/client-output.txt) · [Queue](../aws-two-500k-memory-unpaced-20261113/depth-series.csv) · [Rates](../aws-two-500k-memory-unpaced-20261113/message-rate-series.csv) · [JSON](../aws-two-500k-memory-unpaced-20261113/summary.json) |
| 4 ingests / 500K | ALB | 16:18:58 to 16:23:32 | [Results](../aws-four-500k-memory-unpaced-20261113/README.md) · [Client](../aws-four-500k-memory-unpaced-20261113/client-output.txt) · [Queue](../aws-four-500k-memory-unpaced-20261113/depth-series.csv) · [Rates](../aws-four-500k-memory-unpaced-20261113/message-rate-series.csv) · [JSON](../aws-four-500k-memory-unpaced-20261113/summary.json) |
| 4 ingests / 1M | ALB | 16:28:05 to 16:35:28 | [Results](../aws-four-1m-memory-unpaced-20261113/README.md) · [Client](../aws-four-1m-memory-unpaced-20261113/client-output.txt) · [Queue](../aws-four-1m-memory-unpaced-20261113/depth-series.csv) · [Rates](../aws-four-1m-memory-unpaced-20261113/message-rate-series.csv) · [JSON](../aws-four-1m-memory-unpaced-20261113/summary.json) |

## Performance and queue behavior

| Metric | 1 / 500K | 2 / 500K | 4 / 500K | 4 / 1M |
|---|---:|---:|---:|---:|
| HTTP runtime incl. warmup (s) | 295.83 | 272.90 | 274.01 | 442.82 |
| HTTP overall throughput (req/s) | 1,690.15 | 1,832.15 | 1,824.74 | 2,258.27 |
| HTTP main throughput (req/s) | 2,478.33 | 2,824.75 | 2,805.71 | 2,883.09 |
| HTTP mean (ms) | 47.97 | 42.42 | 42.77 | 42.84 |
| HTTP P95 (ms) | 84.45 | 68.17 | 69.91 | 68.31 |
| HTTP P99 (ms) | 115.42 | 90.74 | 92.08 | 90.44 |
| Consumer mean (ms) | 33.88 | 19.14 | 16.28 | 17.25 |
| Consumer P99 (ms) | 102 | 70 | 60 | 74 |
| Consumer max (ms) | 687 | 380 | 339 | 204 |
| Swipes with Consumer lag ≥100 ms | 5,534 | 1,015 | 611 | 2,631 |
| Largest resort queue depth | 32 | 35 | 54 | 46 |
| Aggregate queue depth, mean | 20.02 | 27.41 | 26.05 | 37.69 |
| Aggregate queue depth, max | 169 | 255 | 430 | 371 |
| Mean publish (msg/s), sampled interval | 1,716.4 | 1,837.9 | 1,870.3 | 2,274.5 |
| Mean ACK (msg/s), sampled interval | 1,703.3 | 1,808.9 | 1,830.2 | 2,284.9 |
| Distinct WebSocket updates | 1,557 | 1,636 | 1,646 | 1,944 |

Depth is Ready + Unacked, sampled approximately once per second across the ten business queues. The 84,382 historical dead letters are excluded. Mean broker rates are time-weighted over captured counter intervals. Consumer lag is measured from event creation at ingest to aggregation start. The three ALB runs had P99 below 100 ms, with individual swipes at or above 100 ms in all four runs.

All four runs matched HTTP 201, RabbitMQ publish/deliver/ACK, Consumer unique-event counts and day-scoped ingest access logs. Every business queue drained. There were zero failed requests, logged retry triggers, duplicate aggregations, broker redeliveries or new dead letters. Distinct WebSocket updates matched the sum of final leaderboard versions in each run.

## ALB distribution

| Workload | Ingest 1 | Ingest 2 | Ingest 3 | Ingest 4 |
|---|---:|---:|---:|---:|
| 1 ingest / 500K | 500,000 | — | — | — |
| 2 ingests / 500K | 250,000 | 250,000 | — | — |
| 4 ingests / 500K | 125,000 | 124,999 | 125,001 | 125,000 |
| 4 ingests / 1M | 249,998 | 250,000 | 249,999 | 250,003 |

The ALB runs verified cookie affinity on fresh WebSocket upgrades and confirmed identical final standings across participating hosts. The two-ingest main phase was 14.0% faster than the direct single-ingest run. The four-ingest 500K main phase was 0.7% slower than two ingests, while four ingests sustained 2,883.09 req/s at 1M. Main-phase CPU was 98.9–99.4% busy on the EC2 instance shared by the broker and client. The host measurements suggest a shared CPU constraint. They do not isolate either process or separate the ALB hop from the effect of adding the second ingest.

## Supporting measurements

- [Combined metrics](comparison.csv) and [per-host CPU, memory and network](host-resources.csv)
- [Controlled swipe and replay evidence](../aws-single-500k-memory-20261113/websocket-swipe-proof.json) from the supplementary paced memory run
- [Local concurrency and prefetch sweep](local-tuning.csv) and [isolated storage A/B](local-storage-ab.csv). These use local workloads distinct from the AWS benchmarks.
- [Earlier ALB failover and idle probe](historical-alb-failover.json). The historical SQLite deployment recovered on another target after about 50.6 seconds and returned 12 health-probe HTTP 502s during detection.
- [Historical SQLite benchmark results](../../monitoring/RESULTS.md) and [AWS CloudWatch ALB metric definitions](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-cloudwatch-metrics.html)

The original SQLite Consumer was restored after benchmark collection. Its JAR hash and processed-event count matched their pretest values.
