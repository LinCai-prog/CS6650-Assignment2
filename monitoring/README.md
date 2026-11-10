# Monitoring and benchmark verification

Use a fresh output directory for each workload. Confirm all ten resort queues are empty before starting, and keep the consumer running after the sender finishes or stops. The screenshots in the submission supplement the full time series.

## Queue samples and the stop guard

`sample_queues.py` uses broker-local RabbitMQ 4.4 diagnostics for one-second Ready and Unacked samples. Run it on the broker with permission to use `rabbitmqctl`. It reads the project queues in virtual host `/` and the dead-letter queue. Use a fresh stop-file name; touching that file on the broker stops the sampler without changing any queue.

For example, stream the sampler's output to the client machine from an existing authenticated SSH connection:

```sh
ssh "$BROKER_SSH" 'sudo python3 /path/to/monitoring/sample_queues.py --samples 1900 --stop-file /tmp/cs6650-run-001.stop' > queues.jsonl
```

In a separate terminal, launch a client through the guard. First build the Java client, and set `HTTP_BASE_URL` and `WS_BASE_URL` for the intended load balancer. Example for the small smoke workload:

```sh
TARGET_RPS=100 WARMUP_TARGET_RPS=50 python3 monitoring/guarded_client.py \
  --queues queues.jsonl --output-dir run-001-guard -- \
  java -cp client-part2/target/classes LoadClient smoke 10
```

The guard waits for a new, complete, fresh sample with all resort queues empty. It terminates the local client on the first observed individual resort depth above 1,000 or if queue telemetry goes stale. The business queues are not purged. The threshold applies to each resort, not the sum or the dead-letter queue. The client's shutdown hook preserves completed request rows in a partial CSV. Review any accepted events after drain, including requests still in flight at stop time. A sampled maximum does not prove the unsampled instantaneous maximum.

For the recorded staged 1M workload, full mode used `LOAD_TOTAL_EVENTS=1000000`, `WARMUP_TARGET_RPS=300`, and `TARGET_RPS=600`, with 64 warmup threads, 512 main threads, and a 10-second WebSocket drain. All 32,000 warmup events were included in the reported measurements. Keep large runs isolated, supervised and guarded.

## System, broker rates and ALB evidence

Run `sample_host.py` on the Linux processing, RabbitMQ and ingest hosts during the same observation window, for example:

```sh
python3 monitoring/sample_host.py --samples 380 --interval 5 --service cs6650-consumer > processing.jsonl
```

Use `rabbitmq-server` or `tomcat9` as the service on those hosts. The JSONL contains `/proc` CPU, memory, network and disk counters and optional service status; it does not export process environments or credentials. Derive CPU and I/O wait percentages and network deltas over the recorded interval. The benchmark also retained native EBS counters where available.

Capture the RabbitMQ console depth and publish/delivery/ACK rates over the run. Broker management rates are sampled estimates. Record the ALB metric, statistic, period, time zone and target group. `RequestCountPerTarget` is a target-group average; it does not identify each host's share. Count swipe POSTs in each backend access log within the same interval for the actual distribution.

## Event and leaderboard verification

Compare the before/after processed-event counts, then independently match successful CSV event IDs against SQLite `processed_events` using a read-only connection and bounded inserts into a temporary expected-ID index. Confirm no new dead letters, all resort queues drained and the leaderboard outbox is empty. Save the full client output, including retries and phase counts.

Consumer lag is ingest event creation to SQL start, for successfully committed unique events. Keep queueing and receive-to-SQL breakdowns separate from HTTP and WebSocket latency. Validate actual WebSocket payloads and compare post-run versions/top-10 snapshots with the database. An observed version count gap is separate from swipe loss and should be reported honestly.

See [measured results](RESULTS.md). The full local evidence archives and the submission PDF are separate from source control.
