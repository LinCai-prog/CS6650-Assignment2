# Benchmark results

These are completed, rate-controlled runs. All accepted unique swipes were verified in SQLite and no new dead letters appeared. The queue column is the maximum sampled Ready + Unacked for one resort, not a sum across resorts.

| Run | Consumer workers | Global input /s | HTTP time (s) | HTTP throughput /s | Mean lag (ms) | Resort peak |
|---|---:|---:|---:|---:|---:|---:|
| 1 ingest, 500K | 40 | 750 | 671.45 | 744.65 | 35.555 | 182 |
| 2 ingests, 500K | 40 | 800 | 630.40 | 793.14 | 58.612 | 205 |
| 4 ingests, 500K | 10 | 750 | 680.92 | 734.30 | 33.718 | 117 |
| 4 ingests, 1M | 40 | 300 warmup / 600 main | 1,730.60 | 577.84 | 24.951 | 133 |

The 1M run includes 32,000 warmup and 968,000 main swipes. Measured offered rates were 298.434/s and 596.420/s. All 1,000,000 client requests ultimately returned HTTP 201. Backend POST counts were 250,000, 250,003, 250,003 and 250,004, respectively; the ten excess attempts were deduplicated. The client recorded 236 distinct leaderboard updates versus 237 database version advances. This one-count difference is unresolved; all ten final WebSocket snapshots independently matched SQLite.

At 800/s, a separate four-ingest 1M attempt stopped in warmup when a resort reached 1,004. The staged 300/600 workload first passed 100K, then the full 1M. Rates, database sizes, cache and worker settings varied across runs, so these results do not isolate a speedup caused by additional ingest instances or establish unlimited throughput. The single SQLite writer and observed I/O waiting are the next capacity investigation, not a reason to relax commit durability.
