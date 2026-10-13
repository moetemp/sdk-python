# Benchmark results

Local runs on one machine, one client process, on Core's Redis store. They
give a starting point, not production numbers. Redis runs on the same host
as the dev server and the client. Other load tests ran on the machine at
the same time, so the tails are noisier than they would be on an idle host.

Machine: Apple M4 Max, 14 cores, Python 3.13.6, Redis 8.10.1, the Temporal dev server.

Commands, with `<port>` the Redis of each persistence mode:

```
uv run python -m tests.contrib.streams.bench.run_bench perf04-activity --redis redis://localhost:<port>/0 --count 300
uv run python -m tests.contrib.streams.bench.run_bench perf04-workflow --redis redis://localhost:<port>/0 --count 200
uv run python -m tests.contrib.streams.bench.run_bench perf05a --redis redis://localhost:<port>/0 --duration 10 --rate 2000
```

| Scenario | Redis | First append ms | p50 ms | p95 ms | p99 ms | Records/s | Behind schedule ms |
| --- | --- | --- | --- | --- | --- | --- | --- |
| perf04-activity | no persistence | 3.2 | 2.85 | 8.95 | 17.22 |  |  |
| perf04-activity | AOF, `appendfsync everysec` | 3.61 | 1.38 | 6.17 | 12.41 |  |  |
| perf04-activity | AOF, `appendfsync always` | 12.37 | 6.19 | 10.49 | 15.4 |  |  |
| perf04-workflow | no persistence |  | 3.2 | 5.71 | 8.59 |  |  |
| perf04-workflow | AOF, `appendfsync everysec` |  | 3.76 | 6.01 | 9.41 |  |  |
| perf04-workflow | AOF, `appendfsync always` |  | 7.96 | 11.84 | 17.67 |  |  |
| perf05a | no persistence |  | 2.43 | 3.88 | 4.93 | 1980.1 | 2.01 |
| perf05a | AOF, `appendfsync everysec` |  | 3.09 | 5.51 | 17.8 | 1980.2 | 39.74 |
| perf05a | AOF, `appendfsync always` |  | 1808.86 | 3011.79 | 3109.91 | 1361.4 | 3123.03 |

PERF-04's Workflow case measures a record from the publish call in Workflow
code to the reader receiving it. Core appends a Workflow's records when the
activation that published them completes, before it reports the Workflow
Task, so that latency is close to an Activity append's.

PERF-05a sends batches of 10 records at a target of 2,000 records a second.
With `appendfsync always`, every append waits for an fsync, and the
producer can't keep the 200 appends a second this target needs: it fell
about three seconds behind, and the latency includes that wait. The other
two modes keep up.

## Raw output

perf04-activity, no persistence:

```json
{"machine": {"platform": "macOS-26.6.2-arm64-arm-64bit-Mach-O", "cpu": "Apple M4 Max", "cores": "14", "memory_bytes": "38654705664", "python": "3.13.6"}, "redis": "8.10.1", "result": {"scenario": "perf04-activity", "gap_ms": 20, "first_append_ms": 3.2, "count": 299, "p50_ms": 2.85, "p95_ms": 8.95, "p99_ms": 17.22, "max_ms": 20.82, "mean_ms": 3.51}}
```

perf04-activity, AOF, `appendfsync everysec`:

```json
{"machine": {"platform": "macOS-26.6.2-arm64-arm-64bit-Mach-O", "cpu": "Apple M4 Max", "cores": "14", "memory_bytes": "38654705664", "python": "3.13.6"}, "redis": "8.10.1", "result": {"scenario": "perf04-activity", "gap_ms": 20, "first_append_ms": 3.61, "count": 299, "p50_ms": 1.38, "p95_ms": 6.17, "p99_ms": 12.41, "max_ms": 14.64, "mean_ms": 2.52}}
```

perf04-activity, AOF, `appendfsync always`:

```json
{"machine": {"platform": "macOS-26.6.2-arm64-arm-64bit-Mach-O", "cpu": "Apple M4 Max", "cores": "14", "memory_bytes": "38654705664", "python": "3.13.6"}, "redis": "8.10.1", "result": {"scenario": "perf04-activity", "gap_ms": 20, "first_append_ms": 12.37, "count": 299, "p50_ms": 6.19, "p95_ms": 10.49, "p99_ms": 15.4, "max_ms": 19.89, "mean_ms": 6.7}}
```

perf04-workflow, no persistence:

```json
{"machine": {"platform": "macOS-26.6.2-arm64-arm-64bit-Mach-O", "cpu": "Apple M4 Max", "cores": "14", "memory_bytes": "38654705664", "python": "3.13.6"}, "redis": "8.10.1", "result": {"scenario": "perf04-workflow", "gap_ms": 20, "count": 200, "p50_ms": 3.2, "p95_ms": 5.71, "p99_ms": 8.59, "max_ms": 12.12, "mean_ms": 3.25}}
```

perf04-workflow, AOF, `appendfsync everysec`:

```json
{"machine": {"platform": "macOS-26.6.2-arm64-arm-64bit-Mach-O", "cpu": "Apple M4 Max", "cores": "14", "memory_bytes": "38654705664", "python": "3.13.6"}, "redis": "8.10.1", "result": {"scenario": "perf04-workflow", "gap_ms": 20, "count": 200, "p50_ms": 3.76, "p95_ms": 6.01, "p99_ms": 9.41, "max_ms": 9.85, "mean_ms": 3.59}}
```

perf04-workflow, AOF, `appendfsync always`:

```json
{"machine": {"platform": "macOS-26.6.2-arm64-arm-64bit-Mach-O", "cpu": "Apple M4 Max", "cores": "14", "memory_bytes": "38654705664", "python": "3.13.6"}, "redis": "8.10.1", "result": {"scenario": "perf04-workflow", "gap_ms": 20, "count": 200, "p50_ms": 7.96, "p95_ms": 11.84, "p99_ms": 17.67, "max_ms": 19.08, "mean_ms": 8.43}}
```

perf05a, no persistence:

```json
{"machine": {"platform": "macOS-26.6.2-arm64-arm-64bit-Mach-O", "cpu": "Apple M4 Max", "cores": "14", "memory_bytes": "38654705664", "python": "3.13.6"}, "redis": "8.10.1", "result": {"scenario": "perf05a", "streams": 1, "readers_per_stream": 1, "record_bytes": 100, "batch": 10, "target_rate_per_stream": 2000.0, "max_behind_schedule_ms": 2.01, "sent_records": 20010, "delivered": 20010, "expected_deliveries": 20010, "complete": true, "published_records_per_s": 2001.0, "delivered_records_per_s": 1980.1, "delivered_bytes_per_s": 198012.7, "count": 20010, "p50_ms": 2.43, "p95_ms": 3.88, "p99_ms": 4.93, "max_ms": 7.83, "mean_ms": 2.69}}
```

perf05a, AOF, `appendfsync everysec`:

```json
{"machine": {"platform": "macOS-26.6.2-arm64-arm-64bit-Mach-O", "cpu": "Apple M4 Max", "cores": "14", "memory_bytes": "38654705664", "python": "3.13.6"}, "redis": "8.10.1", "result": {"scenario": "perf05a", "streams": 1, "readers_per_stream": 1, "record_bytes": 100, "batch": 10, "target_rate_per_stream": 2000.0, "max_behind_schedule_ms": 39.74, "sent_records": 20010, "delivered": 20010, "expected_deliveries": 20010, "complete": true, "published_records_per_s": 2001.0, "delivered_records_per_s": 1980.2, "delivered_bytes_per_s": 198018.9, "count": 20010, "p50_ms": 3.09, "p95_ms": 5.51, "p99_ms": 17.8, "max_ms": 47.55, "mean_ms": 3.65}}
```

perf05a, AOF, `appendfsync always`:

```json
{"machine": {"platform": "macOS-26.6.2-arm64-arm-64bit-Mach-O", "cpu": "Apple M4 Max", "cores": "14", "memory_bytes": "38654705664", "python": "3.13.6"}, "redis": "8.10.1", "result": {"scenario": "perf05a", "streams": 1, "readers_per_stream": 1, "record_bytes": 100, "batch": 10, "target_rate_per_stream": 2000.0, "max_behind_schedule_ms": 3123.03, "sent_records": 13760, "delivered": 13760, "expected_deliveries": 13760, "complete": true, "published_records_per_s": 1376.0, "delivered_records_per_s": 1361.4, "delivered_bytes_per_s": 136139.5, "count": 13760, "p50_ms": 1808.86, "p95_ms": 3011.79, "p99_ms": 3109.91, "max_ms": 3130.54, "mean_ms": 1550.78}}
```
