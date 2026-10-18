# Stream tests

Most stream tests run on the stock dev server. The ones that need the server's stream notifier
skip there with "no stream notifier".

## Live tests

`scripts/streams_live_lane.sh` starts a server with the stream notifier, short poll and recheck
limits, and a Redis Cluster of three primaries with one replica each, unless
`STREAMS_REDIS_CLUSTER_URL` names one. It then runs the stream tests against them, so the live
cases run on memory, Redis and the cluster, the driver cases too. Unless
`STREAMS_ARCHIVE_S3_URL` names a bucket, it starts MinIO in Docker for the archive cases. It
fails when a live test skips or when a gated test did not pass.

```bash
SERVER_BIN=/path/to/temporal-server PORT_BASE=7941 \
  STREAMS_REDIS_URL=redis://127.0.0.1:6441/0 bash scripts/streams_live_lane.sh
```
