#!/bin/bash
export BENCH_DOWNLOAD_SCRIPT="download-hits-parquet-single"
# Embedded engine: no daemon, so stop/start/check are no-ops and the
# driver drops caches between queries instead of restarting.
export BENCH_RESTARTABLE=no
# One process per query with no shared scheduler, so concurrent
# connections would oversubscribe RAM rather than measure throughput.
export BENCH_CONCURRENT_DURATION="${BENCH_CONCURRENT_DURATION:-0}"
exec ../lib/benchmark-common.sh
