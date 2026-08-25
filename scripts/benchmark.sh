#!/bin/bash
#
# Benchmark runner for Fullerstack Substrates.
#
# This project has no benchmarks of its own. Measurement is Humainary's
# perfkit-java, run against this provider by Maven coordinate — 207 benchmark
# methods across 32 classes, covering both Substrates and Serventis.
#
#   ./scripts/benchmark.sh                     # quick smoke run, core suite
#   ./scripts/benchmark.sh decision core       # actionable: 3 forks, 8+10 iterations
#   ./scripts/benchmark.sh decision all        # the full inventory (~3 h)
#   ./scripts/benchmark.sh allocation core     # bytes per op, and the sites
#   ./scripts/benchmark.sh run 'PipeOps.async_emit_batch'
#   ./scripts/benchmark.sh list
#
# Read a result against perfkit's own criteria (BENCHMARKS.md): a run is invalid
# with fewer than 3 fork series, missing metrics, or 99.9% confidence error above
# ~10% of the score. Always quote the error bars. The paired `_control_` rows are
# diagnostic — they must stay stable, and are NEVER subtracted from a target.
#
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
FS="$ROOT/fullerstack-substrates"
PK="$ROOT/perfkit-java"

[ -d "$PK" ] || { echo "!! perfkit-java not cloned — see the README bootstrap"; exit 1; }

export SPI_GROUP=io.fullerstack
export SPI_ARTIFACT=fullerstack-substrates
export SPI_VERSION="$(sed -n 's|^  <version>\(.*\)</version>|\1|p' "$FS/pom.xml" | head -1)"

echo "==> installing $SPI_GROUP:$SPI_ARTIFACT:$SPI_VERSION"
( cd "$FS" && mvn -q clean install -DskipTests ) || exit 1

# perfkit refuses a jar older than the provider it was built against, so rebuild.
echo "==> building perfkit against the provider"
( cd "$PK" && ./jmh.sh build ) || exit 1

if (( $# == 0 )); then
  exec "$PK/jmh.sh" run core
fi
exec "$PK/jmh.sh" "$@"
