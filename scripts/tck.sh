#!/bin/bash
#
# TCK runner for Fullerstack Substrates.
#
# This project has no conformance tests of its own. Conformance is defined by
# Humainary's published TCKs, which are run against this provider by Maven
# coordinate — the same way any other provider would be measured.
#
#   ./scripts/tck.sh                  # both suites
#   ./scripts/tck.sh substrates       # Substrates only
#   ./scripts/tck.sh serventis        # Serventis only
#   ./scripts/tck.sh substrates CellContractTest   # one contract class
#
# Expected: Substrates 960/0/0, Serventis 1227/0/0.
#
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
FS="$ROOT/fullerstack-substrates"
GROUP=io.fullerstack
ARTIFACT=fullerstack-substrates
VERSION="$(sed -n 's|^  <version>\(.*\)</version>|\1|p' "$FS/pom.xml" | head -1)"

WHICH="${1:-both}"
ONLY="${2:-}"

# The TCK resolves the provider from the local Maven repository, so an edit that
# is not installed is not under test.
echo "==> installing $GROUP:$ARTIFACT:$VERSION"
( cd "$FS" && mvn -q clean install -DskipTests ) || exit 1

run () {
  local repo="$1" name="$2"
  [ -d "$ROOT/$repo" ] || { echo "!! $repo not cloned — see the README bootstrap"; return 1; }
  echo
  echo "==> $name TCK"
  local args=( -Dsubstrates.spi.groupId="$GROUP"
               -Dsubstrates.spi.artifactId="$ARTIFACT"
               -Dsubstrates.spi.version="$VERSION" )
  [ -n "$ONLY" ] && args+=( -Dtest="$ONLY" )
  ( cd "$ROOT/$repo" && ./mvnw test "${args[@]}" ) \
    | grep -E "^\[(INFO|ERROR)\] Tests run:.*Skipped: [0-9]+$|^\[ERROR\]   [A-Za-z]|BUILD"
}

case "$WHICH" in
  substrates) run substrates-api-java-tck Substrates ;;
  serventis)  run serventis-api-java-tck  Serventis  ;;
  both)       run substrates-api-java-tck Substrates
              # The Serventis TCK depends on the Substrates TCK artifact.
              ( cd "$ROOT/substrates-api-java-tck" && ./mvnw -q install -DskipTests ) >/dev/null 2>&1
              run serventis-api-java-tck  Serventis  ;;
  *) echo "usage: $0 [both|substrates|serventis] [TestClass]" >&2; exit 2 ;;
esac
