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
# Expected at API 3.15.3, TCKs at 3.15.3 (upstream main, tagged 3.15.3 locally):
#   Substrates 1099/0/0, Serventis 1302/0/0.  (3.12.0 was 1086/0/0 and 1302/0/0.)
#
# Read the console "Tests run:" total, not a sum of the surefire XML reports: the
# XML total reads lower than the console for Substrates because FiberContractTest's five @Nested
# classes share eight method names and the aggregate report counts each once.
# See docs/CONFORMANCE.md, "The 3.3.0 conformance run".
#
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
FS="$ROOT/fullerstack-substrates"

# Humainary 3.15.3 declares Java 27; its source compiles unchanged for Java 26,
# and this family still builds for Java 26. Maven 3.9 reads MAVEN_ARGS, so the kits and perfkit build
# for Java 26 without any change to Humainary's own scripts or poms.
export MAVEN_ARGS="${MAVEN_ARGS:-} -Djava.version=26 -Dmaven.compiler.release=26"
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
