# Fullerstack Substrates

SPI provider implementation of the [Humainary Substrates API](https://github.com/humainary-io/substrates-api-java) — deterministic signal circulation infrastructure for Java 26.

| | |
|---|---|
| **Version** | 3.0.0-SNAPSHOT |
| **API** | Substrates 3.0.2 + Serventis 3.0.2 |
| **Java** | 26 (Virtual Threads + Preview) |
| **Conformance** | Substrates TCK **960/960** · Serventis TCK **1227/1227** |
| **Benchmarks** | [perfkit-java](https://github.com/humainary-io/perfkit-java) — 207 methods, 32 classes |

## Conformance is external

This project has **no tests and no benchmarks of its own**. Both are Humainary's, run
against this provider by Maven coordinate exactly as they would be against any other.

That is a deliberate change from earlier versions, and it was worth making. The in-house
suite was 657 tests, all green — while the upstream TCK failed 68 and errored 3. Not a
contradiction, but the mechanism: tests written by the same process, from the same reading,
at the same time as the code measure self-consistency rather than conformance. Several
asserted the bugs outright.

```bash
./scripts/tck.sh                 # both TCKs against this provider
./scripts/tck.sh substrates      # one suite
./scripts/benchmark.sh decision core
```

## Prerequisites

1. **Java 26** via [SDKMAN](https://sdkman.io/):
   ```bash
   sdk install java 26.ea.35-open && sdk use java 26.ea.35-open
   ```

2. **The Humainary repositories** (not on Maven Central). The APIs are required to build;
   the TCKs and perfkit are required to verify:
   ```bash
   for r in substrates-api-java serventis-api-java specs-api-java \
            substrates-api-java-tck serventis-api-java-tck perfkit-java; do
     git clone https://github.com/humainary-io/$r.git
   done
   for r in specs-api-java substrates-api-java serventis-api-java; do
     ( cd $r && mvn clean install -DskipTests )
   done
   ```
   `specs-api-java` installs first: as of 3.0.2 the APIs depend on it for the
   `@SpecDoc` / `@SpecRef` traceability annotations.

## Build

```bash
mvn clean install        # builds; there are no tests to run
./scripts/tck.sh         # conformance
```

## Usage

The artifact is published to [GitHub Packages](https://github.com/fullerstack-io/fullerstack-humainary/packages), which requires authentication even for public packages — so consumers need a repository declaration **and** credentials.

**1. Repository and dependency:**

```xml
<repositories>
  <repository>
    <id>github-fullerstack</id>
    <url>https://maven.pkg.github.com/fullerstack-io/fullerstack-humainary</url>
    <releases><enabled>true</enabled></releases>
    <snapshots><enabled>true</enabled></snapshots>
  </repository>
</repositories>

<dependencies>
  <dependency>
    <groupId>io.fullerstack</groupId>
    <artifactId>fullerstack-substrates</artifactId>
    <version>3.0.0-SNAPSHOT</version>
  </dependency>
</dependencies>
```

**2. Credentials in `~/.m2/settings.xml`:**

```xml
<settings>
  <servers>
    <server>
      <id>github-fullerstack</id>
      <username>YOUR_GITHUB_USERNAME</username>
      <password>${env.GITHUB_TOKEN}</password>
    </server>
  </servers>
</settings>
```

A [personal access token](https://github.com/settings/tokens) with `read:packages`, exported as `GITHUB_TOKEN`.

**3. The provider loads itself.** `FsCortexProvider` is discovered via `ServiceLoader`:

```java
import static io.humainary.substrates.api.Substrates.*;

var cortex  = cortex();
var circuit = cortex.circuit(cortex.name("example"));

var conduit = circuit.conduit(cortex.name("events"), String.class);

conduit.subscribe(circuit.subscriber(
    cortex.name("logger"),
    (subject, registrar) -> registrar.register(System.out::println)
));

conduit.get(cortex.name("source")).emit("hello");
circuit.await();
circuit.close();
```

## Architecture

Each circuit runs on a single virtual thread with two internal queues:

- **IngressQueue** — wait-free MPSC for external emissions, backed by a 128-slot `QChunk`
- **TransitQueueRing** — single-threaded power-of-2 ring for cascading emissions, drained to
  exhaustion before the next ingress item

That priority is what §5.3 calls **causal completion**: every cascading effect of one
emission resolves before the next external emission is admitted, without locks.

See [Architecture](docs/ARCHITECTURE.md) for VarHandle memory ordering, false-sharing
prevention, the QChunk layout, and the §5.8 stimulus-time and §6.4.1 Window-lease designs.

## Measurement

Run through perfkit rather than quoting numbers here — figures move with the host, and this
project's own history includes benchmark rows that measured nothing at all.

```bash
./scripts/benchmark.sh decision core     # actionable
./scripts/benchmark.sh allocation core   # bytes/op and allocation sites
```

Read results against perfkit's criteria in its `BENCHMARKS.md`: a run is **invalid** below
3 fork series or above ~10% confidence error, scores are always published with error bars,
and paired `_control_` rows are diagnostic — never subtracted from a target.

## Documentation

| Document | What it covers |
|----------|---------------|
| [Developer Guide](docs/DEVELOPER-GUIDE.md) | Usage patterns, best practices, Serventis integration |
| [Architecture](docs/ARCHITECTURE.md) | Implementation decisions, queue internals, async model, thread safety |
| [Conformance](docs/CONFORMANCE.md) | How the TCKs are run, what is measured, and open questions against the spec |
| [2.9 Migration](docs/2.9-MIGRATION.md) | Historical — upgrade notes from 2.8 to 2.9 |

### External References

| Resource | Description |
|----------|-------------|
| [Substrates Specification](https://github.com/humainary-io/substrates-api-spec) | Formal spec + rationale |
| [Substrates API](https://github.com/humainary-io/substrates-api-java) · [TCK](https://github.com/humainary-io/substrates-api-java-tck) | API and its conformance suite |
| [Serventis API](https://github.com/humainary-io/serventis-api-java) · [TCK](https://github.com/humainary-io/serventis-api-java-tck) | Semiotic observability instruments |
| [perfkit-java](https://github.com/humainary-io/perfkit-java) | The JMH suite for providers |

## License

Apache License 2.0

All API design by **[William Louth](https://humainary.io/)** and the **[Humainary](https://humainary.io/)** project.
