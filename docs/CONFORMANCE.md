# Conformance

Conformance is defined by Humainary's published TCKs, not by anything in this repository. The
tests that do live here assert nothing about conformance — see [What the local tests
are](#what-the-local-tests-are-and-are-not).

| Suite | Result |
|---|---|
| [`substrates-api-java-tck`](https://github.com/humainary-io/substrates-api-java-tck) 3.0.5 | **970 run · 0 failures · 0 errors** |
| [`serventis-api-java-tck`](https://github.com/humainary-io/serventis-api-java-tck) 3.0.5 | **1227 run · 0 failures · 0 errors** |

```bash
./scripts/tck.sh                              # both
./scripts/tck.sh substrates CellContractTest  # one class
```

The TCK resolves the provider from the local Maven repository, so **an edit that has not been
installed is not under test**. `scripts/tck.sh` installs first for that reason.

---

## Why there is no in-house conformance suite

The in-house suite was 657 tests and every one passed — while the upstream TCK failed 68 and
errored 3. That is not a contradiction, it is the mechanism. Tests written by the same process,
from the same reading, at the same time as the code measure self-consistency rather than
conformance, and several asserted the bugs outright:

```java
-  assertEquals( 2, state.stream().count() );      // §8.1 requires one slot
-  assertEquals( List.of( 42, 42 ), received );    // a delivery was being dropped
-  assertEquals( 6, received.size() );             // §10.3 requires 9
```

A test authored from the implementation's behaviour can only ever confirm it.

**What was given up.** The deleted suite also held things no API-level TCK reaches: internal
invariants (`QChunkTest`, `AwaitBarrierTest`, `FsCircuitMarkerInvariantTest`) and six annotation
contract tests asserting that `@NotNull`, `@Idempotent`, `@Identity`, `@New`, `@Queued` and
`@Tenure` mean what they claim. Those properties are still unguarded. Recoverable from `febe60d`.

---

## What the local tests are, and are not

`src/test/java` holds 25 tests. None of them asserts a spec requirement, and none is a substitute
for the TCK. Every one exists because a **defect was found that the TCK passes straight through**,
and each is held to a rule the deleted suite was not:

> A regression test is only kept if reintroducing the defect makes it fail.

That was checked for each — the bug was put back, the test failed, the bug was removed again. A
test written from the implementation's behaviour can only confirm it; a test written from a
demonstrated defect, and shown to fail without the fix, cannot.

| suite | what it pins |
|---|---|
| `SinkChannelTest` | a sink channel is accepted wherever a provider pipe is; every usage pattern in the `Sink`/`Capture` javadoc |
| `FlowFiberAttachmentTest` | `Flow`/`Fiber` stages run on the target pipe's circuit, across six target shapes |
| `CaptureProvenanceTest` | all three `Capture#current()` cases — caller, emitting circuit, owning circuit for a ticker |
| `TransitCapacityTest` | ring growth *during* a drain, and cascade order across a doubling |

The TCK passed unchanged before and after every defect these cover, which is the argument for
their existence and the limit of their claim.

---

## Where the TCK asserts more than SPEC.md requires

Recorded rather than raised. None is conformance debt.

**§10.3 — hierarchical (STEM) routing is OPTIONAL, tested unconditionally.** §10.3: *"Hierarchical
routing is an OPTIONAL capability. Implementations that do not provide it MUST behave as if
per-pipe routing were always in effect."* §16.3 repeats it. `ConduitContractTest$StemRouting`
constructs a `STEM` conduit and asserts ancestor delivery with no capability probe and no
`assumeTrue`, so an implementation taking the OPTIONAL out cannot pass. Moot here — this provider
now implements the extension — but the defect stands. The sharper half: the TCK requires ancestor
**materialization**, which §10.3 never states.

**§11.4 — the unnamed-ticker default name.** The spec asks only for *"a valid non-empty default
name"*; `TickerContractTest.ticker_withoutExplicitName_usesCircuitName` asserts the circuit's name
specifically. The same §16.3 paragraph *is* explicit for cell/port/pin, so the omission looks
deliberate. This provider was conformant and was changed to align. Upstream should weaken the
assertion to non-empty, which is what the test's own doc comment claims to test.

**§16.1 #7 vs §6.4.1 — editorial.** §16.1 #7 says Window enforcement *"follows the general
SHOULD-detect framework"*; §6.4.1 says MUST for this type by name. The specific and later rule
wins; §16.1 #7's clause list is stale.

**§16.3 — "the existing entry" is ambiguous** for equivalent `State` writes. No TCK test exercises
the non-head case, and the TCK javadoc's stronger reading contradicts §8.1's observable ordering
MUST. Upstream should say "the most-recently-written entry".

---

## Known gaps, not covered by any TCK test

Real, deliberate, and stated so they are not mistaken for conformance.

**`FsRegistrar.register(Pipe)` unwraps `FsPipe.receiver()` and invokes it directly**, bypassing
`Pipe.emit` and its routing decision. For a same-circuit target that is the intended fast path; for
a pipe owned by a *different* circuit it runs that circuit's receiver on the wrong worker. The
registrar does not know its owning circuit, so this is not a one-line fix.

**§5.8's stimulus holder is reached ambiently.** Operators reach `FsOperators.Wrap.wrap(Consumer)`
with no circuit reference, so the per-chain reading arrives through a `ThreadLocal` bound by the
worker. Where no binding exists the accessor falls back to a per-call clock read rather than
failing — a conformance hole that cannot announce itself. The structural fix is to widen the
operator SPI so the chain context arrives as an argument, which is what the Rust projection does
(its `DESIGN.md` D3). `FsWindow`'s lease had the same root cause and the same workaround.

**`FsCircuit`'s public constructor signature changed** from `FsCircuit(Subject<Circuit>)` to
`FsCircuit(FsCortex, Subject<Circuit>)`. `FsCortex.circuit(Name)` is the only construction site in
tree, but any out-of-tree consumer constructing one directly will break.

**Readability trap in `FsCircuit`:** the `private final FsCortex cortex` field sits alongside a
still-used static import of `Substrates.cortex()`. Java resolves `cortex()` in the method namespace
so both are correct, but a reader can misread it. Renaming the field is a safe follow-up.

### One unexplained intermittent error — measured, not reproduced

In **1 of 12** full TCK runs, `CircuitContractTest.dispatch_multipleNamedPipes_preservesAdmittedEmissions`
failed with a `StackOverflowError`. The other 11 were clean, as were five isolated runs of that
class. It has not reproduced and no dump survived.

Recorded rather than dismissed. The test is a stress case (10 threads × 5 000 emissions through 10
named channels) and the paths it exercises are iterative, not recursive — `drainBatchLoop`,
`TransitQueue.drain` and `FsCircuit.drainLoop` are flat loops. The worker is a **virtual**
thread whose stack is heap-allocated in chunks, so the leading hypothesis is chunk-growth failure
under the memory pressure of a shared-JVM full-suite run rather than unbounded recursion. That is a
hypothesis, not a diagnosis. Anyone touching the dispatch core should try to reproduce it under
`-XX:+HeapDumpOnOutOfMemoryError` with a larger heap and a repeated full-suite loop.

---

## Documented limits (§4.1, §4.2)

3.0.5 gives names a **depth floor of 16 segments** and requires an implementation that imposes a
maximum to document it. This one imposes none: depth is an `int` field on `FsName`, not a packed
encoding, so there is no design ceiling and nothing to declare as a limit.

Measured, by successive extension, with traversal, `path()` and `compareTo` exercised at each depth
— the operations §4.2 now brings in scope alongside traversal:

| depth | result |
|---:|---|
| 16 | the guaranteed floor — accepted |
| 32 | accepted (the reference implementation's stated maximum) |
| 64 | accepted, in one operation and by extension |
| 1 024 | accepted |
| 8 192 | accepted — `stream()`, `path()` (64 425 chars) and `compareTo` all fine |
| 65 536 | `OutOfMemoryError` — heap, not a depth rule |

So the practical bound is memory. Portable code should still assume only the guaranteed 16.

---

## Known gaps, added

**The transit ring never shrinks.** `TransitQueue` doubles on demand and keeps its high-water mark
for the life of the circuit: one 5 000-element fan-out leaves 8 192 slots across two arrays, about
128 KB, retained. Allocation profiling reports garbage rather than retention, so nothing currently
measures it. It matters in proportion to how many circuits a process runs.

---

## Closed since the 3.0.2 conformance pass

- **§16.1 #1 confinement in `Flow.pipe`/`Fiber.pipe`** — for this provider's own non-`FsPipe`
  carrier (a `Sink` channel) the operator chain ran on the *caller's* thread, mutating `scan`
  slots and `window` rings off-worker. It now attaches to the target's circuit like every other
  shape. Pinned by `FlowFiberAttachmentTest`.
- **Six provider guards rejected a `Sink` channel** as "not from this runtime provider" —
  `instanceof FsPipe` used where package identity was meant, so a sink channel could not receive
  from a `Port`, a `Basin` drain, a fan-out list or a `Ticker`, nor serve as another sink's
  endpoint. Pinned by `SinkChannelTest`.
- **§11.1 ticker provenance** — a tick emitted to its target from the scheduler thread, so every
  `Capture` carried the scheduler's context where the accessor names the owning circuit
  explicitly. Ticks now emit into a circuit-owned pipe. Pinned by `CaptureProvenanceTest`.
- **A cross-circuit sink endpoint bypassed the sink's own circuit** — the endpoint was held raw,
  so a capture went straight to the far circuit off the calling thread. It is now normalised
  through `circuit.pipe(endpoint)`.
- **§5.8 stimulus time** — the last TCK failure. Time-aware operators now share one reading per
  ingress chain, so a sleeping transit hop cannot age a time-bounded window.
- **§6.4.1 Window lease** — was a bare `Thread`, re-latched per emission, and so could not see a
  window retained into a later callback on the same worker. It is now `(context, generation)`.
  That case was not a stale read: `Flow.window` rewrites one buffer in place, so the retained view
  reported *current* data while presenting itself as an earlier window.
- **§11.3 `Current`** — was a `ConcurrentHashMap` keyed by `Thread.threadId()` that never evicted,
  leaking an entry per thread for the life of the process. Now a `ThreadLocal`, whose entry dies
  with its thread — the lifetime §11.3 actually specifies.
