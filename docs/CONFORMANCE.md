# Conformance

Conformance is defined by Humainary's published TCKs, not by anything in this repository. The
tests that do live here assert nothing about conformance — see [What the local tests
are](#what-the-local-tests-are-and-are-not).

| Suite | Result |
|---|---|
| [`substrates-api-java-tck`](https://github.com/humainary-io/substrates-api-java-tck) 3.12.0 | **1086 run · 0 failures · 0 errors** |
| [`serventis-api-java-tck`](https://github.com/humainary-io/serventis-api-java-tck) 3.12.0 | **1302 run · 0 failures · 0 errors** |

```bash
./scripts/tck.sh                              # both
./scripts/tck.sh substrates CellContractTest  # one class
```

The TCK resolves the provider from the local Maven repository, so **an edit that has not been
installed is not under test**. `scripts/tck.sh` installs first for that reason. Humainary 3.12.0
declares Java 27; the script builds the kits for Java 26 through `MAVEN_ARGS` (see the README's
*Java version*).

The run for each earlier release, and what it changed here, is in this file's git history.

---

## The 3.12.0 conformance run

| | |
|---|---|
| **Date** | 2026-10-22 |
| **Provider** | `io.fullerstack:fullerstack-substrates:3.0.0-SNAPSHOT`, built against Substrates/Serventis **3.12.0** |
| **Substrates TCK** | `substrates-api-java-tck` at **3.12.0** (upstream `main` commit `16de9e1`, tagged 3.12.0 locally) — **1086 run · 0 failures · 0 errors** |
| **Serventis TCK** | `serventis-api-java-tck` at **3.12.0** (`36c3f8f`) — **1302 run · 0 failures · 0 errors** |
| **Baseline before the changes** | compiled unchanged; 1086 run · **2 failures** (Serventis 1302/1302) |

**What 3.7.0 → 3.12.0 changed.** The Java API moved little: `Circuit.pipe(List)` may return the sole
target itself when it is already on the circuit (`@New(conditional = true)`), an unnamed ticker takes
the circuit's name, and `relate`'s javadoc fixes its state across a throwing operator. The
specification tightened more:
- a `State` write matching an existing slot **must** return the same instance (was *may*);
- each operator's state across a failing function is specified: `relate`, `edge` and `rolling` keep
  the input, `scan` replaces its state only on a normal return, `tumble` counts only combined inputs;
- `diff` with an initial value drops a first emission equal to it;
- a subscription closed before its registration job runs takes no effect, its `onClose` firing once;
- `Window` temporal misuse and `Pin` context misuse are **must** detect;
- `NoSuchElementException` for an empty `Window`'s `first()`/`last()`;
- the routing selector is part of the required `conduit` and `bank` forms.

Every module now declares Java 27; the source compiles unchanged for Java 26, which this family stays
on.

| Kit failure | What 3.12.0 says | Fix |
|---|---|---|
| `FiberContractTest.relate_operationThrows_advancesPreviousInput` | §6.2.3: Relate's previous input advances to the incoming value, and is kept when the operator throws | `FsOperators.Relate` called the operator before advancing `prev`, so a throw left `prev` behind. It now advances first, as `Flow.relate` (`FsFlow.Relation`) already did. |
| `FiberContractTest.steady_gradualDrift_resetsAgainstCandidate` | §6.2.3: the generalized `steady` compares each value with the run's **candidate**, not with the value before it | `FsOperators.SteadyPredicate` compared with the previous value, so a drift that stays close step by step never reset. It now holds the candidate and resets it on a miss. |

**Four more, found by review rather than by the kit.** The kit tests the failing-function rule only for
`relate` and `scan`, and the same-instance `State` rule only at the newest slot, so these passed it while
broken. Each now has a regression test in `FailingFunctionStateTest` that failed before its fix:

| Rule (3.12.0) | Defect | Fix |
|---|---|---|
| §6.2.3: Edge's previous input advances even when the predicate throws | `FsOperators.Edge` tested before advancing `prev` | `prev` is written first, as in `Relate` |
| §6.2.3: Tumble counts only successfully combined inputs | `FsOperators.Tumble` advanced its count, and reset at a boundary, before the combiner ran — a failed input counted, and a failure on the last input discarded the batch | the combiner runs first; only a normal return moves the count or the accumulator |
| §6.2.3: a failing predicate leaves state as it was | `FsOperators.Integrate` stored the new accumulator before testing `fire` | `fire` is tested on the candidate, and the state is committed after |
| §8.1/§16.3: an equal write returns the same instance, the slot keeping its position | `FsState` returned `this` only when the match was the newest slot; an equal write to an older slot built a new state and moved it to the head | `this` whenever the matching slot's value is equal, wherever it sits |

Serventis 3.12.0 needed nothing here (the provider implements Substrates only). For consumers it is
source-compatible: instrument constructors take `Conduit<? super Sign>`, and no sign or dimension was
added or removed.

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

`src/test/java` holds 44 tests across 11 classes. None asserts a spec requirement, and none is a
substitute for the TCK. Every one exists because a **defect was found that the TCK passes straight
through**, and each is held to a rule the deleted suite was not:

> A regression test is only kept if reintroducing the defect makes it fail.

That was checked for each — the bug was put back, the test failed, the bug was removed again. A
test written from the implementation's behaviour can only confirm it; a test written from a
demonstrated defect, and shown to fail without the fix, cannot.

| suite | tests | what it pins |
|---|---:|---|
| `SinkChannelTest` | 10 | a sink channel is accepted wherever a provider pipe is; every usage pattern in the `Sink`/`Capture` javadoc |
| `FlowFiberAttachmentTest` | 6 | `Flow`/`Fiber` stages run on the target pipe's circuit, across six target shapes |
| `TransitCapacityTest` | 6 | ring growth *during* a drain, and cascade order across a doubling |
| `CallbackTopologyTerminalTest` | 4 | §7.6.1: a topology change inside a cascade takes effect at its own position, through a composed terminal |
| `ExtentDepthTest` | 4 | every walk reached through the name hierarchy is iterative, to 20 000 segments |
| `CaptureProvenanceTest` | 3 | all three `Capture#current()` cases — caller, emitting circuit, owning circuit for a ticker |
| `ChanceReplayTest` | 3 | §5.1: `chance` replays from its published seed in a fresh JVM |
| `SubscriptionCloseBeforeAddTest` | 1 | §7.6.1: a subscription closed before its registration job runs is never selected |
| `FailingFunctionStateTest` | 5 | 3.12.0 §6.2.3 / §8.1: `edge`, `tumble`, `integrate` across a failing function; an equal `State` write to an older slot returns the same instance |
| `TickerReanchorTest` | 1 | §11.4: a re-anchor after falling behind shifts the grid's phase rather than freezing for seq × interval |
| `PoolGrowthTest` | 1 | a pool resolving many distinct names grows in place, not by copying its map per name |

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

---

## Known gaps, not covered by any TCK test

Real, deliberate, and stated so they are not mistaken for conformance.

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
named channels) and the paths it exercises are iterative, not recursive — `drainBatch`,
`TransitQueue.drain` and `FsCircuit.drainLoop` are flat loops. The worker is a **virtual**
thread whose stack is heap-allocated in chunks, so the leading hypothesis is chunk-growth failure
under the memory pressure of a shared-JVM full-suite run rather than unbounded recursion. That is a
hypothesis, not a diagnosis. Anyone touching the dispatch core should try to reproduce it under
`-XX:+HeapDumpOnOutOfMemoryError` with a larger heap and a repeated full-suite loop.

---

## Documented limits (§4.1, §4.2)

3.0.5 gives names a **depth floor of 16 segments** and requires an implementation that imposes a
maximum to document it. This one imposes none — depth is an `int` field on `FsName`, not a packed
encoding — so the bound is memory.

**That claim was briefly false, and the checking is the point.** §4.2 extends depth from traversal
to *every operation reached through the hierarchy*, and two of ours were recursive parent walks:
`FsName.path(Function)` and `FsSubject.path()`. Each threw `StackOverflowError` on a 20 000-segment
name where `foldTo` and `stream` returned normally — one method with a ceiling far below the
type's, which is exactly the shape 3.0.5 removed upstream when it rewrote `Extent.foldTo`
iteratively.

Both overrides are gone rather than reimplemented. `Extent.path(mapper, separator)` already folds
from the root iteratively and everything funnels into it, so `FsSubject` inherits the default
outright and `FsName` supplies only the signature adaptation `Name.path(Function)` requires. The
optimisation they were written for — avoiding an `Optional` per level — was against an
implementation upstream no longer has.

Measured after the change, by successive extension:

| depth | result |
|---:|---|
| 16 | the guaranteed floor — accepted, built in one operation and by extension |
| 32 | accepted (the reference implementation's stated maximum) |
| 8 192 | `stream()`, `path()` (64 425 chars) and `compareTo` all fine |
| 20 000 | every walk iterative — `stream`, `foldTo`, `path()`, `path(char)`, `path(Function)` |
| 65 536 | `OutOfMemoryError` — heap, not a depth rule |

Pinned by `ExtentDepthTest`. Portable code should still assume only the guaranteed 16.

---

## Known gaps, added

**§6.4.1 — a `Window` retained into a later callback on the same worker is not always detected**
(MUST since 3.12.0's §16.1 table agrees with §6.4.1). The lease is `(context, generation)` and the
generation moves when the stage emits its next window, so a window kept in a field and used from an
unrelated later callback on the same worker, before that stage emits again, still validates. Ending
the lease when the downstream call returns would close it, but it adds a store on the emission path
(the performance rules require a profile and a prediction first) and would invalidate a window
delivered one queued hop later through a registered pipe — whether that delivery counts as "the
callback it was handed to" needs deciding first. Open; not fixed in the 3.12.0 pass.

**The transit ring never shrinks.** `TransitQueue` doubles on demand and keeps its high-water mark
for the life of the circuit: one 5 000-element fan-out leaves 8 192 slots across two arrays, about
128 KB, retained. Allocation profiling reports garbage rather than retention, so nothing currently
measures it. It matters in proportion to how many circuits a process runs.

---
