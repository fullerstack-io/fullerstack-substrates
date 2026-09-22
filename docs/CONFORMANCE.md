# Conformance

Conformance is defined by Humainary's published TCKs, not by anything in this repository. The
tests that do live here assert nothing about conformance — see [What the local tests
are](#what-the-local-tests-are-and-are-not).

| Suite | Result |
|---|---|
| [`substrates-api-java-tck`](https://github.com/humainary-io/substrates-api-java-tck) 3.3.0 | **1067 run · 0 failures · 0 errors** |
| [`serventis-api-java-tck`](https://github.com/humainary-io/serventis-api-java-tck) 3.3.0 | **1227 run · 0 failures · 0 errors** |

```bash
./scripts/tck.sh                              # both
./scripts/tck.sh substrates CellContractTest  # one class
```

The TCK resolves the provider from the local Maven repository, so **an edit that has not been
installed is not under test**. `scripts/tck.sh` installs first for that reason.

---

## The 3.3.0 conformance run

| | |
|---|---|
| **Date** | 2026-09-22 |
| **Provider** | `io.fullerstack:fullerstack-substrates:3.0.0-SNAPSHOT`, built against Substrates/Serventis **3.3.0** |
| **Substrates TCK** | `substrates-api-java-tck` at **3.3.0** (upstream `main` commit, tagged 3.3.0 locally — upstream does not tag) — **1067 run · 0 failures · 0 errors** (992 at 3.1.2; 75 tests added in 12 new contract classes) |
| **Serventis TCK** | `serventis-api-java-tck` at **3.3.0** — **1227 run · 0 failures · 0 errors** (unchanged) |
| **Local suite** | `mvn -o -nsu clean test` — **33 run · 0 failures · 0 errors** (29 + 4 in `CallbackTopologyTerminalTest`, 2026-09-22) |
| **Source changes** | `FsRegistrar` (a registered pipe is submitted, not invoked); `FsHub`, `FsSubscription`, `FsConduit`, `FsChannel` (rebuild memo keyed by subscription). Nothing on the worker loop or the emit path moved. |
| **Baseline before the changes** | 1067 run · **7 failures** — `ConnectedShutdown` (1), `CrossCircuit` (2), `Rebuild` (1), `StemTopology` (1), `Subscriber` (2) |

**What 3.1.2 → 3.3.0 changed.** The Java API surface did not change (only `@SpecDoc` URLs). The
spec did, in one place that this provider had read the old way: **§5.3** now classifies queued work
by the *context it was submitted from* — work from another circuit's context is ingress to the
receiver; a subscribe, close or cleanup submitted from inside a callback is transit — and "transit
is FIFO over every kind of operation it carries, not over emissions alone". **§7.6.1** defines a
subscription's visibility window in the circuit's *logical processing order* (transit ahead of
ingress, FIFO within each), not in admission position; selection and execution are distinct — a
close ends future selection and never retracts a delivery already dispatched. **§9.1**'s
post-close drop is positioned in that same order. The kit's twelve new classes (`CrossCircuit`,
`CrossCircuitIngress`, `CrossConduit`, `ConnectedShutdown`, `Rebuild`, `Provenance`,
`FailureContinuation`, `CoordinationStage`, `StimulusTime`, `StemTopology`, `CallbackTopology`,
`Network`) observe exactly those sentences, and its README gained a table of assertions a
conformance test *may not* make.

**Fix 1 — a registered pipe is submitted to its own circuit, never invoked in the walk
(§5.3, §6.1, §14; closes 6 of the 7).** `FsRegistrar.register(Pipe)` used to unwrap an `FsPipe` to
its bare receiver and store that in the channel's dispatch array, so the delivery ran inline on the
*emitting* worker in the middle of the dispatch walk. That was the "known gap" recorded below at
3.1.2, and 3.3.0 makes it observable three ways: a same-circuit registered pipe's delivery ran
*ahead* of transit the same cascade had already accepted (`SubscriberContractTest
.dispatch_registeredPipe_sequencesBehindAcceptedTransitWork`, `…StateHolderPipes…` — §5.3 "sequenced
behind transit work already accepted … FIFO over every kind of operation"); a pipe owned by another
circuit ran on the wrong worker and saw the wrong `Current` (`CrossCircuitContractTest` ×2,
`StemTopologyContractTest` — §5.3 "work submitted from another circuit's context … is ingress work
to it", §5.1 confinement); and an emission into a pipe whose circuit had closed was still delivered
(`ConnectedShutdownContractTest` — §9.1 "MUST be silently dropped"). The registrar now stores one
`FsRegistrar.Admit` per registered pipe, whose `accept` is `circuit.submit(receiver, value)` —
`FsCircuit.submit` being the one §5.3 routing decision the provider already had, and the one that
carries the §9.1 drop. A registered **receptor** is still invoked in the walk: §6.3 and §16.3 let a
provider store it directly, and it has no circuit to submit to. Consequence stated so nobody reads it
as a bug: a receptor and a pipe registered on one channel no longer have a total invocation order
between them (the receptor runs in the walk, the pipe one transit hop later); the kit README lists
that as an assertion a test may not make, and §7.4 leaves mixed-kind ordering to the projection.

**Fix 2 — the rebuild memo is keyed by subscription, not subscriber (§7.3, §7.5; closes the 7th).**
`FsHub`'s roster and `FsChannel.rebuild`'s "already rebuilt for this pair" memo were keyed by
`FsSubscriber`. Closing a subscription and resubscribing the same subscriber with no emission in
between left the memo untouched, so the new subscription never ran its callback and inherited the
old subscription's receptors (`RebuildContractTest.resubscribe_afterSubscriptionClose_startsFreshPerPair`).
§7.3 says the callback runs "exactly once for that subscription/channel pair" and §7.5 that "each
subscribe operation creates an independent subscription instance", so the pair's key is the
subscription: `FsSubscription` now carries its subscriber, the hub's add/remove jobs carry the
subscription, and the memo is keyed by it. The same change removes a latent double-delivery when one
subscriber was subscribed twice to one conduit (one callback for two pairs, shared receptors); no kit
test pins that, the text does.

**Post-close policy (§9.1 / §9.3 step 1 require it to be documented).** A circuit's `close()`
enqueues the close marker first and then sets `closed`. Work admitted *before* the marker is still
drained by the worker; an emission admitted from a caller context *after* `closed` is set is dropped
at admission, silently, with no side-channel report. A cascade submission raised on the worker while
`closed` is already set — including, since 3.3.0, a registered pipe's delivery — is also dropped, even
when the ingress item it descends from was admitted before the marker: pre-close work drains, but
its cascades are not guaranteed to complete once close has been accepted. Queued `close`/cleanup jobs
never throw synchronously. A `Fault` is raised synchronously only by operations that must return a
new resource or a snapshot (§9.1's "open-required" class).

**A third change, for a gap the kit does not reach (found by reading §7.6.1 against the add job).**
The conduit's subscribe job installed a subscription unless the *conduit* was closed. §7.6.1 puts a
subscription's window between the point its registration job *executes* and the point its close job
executes, in processing order. A close raised on the worker is transit and can execute before the
caller's add job (ingress): the retire ran against an empty roster, and the add then installed a
subscription whose close had already run — it received every later emission and could never be
retired, retire being idempotent. The add job now asks whether the *retire job has executed*
(`FsSubscription.isRetired`, set on the worker by the unsubscribe job) — not whether close was
accepted (`isClosed`, set on the caller), because `subscribe(); emit(X); close(); emit(Y)` from one
caller has the add job execute ahead of the retire job and `X` must be delivered; keying on `isClosed`
lost `X` and failed two kit tests. `SubscriptionCloseBeforeAddTest` reproduces the ordering with a
receptor that closes the second subscription from inside the first emission's cascade.

**Cost, measured (perfkit 3.3.0, `-XX:+UseCompactObjectHeaders`, same machine, provider
installed and the perfkit jar rebuilt before each run).** Fix 2 is cold (subscribe/close jobs and
rebuild only). Fix 1 puts **one transit hop on every same-circuit registered-pipe delivery** and turns
a foreign registered-pipe delivery into the ordinary cross-circuit admission it always should have
been. The hop costs what the suite's own transit unit costs — `TransitOps.transit_fanout_1k_batch`,
13.2 ± 0.9 ns/op, unchanged — not the +3–6 ns the design guessed:

| row (`-f 2 -wi 3 -i 5 -r 1s`, ns/op) | before | after | delta |
|---|---|---|---|
| `ChannelOps.piped_emit_batch` (one registered `circuit.pipe`) | 13.05 ± 2.7 | 25.03 ± 2.8 | **+12.0** |
| `ChannelOps.piped_emit_admission_batch` (admission only) | 10.30 ± 1.2 | 9.74 ± 2.6 | flat |
| `ChannelOps.observed_emit_batch` / `unobserved_emit_batch` (receptor / none) | 10.4–11.5 | 10.8–11.3 | flat |
| `CyclicOps.cyclic_emit_registrar` (pool-backed subscriber, one hop per cycle) | 13.99 ± 0.9 | 31.0 ± 10.0 | +17 |
| `TransitOps.transit_fanout_1k_batch` (the hop unit) | 13.4 ± 10 (f1) | 13.2 ± 0.9 | flat |

Single-fork (`-f 1 -wi 3 -i 3`) sweep of 47 rows: every `PipeOps.*`, `CrossCircuitOps.*`,
`StemOps.baseline_*`/`stem_*` row flat within its error; `StemOps.subscriber_emit_batch` +3 / +34 /
+40 / +122 ns at depth 1 / 3 / 5 / 10 (one registered conduit pipe per ancestor level, so one hop
per level); `MirrorOps.mirror_emit_identity/string` +21 / +27 (pool-backed subscriber, one hop plus
the mirror channel's own dispatch now on the transit side); `CyclicOps.cyclic_emit_*_100k` +11–14.
Why a hop is ≈13 ns and not 3–6 is **not profiled** and is stated as a hypothesis only: the delivery
now crosses `TransitQueue.drain`'s megamorphic `r.accept(v)` site, which cannot inline the receptor
that the channel's single-registration `dispatch` site could, plus `Admit.accept → submit` (volatile
`closed` read, `Thread.currentThread()` compare) and the drain's per-item try/catch and cursor
bookkeeping. If that row matters, the next step is `-prof perfasm` on `piped_emit_batch`, not a
change. Serventis rows and the provider's own tests register receptors only and are unaffected.

**Composed terminals (closed 2026-09-22, repair round 1).** `FsFiber.pipe`/`FsFlow.pipe` terminals used to submit
`channel.cascadeDispatch` — the dispatch consumer assembled at the channel's last rebuild — straight to transit, skipping
the version check "because subscriber state cannot change mid-cascade". 3.3.0 §7.6.1 selects recipients as "exactly
those subscriptions effective at that position", the position at which the channel processes the emission, and says a
subscribe or close raised inside a cascade "takes effect within the current cascade" (§5.3, §7.6.1 consequence 3, §7.6.2
"including when the subscription change was raised from within a cascade"). The kit does not reach this path
(`CallbackTopologyContractTest` emits through the plain channel pipe); a probe did: callback
`conduit.subscribe(S2); viaFiber.emit("E")` gave S2 `[F]` where the plain path gave `[E, F]`. A version check in the
terminal itself cannot fix it — the change may still be ahead of the delivery in transit — so the terminal now submits the
channel (`target.emit(v)` in substance) and `FsChannel.receive` checks at the position of delivery; `cascadeDispatch`,
`stemCascade` and `cascade()` are gone. Hop count unchanged. Pinned by the provider's own
`CallbackTopologyTerminalTest` (fiber and flow × subscribe-then-emit, close-then-emit inside one callback; 4 tests). No
perfkit row ends a fiber/flow at a conduit pipe (every `sink` there is `circuit.pipe(...)`), so the branch was priced with a
scratch JMH row at the perfkit's VM options (`-XX:+UseCompactObjectHeaders`), paired against the pre-change jar,
`-f 3 -wi 3 -i 5 -r 1s`: `fiber.guard.pipe(conduit.get(n))` **41.9 ± 3.7 → 45.0 ± 5.7 ns/op (+3.1)**,
`flow.map.pipe(conduit.get(n))` **41.1 ± 5.6 → 43.7 ± 4.9 (+2.7)**; the untouched `fiber.pipe(circuit.pipe(r))` control
41.7 → 45.5 ± 10.7 and the plain `channel.emit` control 14.9 → 15.6 — the move is at the edge of the predicted +1–3 ns
(one same-class call level, ~6 loads, three branches in `receive`) and inside the run's own noise. The mandated
`ChannelOps|CrossCircuitOps|PipeOps` rows (26, `-f 1 -wi 3 -i 3`) are all flat within error, as predicted: they never enter
the branch.

---

## The 3.1.2 conformance run

| | |
|---|---|
| **Date** | 2026-09-14 |
| **Provider** | `io.fullerstack:fullerstack-substrates:3.0.0-SNAPSHOT`, built against Substrates/Serventis **3.1.2** |
| **Substrates TCK** | `substrates-api-java-tck` at tag **3.1.2** — **992 run · 0 failures · 0 errors** (974 at 3.0.7; 18 tests added) |
| **Serventis TCK** | `serventis-api-java-tck` at tag **3.1.2** — **1227 run · 0 failures · 0 errors** (unchanged) |
| **Local suite** | `mvn -o -nsu clean test` — **29 run · 0 failures · 0 errors** |
| **Source changes** | `FsCircuit`: five methods 3.1 made abstract — `conduit(Name)`, `conduit(Name, Routing)`, `bank(Routing)`, `basin(Name, int)`, `pipe(Name)`; `FsBasin`: a named constructor. Nothing else moved. |

**What 3.0.7 → 3.1.2 changed.** One idea, applied across the circuit factories: the class token is a
**type witness only** — "used only for inference and is not retained" — so every factory gains a
witness-free form and the class-token forms become `default` methods that delegate to it. William's
stated reason is a parameterized emission type such as `Basin<Capture<E>>`, "which a class token
cannot express"; that is the shape a recorder capturing over a sink builds, so the witness-free
form is the one such a caller needs. This provider mirrors the API's own direction: the witness-free
forms are the real implementations and the token forms check the token for `null` (the TCK requires
it) and delegate. Also: a **named basin** (`basin(Name, int)`), a **named discarding pipe**
(`pipe(Name)`, defined by the spec as the empty-target-list form and implemented as exactly that
call), tighter lifecycle prose (providers document whether pre-close work drains or is dropped;
`subscribe` validates synchronously), and one new ordering test —
`close_nestedScopes_closesOwnResourcesBeforeDescendants` — which `FsScope.close` already satisfied.
Serventis 3.1.2 re-pins to Substrates 3.1.2 and changes spelling; its TCK is unchanged.

**State, Subject and Capture did not move.** Checked specifically, because the question came up:
§8 is byte-identical between 3.0.7 and 3.1.2, `Subject` still exposes state through a getter only,
`Capture.state()` is still the provider's measures, and no `emit(E, State)` exists. This provider's
subjects and captures still publish the empty state, which the spec permits ("the default is the
empty state") and the TCK does not test either way. A prototype of 2026-09-14 that folded emitted
State into a subject and stamped it into captures was reverted the same day: publishing is the
provider's by the API's words, but the write route was not, and the consumer can mint its own
capture through the API instead (Custodis `DECISIONS-v0.8.md`).

---

## The 3.0.7 conformance run

Recorded here because this is the provider's repository; until now the only written record of a
run lived in the repository that happened to trigger it.

| | |
|---|---|
| **Date** | 2026-09-08 |
| **Provider** | `io.fullerstack:fullerstack-substrates:3.0.0-SNAPSHOT`, built against Substrates/Serventis **3.0.7** |
| **Substrates TCK** | `substrates-api-java-tck` at tag **3.0.7** — **974 run · 0 failures · 0 errors** |
| **Serventis TCK** | `serventis-api-java-tck` at tag **3.0.7** — **1227 run · 0 failures · 0 errors** |
| **Local suite** | `mvn -o -nsu clean test` — **29 run · 0 failures · 0 errors**, 5 classes |
| **Source changes** | **none** — `src/main` compiled unchanged at 3.0.7 |

```bash
mvn -o -nsu clean install -DskipTests            # the jar under test
cd ../substrates-api-java-tck && ./mvnw -o -nsu test \
  -Dsubstrates.spi.groupId=io.fullerstack \
  -Dsubstrates.spi.artifactId=fullerstack-substrates \
  -Dsubstrates.spi.version=3.0.0-SNAPSHOT
```

**Read 974, not 966.** Surefire's aggregate XML report undercounts this suite by eight.
`FiberContractTest` splits into five `@Nested` classes and eight method names appear in more than
one of them — `above_greaterValues_passesOnlyMatches`, `below_lesserValues_passesOnlyMatches`,
`clamp_outsideValues_coercesIntoRange`, `deadband_insideAndOutsideValues_dropsInclusiveBand`,
`max_belowAtAboveBound_passesAtOrBelow`, `min_belowAtAboveBound_passesAtOrAbove`,
`pipe_nullTarget_throwsNullPointerException`, `range_insideAndOutsideValues_passesInclusiveInterval`.
`TEST-…FiberContractTest.xml` carries `tests="176"` over 184 `<testcase>` elements, so summing the
`tests` attribute across the 33 report files gives 966 while 974 cases actually ran. The console
`Tests run:` total and the per-class lines both agree on 974; the eight are executed and passing,
not skipped. Any tool that totals the XML attribute will report the low number.

**What 3.0.5 → 3.0.7 changed.** §9.1 attributes a fault to the rejecting guard; §10.4 states that a
bank is not closed by its circuit closing; source-retention `@New(conditional)` annotations; one
`default` `Spliterator` on `Extent`; six added TCK tests. Serventis is version pins only. None of
it moved this implementation — the bump is `pom.xml` and this sentence.

The figures this file and `scripts/tck.sh` carried before today — **970** and **960** — disagreed
with each other, so at most one could have been right. 960 is explained: six fewer tests at 3.0.5
puts the console total at 968, and 968 − 8 is exactly the XML-attribute reading — so `tck.sh` was
almost certainly quoting the undercount (unverified — checking 3.0.5 out to confirm was out of
scope). 970 has no such account and was not re-measured when it was written. Only the 974 above is
a measured number.

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

`src/test/java` holds 33 tests across 6 classes. None asserts a spec requirement, and none is a substitute
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
| `ExtentDepthTest` | that every walk reached through the name hierarchy is iterative, to 20 000 segments |

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

*(Closed at 3.3.0.)* **`FsRegistrar.register(Pipe)` unwrapped `FsPipe.receiver()` and invoked it
directly**, bypassing `Pipe.emit` and its routing decision — a foreign pipe's receiver ran on the
wrong worker, and a same-circuit one ran ahead of accepted transit. The earlier note that "the
registrar does not know its owning circuit, so this is not a one-line fix" was a misdiagnosis: the
*pipe* knows its circuit, and the fix was the one line described under the 3.3.0 run.

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
