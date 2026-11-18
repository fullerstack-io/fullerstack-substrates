# Conformance

Conformance is defined by Humainary's published TCKs, not by anything in this repository. The
tests that do live here assert nothing about conformance — see [What the local tests
are](#what-the-local-tests-are-and-are-not).

| Suite | Result |
|---|---|
| [`substrates-api-java-tck`](https://github.com/humainary-io/substrates-api-java-tck) 3.15.3 | **1099 run · 0 failures · 0 errors** |
| [`serventis-api-java-tck`](https://github.com/humainary-io/serventis-api-java-tck) 3.15.3 | **1302 run · 0 failures · 0 errors** |

```bash
./scripts/tck.sh                              # both
./scripts/tck.sh substrates CellContractTest  # one class
```

The TCK resolves the provider from the local Maven repository, so **an edit that has not been
installed is not under test**. `scripts/tck.sh` installs first for that reason. Humainary 3.15.3
declares Java 27; the script builds the kits for Java 26 through `MAVEN_ARGS` (see the README's
*Java version*).

The run for each earlier release, and what it changed here, is in this file's git history.

---

## The 3.15.3 conformance run

| | |
|---|---|
| **Date** | 2026-11-18 |
| **Provider** | `io.fullerstack:fullerstack-substrates:3.15.3-SNAPSHOT`, built against Substrates/Serventis **3.15.3** |
| **Substrates TCK** | `substrates-api-java-tck` at **3.15.3** (upstream `main` commit `2df44a3`, tagged 3.15.3 locally) — **1099 run · 0 failures · 0 errors** |
| **Serventis TCK** | `serventis-api-java-tck` at **3.15.3** (`dc4c373`) — **1302 run · 0 failures · 0 errors** |
| **Baseline before the changes** | compiled unchanged; 1099 run · **1 failure** (Serventis 1302/1302) |

**What 3.12.0 → 3.15.3 changed.** One removal from the Java API: `Receptor.NOOP` and the three
`Receptor.of(...)` factories are gone (write `_ -> { }`, or `circuit.pipe()` / `circuit.pipe(name)` for
a discarding pipe). The provider used none of them. Everything else is text:
- a `Closure` consumed once makes every later `consume` a **no-op** — no block, no error (§6.4, §16.3;
  it was an illegal temporal use). `FsClosure` already returned silently;
- `closeAwait()` from the owning circuit's context must be rejected **before** close is requested,
  leaving the resource open (§9.1). Every `closeAwait` here already checks the caller first;
- a subscription's window ends at the earliest of three close jobs — its own, its subscriber's and its
  source's — each at its own position in processing order (§7.6.1), and an empty window's `onClose`
  fires no later than the point its registration job executes;
- closing a circuit does not close the conduits, banks, tickers, subscribers or subscriptions created
  from it (§9.3 step 4); their channels deliver nothing once the circuit context has terminated;
- javadoc now states what the 3.12.0 spec already required of `diff(key)`, `edge`, `rolling` and
  `tumble` across a throwing function. Read against `FsOperators`: all four already conform.

The kit grew 1086 → 1099 with no new contract class: quiet-period liveness for `await`, `close` and
ingress (four tests), name identity under concurrent and wide extension and for enums (three), scope
close order after out-of-order departures and with repeats (two), `closeAwait` from the owning circuit
leaving a subscription open, a throwing `onClose` preserving sibling cleanup, and the two
close-from-callback position tests.

| Kit failure | What 3.15.3 says | Fix |
|---|---|---|
| `CallbackTopologyContractTest.subscriberCloseFromCallback_endsItsSubscriptionsAtItsOwnPosition` | §7.6.1 / §5.3: a subscriber's close ends each of its subscriptions at the subscriber's close **job**; transit `[1, close(subscriber), 2]` delivers `1` and not `2` | `FsSubscriber.activate` skipped the callback whenever the subscriber's `closed` flag was set — a flag written the moment `close()` is *called*, not when its job executes. The channel processing `1` rebuilt for the subscription, found the flag, registered nothing, and `1` was lost. The gate is removed: the hub roster, which the retire job edits at its own position, already bounds the window. Cold path (channel rebuild) only. |

Serventis 3.15.3 is version strings and two javadoc lines; no sign, dimension or signature moved.

---

## Close: what drains and what is dropped (§9.1, §9.3)

§9.1 requires work positioned *behind* a component's close job to be dropped, and lets a provider
choose what happens to work positioned *ahead* of it — "implementations MUST document this choice".
§9.3 asks the same of a circuit. This is that document. "Position" is a position in the circuit's
logical processing order (§7.6.1): transit ahead of ingress, each FIFO.

Two flags exist per closeable component and they are not the same thing. **Close accepted** is
written on the caller's side the moment `close()` is called; only the synchronous open-required
guards read it (`subscribe`, the circuit factories, `Bank.get`), with `await` and the ticker's scheduler task. **Close executed** is the close job
running on the worker; it alone decides what queued work takes effect. No queued job consults a
caller-side flag, with the one exception stated for the circuit.

| Resource | Its close job | Ahead of the close job | Behind the close job |
|---|---|---|---|
| **Subscription** | One job: the subscription leaves the roster, then its `onClose` runs — one position. Its window ends at whichever close job reaches it first on the worker — its own, its subscriber's or its source's — and the later ones find it ended and do nothing; no close is discarded on the caller's side. | **Drains.** Its registration job and every emission ahead of the close are processed; deliveries already selected are performed (§7.5). | Not selected by any later emission. A registration job still behind it takes no effect (empty window); `onClose` has by then already run, once (§7.6.1). |
| **Subscriber** | One job for all its subscriptions: every one still open leaves its roster, then each of their `onClose` callbacks runs, in subscription order. One job is enough because §7.2 confines a subscriber's subscriptions to one circuit. | **Drains**, as for a subscription. A subscriber callback due for an emission ahead of the close job is still invoked (§7.3). | None of its subscriptions is selected. Whatever an `onClose` raises is behind the close of all of them. `subscribe` with it is rejected synchronously from the moment close is accepted. |
| **Conduit** | One job: the roster empties, every subscription not already ended is ended — one whose registration job has not run yet included — then each of their `onClose` callbacks runs, in subscription order. | **Drains.** `[subscribe(S), emit 1, close()]` from one caller delivers `1` to `S`. | Emissions to its pipes are accepted and reach no one. A registration job behind it takes no effect. `subscribe` is rejected synchronously from the moment close is accepted. The pool keeps its channels: `get(name)` still returns the same pipe. |
| **Bank** | No job of its own: `close()` closes each materialized conduit in creation order, one close job per conduit. Two conduits of one bank therefore close at two positions, and another caller's work can land between them. | As for each conduit. | As for each conduit. `get` is rejected synchronously from the moment close is accepted. |
| **Ticker** | No job. `close()` cancels the schedule on the caller's side. | Ticks already admitted are delivered (§11.4 MAY). | See *Known gaps*: one tick whose scheduler task was already running can be admitted after `close()` returns. |
| **Circuit** | The close marker, an ingress position. `close()` admits the marker and then marks the circuit closed. | Ingress admissions ahead of the marker are each **dispatched**. Work they *raise* once close has been accepted — cascade emissions, a registered pipe's delivery, subscribe and close jobs, `onClose` — is **dropped**: the one place a caller-side flag decides a queued operation, and it is the §9.3 step 3 choice. Transit already queued when close was accepted drains. | An emission submitted once close has been accepted is dropped silently on the caller's side, with no error and no side channel (§9.3 step 1). The worker does not stop at the marker; it exits once ingress is empty. So an admission that raced the `close()` call — it read the circuit as open and was linked just behind the marker — is still dispatched. No other emission or lifecycle job behind the marker is; an `await` marker or `pulse` probe that raced the close the same way may be. Cleanup of the circuit's conduits, subscribers and subscriptions queued after this point never runs, and their `onClose` never fires (§9.1, §9.3 step 4). |

So the portable sequence is the one §9.3 gives: stop submitting, `await()`, then `close()`.

**A subscribe that races a close.** `subscribe` re-checks both closed flags and records the
subscription with its subscriber and its source as one step, under both monitors. A close of either
accepted before that step rejects the call synchronously — nothing recorded, nothing admitted, no
`onClose` (§7.2). A close accepted after it carries the subscription in its close job; if that job
runs ahead of the registration job the window is empty and `onClose` fires once, in the close job and
so ahead of the registration job (§7.6.1).

**Closed handles.** A source and a subscriber each keep their live subscriptions until a close job
ends them; the job, not the caller, drops the handle. A subscription closed after its circuit has
closed is therefore kept until its source and subscriber are themselves unreachable.

**`closeAwait`** on every one of these rejects a call from the owning circuit before close is
requested (§9.1), a subscriber with no subscriptions included.

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

`src/test/java` holds 51 tests across 13 classes. None asserts a spec requirement, and none is a
substitute for the TCK. Every class exists because a **defect was found that the TCK passes straight
through**, and each defect test is held to a rule the deleted suite was not:

> A regression test is only kept if reintroducing the defect makes it fail.

That was checked for each — the bug was put back, the test failed, the bug was removed again. A few
tests beside them are **guards** instead: they pass on the old code and exist so that a fix cannot go
too far (`WindowCascadeLeaseTest`'s pipe-hop and queued-emission cases keep the lease from faulting a
window's legitimate receiving callback). The table names which is which. A
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
| `ClosePositionTest` | 4 | §7.6.1 / §9.1: a source's and a subscriber's close are each one job at one position — `[subscribe, emit 1, close]` delivers `1`; what one `onClose` raises reaches no sibling subscription (3 defects). An empty window's `onClose` runs ahead of everything behind its registration job (1 guard: the single-caller order already held. The cross-thread cases — two routes closing one subscription, a close racing a subscribe — have no deterministic test: nothing outside the provider can hold a caller between accepting a close and admitting its job. They rest on the construction: each close admits its job under the lock that accepts it, and exactly-once is decided on the worker) |
| `FailingFunctionStateTest` | 5 | 3.12.0 §6.2.3 / §8.1: `edge`, `tumble`, `integrate` across a failing function; an equal `State` write to an older slot returns the same instance |
| `WindowCascadeLeaseTest` | 3 | §6.4.1: a window read from a later cascade faults (defect); one delivered a queued hop later, and one across its own queued emission, stays valid (2 guards) |
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

**§6.4.1 — inside one cascade, a `Window` stashed by one callback and read by another is not
detected.** The lease is the owner thread, a generation, and the worker's **cascade epoch**: it ends
when the ingress item that minted the window has finished its cascade (fixed after 3.12.0; pinned by
`WindowCascadeLeaseTest`, which failed before the fix). Everything a window may legitimately reach — its
receiving callback, including one queued hop downstream through a same-circuit pipe — runs inside that
cascade, because transit drains fully before the next ingress item (§5.3); any later ingress item is a
later callback and faults. What remains is a different callback *of the same cascade* reading a window
another callback kept. Distinguishing the two needs a token per dispatch, which is the framework hot
path §6.4.1 keeps this check off, and would fault the legitimate pipe-hop delivery.

**§11.4 — one tick can be admitted after `Ticker.close()` returns.** `close()` sets a flag and cancels
the pending schedule; a scheduler task that had already passed its flag check emits its tick anyway.
At most one, and only in that race. §11.4 says closing "stops further emissions" and allows only
ticks "already admitted … before the close" to be delivered, so this one is outside the text. Closing
it means either a close job the tick's receptor checks or a monitor around the check-and-emit; neither
has been done.

**The transit ring never shrinks.** `TransitQueue` doubles on demand and keeps its high-water mark
for the life of the circuit: one 5 000-element fan-out leaves 8 192 slots across two arrays, about
128 KB, retained. Allocation profiling reports garbage rather than retention, so nothing currently
measures it. It matters in proportion to how many circuits a process runs.

---
