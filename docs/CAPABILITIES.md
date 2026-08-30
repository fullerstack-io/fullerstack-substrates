# Capabilities, not factories

> A component should hold what it was *issued*, not what issued it.
> A pipe is owned by its circuit, and emits to its circuit.

This note records a structural finding, the single cause behind it, and what changed. It is a
companion to `DECISIONS.md` (verdicts on individual changes) — this one records the rule those
changes were measured against.

---

## 1. What the API says

```
Substrates.cortex()                → Cortex     private static final INSTANCE — C2 constant-folds it
  cortex.circuit() / circuit(Name) → Circuit
  cortex.pool(Function<Name,T>)    → Pool        the prescribed name-keyed pool
  cortex.flow() / fiber()          → unattached operators
  cortex.current()                 → Current

Circuit is the factory for every component:
  pipe() ×6 · cell · port · pin · basin · conduit · sink · bank
```

Three properties are load-bearing.

**`Cortex` has no `pipe()`.** A pipe cannot exist except relative to a circuit. `Pipe` is
`@Tenure(ANCHORED)` and its contract says: *"Lifetime managed by owning context (channel,
subscription, circuit)."* A pipe is a **capability issued by a circuit**.

**The `Circuit` is the factory, which is why nothing needs to hold it.** Every component is
constructed *by* the circuit, so the circuit can hand each one the capability it needs. Handing
itself instead is a choice, and it generated everything in §2.

**Get-or-create-by-name is a `Pool`.** `Conduit<E> extends Pool<Pipe<E>>` and
`Sink<E> extends Pool<Pipe<E>>` by declaration, and `Cortex.pool(Function)` is the root form.

---

## 2. The finding — one cause, three layers

| layer | the shared mechanism | what was in the tree |
|---|---|---|
| **storage** | a power-of-two ring | `DelayLine`, `TransitQueue`, `FsBasin`'s `ArrayDeque` |
| **submission** | `FsCircuit.submit(receiver, value)` | `Pipe.emit` + **7** hand-rolled `CircuitJob` sites |
| **lookup** | `Pool<Pipe<E>>` | `FsDerivedPool` + `FsConduit`'s map+cache + `FsSink`'s map+cache |

Each author was handed raw materials rather than a capability, so each built the mechanism again.

### The pipe was never missing — it was spelled `Consumer<Object>`

Thirteen types implement it and all pass through one chokepoint, `FsCircuit.submit`. The
uniformity existed; what was missing is that the universal role carried a JDK name, so a new
component author saw `Consumer<Object>`, inferred no house pattern, and invented one.

### Two nouns, one unnamed

An emission is `(receiver, value)` and had a name — `Pipe`. Work-to-run was `(runnable, null)` and
had none, so seven sites wrote `submit(new CircuitJob(() -> …), null)` by hand. The fix was not to
name the second noun but to notice there wasn't one: the work's varying part *is* the value.

---

## 3. The rule

A component holds what it was issued:

- **a `Pipe`** — when all it does is emit
- **a `Pool<Pipe<E>>`** — when it gets-or-creates pipes by name
- **the `FsCircuit`** — only for lifecycle authority (`await`, `isClosed`, `checkExternalCaller`,
  `requireOpen`), context identity (`current`, `onWorker`), or lazy per-name construction

### Why it is not cosmetics

It is **capability narrowing**, and the same move as `ab3b3f6` one level up. That commit made
`submitIngress` private because five classes reached past the §5.3 routing rule, which had survived
only by convention — and convention is what let it drift into a lost-emission bug. A component
holding the whole circuit *can* call `await()`, `close()`, `pulse()`. One holding a `Pipe` can queue
an emission and nothing else.

---

## 4. What changed

### Composed through the factories

| component | now holds |
|---|---|
| `FsPort` | three `circuit.pipe(…)`; `replace`/`update`/`emit` emit the value, the function, the target. No circuit field |
| `FsBasin` | `circuit.pipe(…)` feed + drain; storage on `DelayLine`. No circuit field |
| `FsConduit` | `cortex().pool(…)` + three `circuit.pipe(…)` for subscribe/unsubscribe/clear |
| `FsSink` | `cortex().pool(…)`; endpoint normalised through `circuit.pipe(endpoint)` |
| `FsSubscription` | `circuit.pipe(…)`, built only when a callback was supplied |
| `FsTicker` | `circuit.pipe(…)` — a tick is emitted into the circuit, not to the target off the scheduler |
| `FsChannel` | dead circuit field deleted |
| `FsFlow` / `FsFiber` | terminal chain step emits into the target pipe |

`FsCell` already held only its pipe and was the model. `CircuitJob` is gone — nothing hand-rolls a
carrier. One `Pool` implementation remains (`FsDerivedPool`), and `Cortex.pool(fn)` resolves to it,
so a conduit channel lookup and a `pool(flow::pipe)` derivation run the same code.

Allocation, exact: `PortOps` `replace` / `update` / `emit` went **56.00 → 24.00 B/op**, landing on
the empty-pipe baseline — nothing allocated beyond what the caller holds. `update_arg` 64.00 →
40.01.

### Three defects in `FsSink`, none TCK-covered

The channel is the one pipe this provider mints that is not an `FsPipe`: it stamps its `Capture`
before the queue hop, which is what gives §11.1 the *caller's* context for an external emission.
That difference leaked three ways.

1. **Six guards rejected it** as "not from this runtime provider" — `instanceof FsPipe` used as a
   provider test where `FsOperators.foreign()` is the correct one. A sink channel could not receive
   from a `Port`, a `Basin` drain, a fan-out list, a `Ticker`, or serve as another sink's endpoint.
2. **`Flow`/`Fiber` ran operators on the caller's thread.** `sink.pool(flow::pipe)` is the
   javadoc's own pre-capture filter, and it fell to the foreign-carrier branch — `scan` slots and
   `window` rings mutated off-worker, against the documented Threading Guarantee.
3. **A cross-circuit endpoint bypassed the sink's own circuit.** The endpoint was held raw, so the
   capture went straight to the far circuit from the calling thread. `circuit.pipe(endpoint)` is
   what the spec calls "the usual same-circuit pipe optimization".

A fourth, in `FsTicker`: a tick emitted to its target from the scheduler thread, so every capture
was stamped with the scheduler rather than the owning circuit — which `Capture#current()` names
explicitly ("not the ticker's scheduling thread").

### Naming

The queues now carry the spec's §5.3 vocabulary: `IngressQueue`, `TransitQueue`, and `Admission`
for the carrier. `TransitJobQueue` was deleted — never wired, tied on time, and beaten on
allocation by the ring.

---

## 5. Constraints — measured, and they fence any further work

1. **Never collapse the dispatch.** `ReceptorAdapter` / `AwaitMarker` / `CloseMarker` /
   `PulseProbe` are separate concrete classes so the drain's `r.accept(v)` stays monomorphic.
   Collapsing them cost a class_check trap: `async_emit_batch_await` ~22 ns → 30+.
2. **A shared ring cannot serve the window read.** `FsWindow.at` derives `mask = buffer.length - 1`
   at the use site so C2 can prove the index in range; that is why `DelayLine.buffer()` hands out
   the raw array. Pushing the derived-mask rule *into* `DelayLine`/`TransitQueue` was tested and
   buys nothing — the guard changes shape rather than disappearing.
3. **A sink channel cannot be a plain queued pipe.** TCK-enforced:
   `SinkContractTest.current_externalEmission_identifiesCallerContext` and
   `current_crossCircuitEndpoint_preservesIngressCallerContext` both fail if the channel queues
   before minting.
4. **`cortex()` is free but is not a substitute for the circuit.** A `private static final`
   singleton that C2 constant-folds — but it cannot report *whose* worker you are on.

---

## 6. Verification

The Substrates TCK passes unchanged **before and after every defect above**, so it covers none of
them. `src/test/java` carries the regression tests that do:

| suite | covers |
|---|---|
| `SinkChannelTest` | the six provider guards, cross-circuit routing, and every usage pattern in the `Sink`/`Capture` javadoc |
| `FlowFiberAttachmentTest` | the Threading Guarantee across six target shapes, Flow and Fiber |
| `CaptureProvenanceTest` | all three `Capture#current()` cases, including the ticker |
| `TransitCapacityTest` | ring growth *during* a drain, and cascade order across a doubling |

Each was checked by reintroducing the defect it guards and confirming it fails.

**Still open:** the transit ring never shrinks — one wide fan-out sets a permanent high-water mark
(5000 elements ⇒ 8192 slots × two arrays ≈ 128 KB per circuit). Allocation profiling cannot see
retention, so nothing currently measures it.
