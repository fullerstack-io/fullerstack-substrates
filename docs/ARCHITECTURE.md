# Fullerstack Substrates — Implementation Architecture

This document describes how Fullerstack implements the [Substrates Specification](https://github.com/humainary-io/substrates-api-spec) (Humainary 3.12.0). For what Substrates is and why it works this way, read the official [Specification](https://github.com/humainary-io/substrates-api-spec/blob/main/SPEC.md) and [Rationale](https://github.com/humainary-io/substrates-api-spec/blob/main/RATIONALE.md). Designs that were built, measured and rejected are recorded in [Decisions](DECISIONS.md).

---

## Implementation Decisions

The spec is language-independent. These are our Java 26 projection choices:

| Spec Concept | Our Implementation | Why |
|---|---|---|
| Execution context | Virtual thread (one per circuit) | Lightweight, no platform thread exhaustion |
| Ingress queue | Custom `IngressQueue` (wait-free MPSC, intrusive-linked `Admission` per emission) | ~13ns emit, no CAS retry loop on producers |
| Transit queue | Custom `TransitQueue` (single-threaded power-of-2 ring) | Zero indirection on cascade hot path, automatic growth |
| Per-emission operators | `FsFiber` (immutable, reusable, 41 operator overloads) | Spec §6 — Fiber is the per-emission processing recipe |
| Memory ordering | `VarHandle` release/acquire where a field crosses threads on the hot path (`Admission.next`, `WindowLease.generation`) | A `volatile` declaration would force the stronger access on every plain store; the worker's `parked` flag stays `volatile` because it is one half of a Dekker handshake |
| Context check | `FsCircuit.onWorker()` — `Thread.currentThread() == worker` | The same predicate as §11.3's `cortex.current() == circuit.current()`, at one field load instead of a ThreadLocal walk |
| False sharing | Padding hierarchy of `int` fields around `head`/`tail` | `@Contended` is inert without `-XX:-RestrictContended`; measured 20.5ns unpadded vs 13.5ns padded |
| Name interning | `ConcurrentHashMap` with hierarchical parent links | O(1) identity comparison via reference equality |
| Subject identity | `AtomicLong` counter (~5ns vs UUID's ~300ns) | Simple, fast, no collision risk in single JVM |
| Slot storage | Immutable `record` (Name + value + type) | Compact, no synchronization needed |
| Resource lifecycle | `Scope` with one creation-ordered member list (resources and child scopes together), closed newest-first, iteratively | §9.2 / `Scope.close()` ordering; no stack overflow on a deep scope tree |

## Class Map

35 source files in `io.fullerstack.substrates`:

```
FsCortexProvider (SPI entry point)
  └── FsCortex (entry point — creates circuits, scopes, names, states, slots, flows, fibers, pools;
        │        binds each worker's Current)
        └── FsCircuit (dual-queue sequential execution engine)
              ├── IngressQueue (wait-free MPSC — admissions from any other thread)
              │     └── Admission (one per emission; carries receiver, value, marker flag, next)
              ├── TransitQueue (single-threaded power-of-2 ring; cascade FIFO)
              ├── FsConduit (channel factory + subscription management)
              │     ├── FsHub (subscription roster + version counter)
              │     ├── FsChannel (per-name dispatch — version-checked on every delivery, §7.6.2)
              │     │     └── FsPipe (async emission carrier — emit only)
              │     └── FsDerivedPool (the only Pool implementation: channel storage via
              │                        cortex.pool(...), and the pool(Function) / pool(Flow) /
              │                        pool(Fiber) views)
              ├── FsBank (closeable name-indexed conduit factory)
              ├── FsCell (circuit-owned single-slot state; receptor-pipe + volatile)
              ├── FsPin (circuit-owned, owner-context-guarded state handle;
              │          immediate get/set on the worker thread, ISE elsewhere)
              ├── FsPort (circuit-owned queued mutation handle without read; holds three
              │           circuit-issued pipes, so replace/update/emit emit the value, the
              │           function and the target rather than wrapping each in a carrier)
              ├── FsFlow (type-changing composition: map / relate / scan / window(int) /
              │           window(Duration, int) / run / change / flow / fiber / pipe —
              │           held as an immutable Recipe)
              ├── FsFiber (per-emission operators: 41 overloads over 36 names, plus two `pipe` forms — guard, diff,
              │           limit, peek, replace, ..., chance, change, deadband, delay, edge,
              │           every, heartbeat, hysteresis, inhibit, pulse, rolling, steady, tumble,
              │           distinct, route, streak, tee, when)
              ├── FsOperators (the per-emission operator classes FsFiber composes, plus the §5.8
              │                stimulus holder and the provider-mismatch check)
              ├── FsSubscriber (emission observer with lazy callback)
              │     └── FsSubscription (subscription lifecycle handle)
              │           └── FsRegistrar (Consumer<Object> registration during callback: a receptor
              │                 inline, a pipe as an Admit submitted to its own circuit — §5.3;
              │                 rejects use from any thread but the issuing worker — §6.4)
              ├── FsTicker (circuit-owned periodic emitter; grid-anchored fixed-rate schedule,
              │             gap-free Long sequence, bounded catch-up — a stall of more than one
              │             interval re-anchors the grid at the current tick (anchorSeq), a
              │             one-time phase shift; backed by a lazy single-thread daemon
              │             ScheduledExecutorService shared across all tickers on the circuit,
              │             shut down on circuit close; each tick reaches the target through a
              │             circuit-owned pipe, so from the worker)
              ├── FsWindow (strided view over a DelayLine ring; restriction ops share buffer)
              │     └── WindowLease (§6.4.1 (context, generation) lease; owns the ring reference)
              ├── FsSink (capture-producing pool of channels; endpoint normalised through
              │           circuit.pipe(...) so a capture lands on this circuit before crossing)
              └── FsBasin (bounded buffered emission capture, DelayLine-backed)

DelayLine (worker-confined power-of-2 ring behind delay / rolling / window / basin)
Recipe (immutable operator recipe shared by FsFiber and FsFlow)

FsName (hierarchical dot-notation names with interning)
FsSubject (identity: Id + Name + State + Type)
FsState (slot-based state container)
  └── FsSlot (typed name-value pair)
FsScope (structured resource lifecycle)
  └── FsClosure (block-scoped resource management)
FsCurrent (circuit execution context)
```

There is no shared abstract base for Substrate implementations. Most build their `Subject` eagerly in a `final` field in the constructor; `FsPipe` and `FsSubscription` derive theirs on first `subject()` call, under the instance monitor, so an attachment or subscription nobody asks about never mints an `FsSubject` and its id.

---

## Queue Architecture

The performance-critical core. Spec requirements:

- **One execution context per circuit** — all processing on a single virtual thread
- **Deterministic ordering** — emissions processed in strict enqueue order
- **Dual-queue model** — ingress (external) + transit (cascading) with transit priority
- **No call stack recursion** — cascading emissions enqueue, never invoke nested calls
- **Stack safe** — even deeply cascading chains don't overflow the stack
- **Wait-free producer** — callers never block when emitting

The routing decision between the two queues has one definition, `FsCircuit.submit`: a call on this circuit's worker enqueues to transit, any other thread (another circuit's worker included) admits to ingress, and an emission after close is dropped. `FsPipe.emit`, every Flow/Fiber terminal and every registered pipe's `Admit` reach the queues through it.

### Admission — the ingress carrier

`IngressQueue` mints one `Admission` per emission, carrying its own `next` link:

```java
final class Admission {
  Consumer<Object> receiver;   // the receptor this emission delivers to
  Object           value;      // the emission; for an await marker, the awaiter's barrier
  boolean          marker;     // await/close barrier rather than user work
  Admission        next;       // NOT volatile — ordering comes from the access mode
}
```

**Why one node per emission, and not a batched chunk?** A chunked alternative — one node holding
128 entries in an interleaved `Object[]` — was built and rejected. It allocated a third as much,
but receivers had to be read back out of an untyped array, making every dispatch an interface
cast; perfasm found that secondary supertype check to be the hottest instruction in the drain. A
typed `Admission` field is a *class*, so there is no check and dispatch is `invokevirtual`.

**Why nothing is recycled here.** A carrier that crosses the thread boundary cannot be: a producer
can claim a node the consumer has already freed, which is an ABA that lost emissions and left
`await` parked forever. Worker-confined carriers can be recycled — `TransitQueue` holds its
`(receiver, value)` pairs inline in two parallel arrays and mints no node at all.

**Why `next` is not `volatile`.** Ordering comes from the access mode at the point of use, not the
declaration: the producer publishes with `NEXT.setRelease` and the consumer reads with
`NEXT.getAcquire`. A modifier would force a StoreLoad fence on every plain store — measured at
40.89% of one profile against 8.22% once removed.

### IngressQueue — wait-free MPSC

Producers publish in two steps: one atomic exchange of the head, then the release store that
commits it.

```java
void enqueue ( Consumer<Object> receiver, Object value, boolean marker ) {
  final Admission admission = new Admission ();
  admission.receiver = receiver;
  admission.value    = value;
  admission.marker   = marker;
  final Admission prev = (Admission) HEAD.getAndSet ( this, admission );  // wait-free
  NEXT.setRelease ( prev, admission );                                     // commit
}
```

**Key properties:**
- `getAndSet` always succeeds — no CAS retry loop, genuinely wait-free
- Fields are written before the exchange, which is a full fence; the consumer reaches the node
  only through an acquire load of `next`, so the release carries them across
- FIFO across producers, by exchange order
- The consumer walks from a stub, so `tail` always points at an already-consumed node and the
  node after it is next to run

**Two reads, for two different questions.** `peek()` is `NEXT.getAcquire(tail)` — non-null when
something is committed, used by the worker's spin. `quiescent()` additionally requires
`head == tail`, and is read only when the worker is about to park.

The difference is load-bearing. A producer publishes in two steps and only the second is visible
to `peek`; between them the queue *looks* empty, and the producer's read of the worker's `parked`
flag can be reordered ahead of its link on any store-buffered machine. Both sides could then
conclude "nothing to do" about the same emission — the worker parks and the producer does not wake
it. The head closes that: its write is the exchange, a locked read-modify-write and globally
ordered, so if a producer's flag read preceded the worker's flag write then that producer's
exchange did too, and the worker sees `head != tail` and does not park.

**Consumer drain** (`drainBatch`) runs every committed admission, interleaving transit after each:

```java
Admission t = tail;
Admission j = (Admission) NEXT.getAcquire ( t );
if ( j == null ) return false;          // nothing committed

do {
  t.next = null;                        // unlink the consumed node for GC
  t = j;
  circuit.stimulus.valid = false;       // §5.8: one clock reading per ingress item
  run ( j, circuit );                   // dispatch, then drain transit to empty
  j = (Admission) NEXT.getAcquire ( t );
} while ( j != null );
tail = t;
```

`run` sends a marker straight to its receiver; any other admission is dispatched inside the §15.4
isolation guard and then, if transit has work, transit is drained. Causal completion (§5.3): each
admission's transit cascade is drained to empty before the next ingress admission runs.

### TransitQueue — single-threaded ring

Cascading emissions (raised on the worker, from within callbacks) go to the transit ring. No atomics — only the circuit thread accesses it. Two parallel arrays addressed by `head & mask` / `tail & mask`:

```java
void enqueue(Consumer<Object> receiver, Object value) {
  int i = tail & mask;
  receivers[i] = receiver;
  values[i] = value;
  tail++;
  if (tail - head > mask) grow();          // double on overflow
}

boolean drain() {
  if (head == tail) return false;
  do {
    int i = head & mask;
    Consumer<Object> r = (Consumer<Object>) receivers[i];
    Object v = values[i];
    receivers[i] = null;
    values[i] = null;
    head++;
    try { r.accept(v); } catch (Throwable ignored) { }   // §15.4 isolation
  } while (head != tail);
  // Reset cursors back to home position — single-threaded, no synchronization.
  head = 0;
  tail = 0;
  return true;
}
```

**Key properties:**
- No chunk-advance check, no ring-reset branch, no linked-list `next` maintenance — just `& mask` and indexed array access
- Pre-allocated initial capacity (8); grows by doubling with a one-shot copy when needed
- Cursors reset to 0 after each drain so the ring stays at home position with no fragmentation
- Read cursor chases write cursor — cascades within cascades resolve in a single drain call
- FIFO, so a cascade resolves breadth-first: siblings run before grandchildren (`A` emits `A1`, `A2`; `A1` emits `A1a` ⇒ `A1`, `A2`, `A1a`)
- Transit drains with **priority** over ingress — all cascading effects complete before the next external emission

Transit priority ensures **causal completion** — all cascading effects of an emission resolve before the next external emission is processed. This eliminates race conditions without locks.

**Single-entry fast path:** when a fiber/flow chain produces exactly one downstream emission per input (the common case for `guard`, `map`, `peek`), the drain loop runs a single iteration. The reset-to-home write at the end is unconditional and very cheap.

### Worker loop

The circuit thread runs a spin-then-park loop:

```java
private void drainLoop() {
  final IngressQueue q = ingress;

  for (;;) {
    if (q.drainBatch(this)) continue;      // drain ingress + interleaved transit
    if (shouldExit) return;

    // Spin first: a circuit fed faster than the spin lasts never parks at all
    Object found = null;
    for (int i = 0; i < SPIN_COUNT && found == null; i++) {
      Thread.onSpinWait();
      found = q.peek();
    }
    if (found != null) continue;

    parked = true;                         // published before the re-check
    VarHandle.fullFence();

    // quiescent() = nothing linked AND no producer holding an unlinked slot
    while (q.quiescent() && !shouldExit) LockSupport.parkNanos(PARK_NANOS);

    parked = false;
  }
}

// on every admission from a thread that is not the worker
private void submitIngress(Consumer<Object> receiver, Object value) {
  ingress.enqueue(receiver, value, false);
  if (parked) LockSupport.unpark(worker);
}
```

**Park until told:** the worker spins, then publishes `parked` and parks; a producer that admits work reads that flag after its exchange and unparks the worker. The pair is a Dekker handshake, so no admission can be left unnoticed, and the timeout is only a safety net for a wake that was somehow missed. An idle circuit therefore consumes nothing, and a circuit fed faster than the spin lasts never parks, so its producers never pay an unpark. The producer pays one volatile read and a not-taken branch (`closed` and `parked` are declared side by side so they share a line). Why a self-waking timed park was rejected: see [Decisions — Worker idle policy](DECISIONS.md#worker-idle-policy).

**Callback isolation (spec §15.4):** `IngressQueue.run` (for every non-marker admission) and `TransitQueue.drain` each wrap their dispatch in a `try { … } catch (Throwable ignored) { }` so an uncaught client-callback exception cannot terminate the worker. `FsChannel`'s multi-consumer dispatch lambda wraps each sibling receptor invocation the same way, and its hierarchical (STEM) walk guards each level, so a throwing receptor doesn't cost siblings or ancestors their delivery of the emission (§15.4 #2). The subscriber callback in `FsChannel.rebuild` is guarded too: on a throw it keeps whatever the callback registered before failing and still records an entry for that subscription/channel pair, so the callback is never retried for it (§15.4 #3).

### Marker class split — JIT monomorphism

User receptors flow through a single concrete class so the hot-path `accept` site stays monomorphic:

```java
static final class ReceptorAdapter<E> implements Consumer<Object>, Receptor<E> {
  final Receptor<? super E> receptor;
  @SuppressWarnings("unchecked")
  public void accept(Object o) { receptor.receive((E) o); }
  public void receive(E emission) { receptor.receive(emission); }
}
```

Markers and the pulse probe go through their own concrete classes — distinct types from `ReceptorAdapter`:

```java
static final class AwaitMarker  implements Consumer<Object> { /* await marker */ }
static final class CloseMarker  implements Consumer<Object> { /* close marker */ }
static final class PulseProbe   implements Consumer<Object> { /* §5.7 round-trip probe */ }
```

The await and close markers are admitted with `Admission.marker = true`, and `IngressQueue.run` branches on that flag before the hot call site: a marker is dispatched from its own `accept` call, skipping the isolation guard and the transit drain, so marker types never enter the hot site's type profile. A marker is a field test rather than a subclass, because a virtual `Admission.run` per kind was measured failing to inline into the drain. The pulse probe is admitted as ordinary work (`marker = false`) and so does pass through the hot site; it is a single-use diagnostic, not a steady-state type.

The circuit's own queued work — `FsConduit`'s subscribe/unsubscribe/clear, `FsPort`, `FsBasin`, `FsSubscription`'s close callback — carries no job class at all: each emits the value that varies into a pipe the circuit issued it through `Circuit.pipe(Receptor)`, so it reaches the queue as a `ReceptorAdapter`. See [Decisions — Capabilities over factories](DECISIONS.md#capabilities-over-factories--2026-08-30).

**Why all this?** Multiple lambda classes flowing through a single virtual call would cause bimorphic or megamorphic dispatch and class_check traps. Splitting by purpose keeps each call site monomorphic — C2 can devirtualise and inline.

---

## Async Model — Why Everything Is Async

Substrates is async-first — `pipe.emit(value)` enqueues and returns immediately. Processing happens later on the circuit's virtual thread. This is the opposite of RxJava (synchronous by default).

Spec §14 (Circuit-Dispatched Pipe Dispatch) formalises this for cyclic topologies via `circuit.pipe(target)` — a pipe that dispatches through the circuit's queue rather than calling the target directly. This is what makes deeply recurrent networks stack-safe. A target that already belongs to the same circuit is returned as-is, since its own `emit` already routes through the queue; a target on another circuit is wrapped in a receptor pipe that forwards to it.

### RxJava vs Substrates

**RxJava (synchronous by default):**

```java
BehaviorSubject<String> subject = BehaviorSubject.create();
AtomicReference<String> received = new AtomicReference<>();
subject.subscribe(value -> received.set(value));

subject.onNext("hello");  // BLOCKS until callback completes
assertEquals("hello", received.get());  // Works immediately
```

**Substrates (asynchronous by default):**

```java
var circuit = cortex().circuit(cortex().name("test"));
var conduit = circuit.conduit(cortex().name("test"), String.class);

AtomicReference<String> received = new AtomicReference<>();
conduit.subscribe(circuit.subscriber(
    cortex().name("sub"),
    (subject, registrar) -> registrar.register(received::set)
));

conduit.get(cortex().name("ch")).emit("hello");

assertNull(received.get());   // Still null — async hasn't run
circuit.await();               // Block until queue drained
assertEquals("hello", received.get());  // Now available
```

**The difference**: `emit()` posts to the ingress queue and returns in ~13ns. The callback runs later on the circuit's virtual thread. If you assert before `await()`, you get null.

### Testing with `circuit.await()`

The most important pattern in Substrates testing.

**Correct pattern:**

```java
@Test
void testEmission () {
    var circuit = cortex().circuit(cortex().name("test"));
    var conduit = circuit.conduit(cortex().name("c"), String.class);

    AtomicReference<String> received = new AtomicReference<>();
    conduit.subscribe(circuit.subscriber(
        cortex().name("sub"),
        (subject, registrar) -> registrar.register(received::set)
    ));

    conduit.get(cortex().name("ch")).emit("hello");

    circuit.await();  // Wait for all pending emissions to process
    assertEquals("hello", received.get());

    circuit.close();
}
```

**Wrong: latches for queue sync:**

```java
// WRONG — race condition between emit() and latch.await()
CountDownLatch latch = new CountDownLatch(1);
registrar.register(value -> {
    received.set(value);
    latch.countDown();
});
pipe.emit("hello");
assertTrue(latch.await(2, TimeUnit.SECONDS));  // May timeout
```

Latches work for thread coordination (starting N threads at once). They don't work for async queue synchronization. Use `circuit.await()`.

**When latches *are* appropriate** — coordinating producers, then awaiting:

```java
int threads = 10;
CountDownLatch startLatch = new CountDownLatch(1);
CountDownLatch doneLatch  = new CountDownLatch(threads);

for (int i = 0; i < threads; i++) {
    Thread.startVirtualThread(() -> {
        try {
            startLatch.await();
            pipe.emit("value");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            doneLatch.countDown();
        }
    });
}

startLatch.countDown();
doneLatch.await(5, TimeUnit.SECONDS);
circuit.await();  // THEN drain the queue
```

### Anti-patterns

**Don't await after every emit:**

```java
// WRONG — defeats async design
for (int i = 0; i < 1000; i++) {
    pipe.emit(i);
    circuit.await();  // Serializes everything, 1000x overhead
}

// CORRECT — batch emit, await once
for (int i = 0; i < 1000; i++) {
    pipe.emit(i);
}
circuit.await();
```

**Don't assert before await:**

```java
// WRONG
pipe.emit("hello");
assertEquals("hello", received.get());  // NULL

// CORRECT
pipe.emit("hello");
circuit.await();
assertEquals("hello", received.get());
```

### Cross-circuit synchronization

`await()` is local to one circuit. When signals cross circuit boundaries (e.g., a pipe created by `circuit2` registered on a conduit of `circuit1`, whose deliveries become ingress on `circuit2`), you need to await both, upstream first:

```java
var circuit1 = cortex().circuit(cortex().name("source"));
var circuit2 = cortex().circuit(cortex().name("target"));

// ... register a circuit2 pipe on a circuit1 conduit ...

pipe.emit("value");
circuit1.await();  // Drain source (delivery admitted to circuit2)
circuit2.await();  // Drain target (delivery processed)
```

For deep chains (3+ circuits) awaiting each circuit in upstream order suffices; a topology with cycles between circuits may need multiple rounds:

```java
for (int round = 0; round < 3; round++) {
    circuit1.await();
    circuit2.await();
    circuit3.await();
}
```

### How `await()` works

`await()` admits a marker into the ingress queue at the caller's own position and waits for the worker to reach it. Each awaiter admits its own marker carrying its own barrier, so concurrent awaiters never share one (§5.5: the barrier covers every operation accepted before *that* call). When the circuit thread processes the marker (after all preceding admissions and their transit cascades), it releases that barrier. This guarantees all emissions submitted before `await()` have been fully processed, including any cascading transit emissions they triggered.

The thread-identity check that prevents worker-thread callers from deadlocking is centralised in `checkExternalCaller(op)` and reused by `await()`, `pulse()`, and every `closeAwait()` (circuit, conduit, bank, ticker, subscriber, subscription). `closeAwait()` calls `checkExternalCaller` *before* any close side-effect, so an illegal call fails fast with no partial state change (spec §16.1 #13).

```java
public void await() {
  checkExternalCaller("await");
  if (closed) { awaitClosed(); return; }   // joins the worker thread
  awaitImpl();
}

void checkExternalCaller(String op) {
  if (onWorker())
    throw new IllegalStateException("Cannot call Circuit::" + op + " from within a circuit's thread");
}

private void awaitImpl() {
  final AwaitBarrier barrier = AWAIT_BARRIER.get();   // per-thread, reused across calls
  barrier.released = false;                           // volatile flag

  ingress.enqueue(awaitMarkerReceiver, barrier, true);  // this caller's own position
  LockSupport.unpark(worker);

  for (int i = 0; i < AWAIT_SPIN_COUNT; i++) {        // the marker usually fires within the spin
    if (barrier.released) return;
    Thread.onSpinWait();
  }
  while (!barrier.released) {                         // slow path: bounded parks
    if (closed && !worker.isAlive()) return;          // a marker behind close never fires (§9.1)
    LockSupport.parkNanos(AWAIT_PARK_NANOS);
  }
}

// Marker callback — runs on circuit thread, releases this marker's awaiter
private void onAwaitMarker(Object barrier) {
  final AwaitBarrier b = (AwaitBarrier) barrier;
  b.released = true;
  LockSupport.unpark(b.thread);
}
```

FIFO ordering guarantees all prior emissions complete before the marker executes.

### Summary

| | RxJava | Substrates |
|---|---|---|
| `emit()` | Blocks until callback completes | Returns immediately (~13ns) |
| Callbacks | Execute on calling thread | Execute on circuit virtual thread |
| Testing | Assert directly after emit | Must `circuit.await()` first |
| Ordering | Depends on scheduler | Deterministic FIFO; each cascade completes before the next ingress item |
| Concurrency | Locks needed | Lock-free (single-threaded) |

---

## Context, Time and Temporal Validity

Three mechanisms answer questions an operator or a caller cannot answer locally. They are grouped
here because they share one root cause: **an operator reaches `FsOperators.Wrap.wrap(Consumer)`
with no reference to the circuit driving it.** The chain it builds is nested `Consumer.accept(E)`
calls, and `accept` takes one parameter — the value.

### Which context am I? (§11.3)

§11.3 names `cortex.current() == circuit.current()` as the specified way to ask, and §5.7 requires
this circuit's `Current` to **be** its worker's Current — otherwise that comparison would always
answer "not on the circuit". The worker binds it as the first act of `workerLoop`
(`cortex.bindCurrent`), so the comparison holds for any caller who uses it.

The provider asks the same predicate in the form §11.3 names as Java's mechanism:
`FsCircuit.onWorker()`, which is `Thread.currentThread() == worker`. Because the worker's Current
is bound to that thread and nothing else ever binds it, the two compare equal exactly when the
threads do. Its call sites are `FsCircuit.submit` (the §5.3 ingress-versus-transit routing every
emission takes), `checkExternalCaller`, and `FsPin`'s §11.6 owner guard. `FsRegistrar` and
`WindowLease` hold the issuing worker `Thread` themselves and compare against it.

The literal form is not used on these paths because it is six dependent loads through a
ThreadLocal, measured at 16.9% of the emission profile; the thread form is one field load. On
`Pin.set` the change took the guard from ~5.9 ns to a plain field store. See
[Decisions — One routing decision, one context check](DECISIONS.md#one-routing-decision-one-context-check).
The `Current` is still consulted where its cost does not matter: on cold fault paths, so an error
names the offending context, and in `FsSink`'s §11.1 capture attribution.

`Current` is held in a `ThreadLocal` in `FsCortex`. A ThreadLocal entry dies with its thread,
which is the lifetime §11.3 asks interning to last for; a map keyed by thread id would never evict,
and virtual threads make thread ids unbounded.

### What time is it? (§5.8)

§5.8: *"A single ingress item and the entire transit cascade it triggers share one processing-time
reading … it does **not** advance across the internal transit hops of one cascade."* A whole
cascade is one causal moment, so a transit hop that blocks for 25 ms must be invisible to a 5 ms
`window(Duration, capacity)`.

`FsCircuit.stimulus` is a two-field holder — value and validity — invalidated by a plain boolean
store once per ingress item in `IngressQueue.drainBatch`, **before dispatch and deliberately
not inside `drainTransit()`**. The reading is established by the first time-aware operator that
asks (`FsOperators.stimulus()`), which is §5.8's own *"lazily established"* licence: an
implementation need not read the clock for ingress items no time-aware operator observes.

The three readers — `FsFlow`'s `DurationWindow` (behind `window(Duration, int)`),
`FsOperators.Heartbeat` and `FsOperators.EveryTime` — reach the holder through the
`FsOperators.STIMULUS` `ThreadLocal`, which the worker binds once at thread start. **That is the
weak part of this design**: where no binding exists the accessor falls back to a per-call clock
read rather than failing, so the non-conformance cannot announce itself. See
[Conformance](CONFORMANCE.md).

### Is this window still mine? (§6.4.1)

`Flow.window` allocates **one** buffer per materialisation and rewrites it in place on every
emission; the `Window` handed downstream is a strided view over that live storage, not a snapshot.
That is what the spec sanctions — §6.2.3 puts the obligation on the caller (*"MUST NOT be retained
… callers that need values beyond the callback MUST copy them"*) and §6.4.1 then requires the
implementation to **detect** violations: *"undefined behavior is not an acceptable choice for this
type."*

`WindowLease` is that detector, and it is a **(context, generation)** pair, shared by a root
window and every view derived from it. Both halves are load-bearing:

- the **context** (the minting `Thread`) catches a window that left the worker — removing it fails
  `window_afterCallbackReturn_throwsFault` and `window_allOperationsAfterCallbackReturn_throwFault`,
  measured;
- the **generation** catches a window retained into a later callback **on the same worker**, which
  a bare context check cannot see because the worker is still the worker.

That second case is not a stale read. The buffer has been rewritten, so the retained view reports
whatever is in it *now* while presenting itself as the window from an earlier callback — a silent
wrong answer, which is the worst reading of §6.4.1.

The generation is published with a release store and read with an acquire load rather than being
`volatile`: `latch()` runs on the emission path, and on x86 a release store is a plain store while
a volatile store is a locked instruction.

Closing the lease around the operator call instead — latch, `accept`, release — does not work: a
transit hop queues the window and the receptor runs after `accept` has returned, so release would
fault every delivery the hop legitimately makes.

## Name Interning

`FsName` interns every name by its full path in one `ConcurrentHashMap`, and each name holds its parent reference and its direct children. Two names with the same path are the same object (reference equality). This makes name comparison O(1) — pool lookups such as `conduit.get(name)` and slot matching compare names by identity.

```
cortex.name("kafka.broker.1")
  → FsName["kafka"]
       └── FsName["broker"] (parent = kafka)
            └── FsName["1"] (parent = broker)
```

Subsequent calls to `cortex.name("kafka.broker.1")` return the same `FsName` instance.

## Flow and Fiber

Per-emission processing lives on `Fiber<E>`; `Flow<I,O>` carries the type-changing surface. Both hold their operators as an immutable `Recipe` — a newest-first tree of operator factories, so composition is O(1) and shares everything composed before it. A per-attachment factory (`Flow.fiber(Function<Subject<?>, Fiber<O>>)`, `Flow.flow(Function<...>)`) is a `Recipe.Deferred` node, resolved once per `pipe(target)` call against the target's subject.

### `FsFiber` — per-emission operators

`FsFiber` is an immutable, reusable composition of operators (41 overloads over 36 names, plus two `pipe` forms) that act on emissions of a single type. Each operator method returns a new fiber with the operator appended; the fiber value is reusable and may be materialised against multiple pipes, with each materialisation producing independent state. The operator classes live in `FsOperators`.

- **diff()** — suppress unchanged values (Shannon's principle: only changes carry information)
- **guard(predicate)** — filter by predicate, with optional stateful bi-predicate
- **limit(n)** / **skip(n)** — count-bounded pass / drop
- **peek(receptor)** — side-effect without consuming
- **reduce(initial, op)** — running aggregation
- **integrate(initial, accumulator, fire)** — accumulate until `fire` matches, emit and reset to `initial`
- **relate(initial, op)** — project each `(prev, curr)` pair; `prev` advances to the incoming value *before* `op` runs, so an `op` that throws drops that emission and the next pairs with this input (§6.2.3)
- **replace(op)** — value transformation
- **takeWhile / dropWhile** — predicate-based windowing
- **above / below / clamp / deadband / range / max / min / high / low** — comparator-based sift
- **chance(p)** — passes each emission with probability `p` from a per-operator SplitMix64 state, seeded from `-Dio.fullerstack.substrates.seed` mixed with the operator's construction ordinal; unset, a seed is drawn once per JVM and published back under that property so a run can be replayed (§5.1)
- **steady(count)** / **steady(count, predicate)** — emit once a run reaches `count`; the predicate form compares each value against the run's candidate (the value that started the run), not against the value before it, so a gradual drift still resets once it leaves the candidate
- **change, delay, edge, every(n), every(Duration), heartbeat, hysteresis, inhibit, pulse, rolling, tumble**
- **distinct()** — unbounded duplicate suppression via `HashSet`
- **distinct(capacity)** — FIFO-windowed duplicate suppression via `LinkedHashSet`; suppressed duplicates do not refresh position
- **route(predicate, pipe)** — predicate-matched values diverted to a side pipe, non-matching pass through (demux)
- **streak(required, matches)** — emit Nth consecutive match, then re-arm; non-match resets counter. `required == 1` short-circuits to `guard(matches)` (no carried state)
- **tee(pipe)** — fan-out: side-pipe receives, value continues downstream
- **when(predicate, fiber)** — matching values traverse a pre-materialised sub-fiber chain that terminates at the same downstream; non-matching pass through unchanged. Empty sub-fiber → stage is identity (returned as-is)

### `FsFlow` — type transformation

`FsFlow<I,O>` provides the type-changing surface: `map`, `relate`, `scan`, `window(int)`, `window(Duration, int)`, `run`, `change`, `flow`, `fiber`, `pipe`. `flow.fiber(fiber)` attaches a fiber at the output side; `flow.pipe(target)` materialises the chain into a new pipe whose terminal submits the value to the target's circuit with the target's own receiver — `target.emit(v)` in substance — so a conduit channel runs its dispatch, and the §7.6.2 version check, at the position where it processes the emission (§7.6.1). `FsFiber.pipe` terminates the same way. A terminal that captured the channel's dispatch consumer instead would miss any subscribe or close raised earlier in the same cascade.

Both flow and fiber state are safe without synchronization because all processing runs on the circuit's single thread.

## Lazy Rebuild — one dispatch path, version-checked at the position of delivery

When a subscription is added or removed on a conduit, the change is not applied to any channel immediately. The registration or close job increments the conduit's hub version counter at its own position in the circuit's processing order — behind the cascade if it was submitted from a caller context, inside it if it was raised from a callback (§5.3, §7.6.1). On its next delivery each channel compares its `builtVersion` with the hub's (one int compare in `FsChannel.receive`) and rebuilds if they differ.

Each named pipe in a conduit is fronted by an `FsChannel`, which is the receiver every emission addressed to that name is submitted with — from `conduit.get(name)`'s pipe, from a fiber/flow terminal aimed at that pipe, from a registered pipe's `Admit`. After rebuild the channel holds one pre-built `Consumer<Object>`, **`dispatch`** — this channel's registrations only, no STEM walk — used by `receive()` after the version check and by `dispatchStem` when walking ancestors (so an ancestor's STEM walk is not retriggered; each ancestor is version-checked in that walk, §10.2).

Rebuild runs the subscriber callback once per subscription/channel pair with a fresh `FsRegistrar`. A registered receptor is stored to be invoked inline during the dispatch walk; a registered pipe is stored as an `Admit` that submits each delivery to the pipe's own circuit through `FsCircuit.submit` — transit behind work the cascade has already accepted when that circuit is this one, ingress on the owner's queue when it is not (§5.3, §6.3). The registrar records the issuing worker thread at construction and throws `IllegalStateException` if `register` is called from any other thread, even while the callback is still running, or after the callback returned (§6.4). The memo is keyed by subscription, not subscriber, so a close-then-resubscribe is a fresh pair whose callback runs again.

There is deliberately no second, check-free consumer for the transit side. §7.6.1 selects recipients as "exactly those subscriptions effective at that position" — the position at which the channel processes the emission — and a subscribe or close raised inside a cascade takes effect within that cascade, so a topology change can sit in transit ahead of a delivery. The check has to run where the emission is processed, which is what submitting the channel itself does.

This avoids locking during subscription changes — the spec's "eventual consistency" model (§7.6.2): a subscription registered between two emissions sees the second and not the first, whichever context registered it.

## Thread Safety Summary

| Component | Thread Safety | Why |
|---|---|---|
| `pipe.emit()` | Thread-safe (any thread) | From another thread: wait-free `getAndSet` onto `IngressQueue`. On the worker: plain store into `TransitQueue` |
| Flow operators | Circuit-thread only | State accessed only from circuit thread |
| Subscriber callbacks | Circuit-thread only | Invoked during circuit drain; the registrar rejects any other thread |
| `circuit.await()` | Thread-safe (any caller thread) | Per-awaiter barrier (volatile flag) + park/unpark; fails fast if called from worker |
| `circuit.close()` | Thread-safe, idempotent | Close marker enqueued, then volatile `closed` set, then unpark |
| `resource.closeAwait()` | Thread-safe; rejects worker thread | Fails fast via `checkExternalCaller` before any side effect, then close + await |
| `cortex.name()` | Thread-safe | ConcurrentHashMap interning |
| `scope.close()` | Thread-safe, idempotent | Terminal transition is one `AtomicBoolean.getAndSet`; members detached under the scope's lock, closed outside it |

## Constraints

| Constraint | Reason |
|---|---|
| Virtual thread per circuit worker | Scalability — thousands of circuits without platform thread exhaustion. (A circuit that creates tickers also starts one platform daemon thread for its shared ticker scheduler.) |
| Sequential execution | Thread safety guarantee — no locks needed in callbacks |
| Transit priority | Causality preservation — cascading effects complete before the next ingress item |
| Wait-free producer | Performance — `getAndSet` always succeeds in one atomic operation |
| No ingress node recycling | A recycled cross-thread node is an ABA: a producer can claim a node the consumer already freed |
| Eager thread start | Circuit ready immediately on construction |
| Spin, then park until told | An idle circuit consumes nothing; a busy one never parks, so its producers never pay an unpark |

## Constants

| Constant | Value | Description |
|---|---|---|
| `TransitQueue.INITIAL_CAP` | 8 | Initial transit ring capacity (grows by doubling). Cyclic cascades alternate enqueue/dequeue on one thread, so steady-state max simultaneous entries ≈ 1; an 8-slot start covers any realistic multi-submit fiber without growth |
| `FsCircuit.SPIN_COUNT` | 1000 | Worker spin before parking (~26µs with `Thread.onSpinWait`, measured on Zen 3). A circuit fed faster than this never parks. Override: `io.fullerstack.substrates.worker.spin` |
| `FsCircuit.AWAIT_SPIN_COUNT` | 50 | Awaiter spin before parking. A marker on a running worker fires in ~100ns, so a short budget catches it; behind a drain no budget catches it. 50 and 1000 measure the same on every row, 0 is 6-7x worse on tight awaits. Override: `io.fullerstack.substrates.await.spin` |
| `FsCircuit.AWAIT_PARK_NANOS` | 1,000,000 | Awaiter's bounded park on the slow path (1ms), so a close racing an await is observed |
| `FsCircuit.PARK_NANOS` | 100,000,000 | Worker park timeout (100ms). A safety net only — producers unpark. Override: `io.fullerstack.substrates.worker.park` |
| `FsOperators.Chance.SEED_PROPERTY` | `io.fullerstack.substrates.seed` | Seed for every `chance` operator in the JVM; drawn and published back when unset |

## Diagnostics

`Circuit.pulse()` returns an `Optional<Pulse>` snapshot of a no-op probe's round-trip through the ingress queue, exposing four timestamps (start / enqueued / dequeued / stop) for supervisory observers, and empty once the worker has terminated. See `FsCircuit.pulse()` and the `PulseProbe` inner class — the spec-level diagnostic surface. The provider keeps no internal queue/drain counters: `Pulse` provides representative timing without counter writes on the per-emission hot path.

The classes `AwaitMarker`, `CloseMarker`, `PulseProbe`, and `ReceptorAdapter` must remain distinct concrete classes — collapsing any of these into a shared base reintroduces a bimorphic call-site profile in the drain, recorded in `FsCircuit`'s marker note as a regression from ~22 ns to 30+ ns on a batch-await row. **No TCK covers it and neither do the local regression tests**, so the invariant remains documentation only. See [Conformance](CONFORMANCE.md).

## Performance

Measurement is [perfkit-java](https://github.com/humainary-io/perfkit-java), run against this
provider by Maven coordinate — 246 benchmark methods across 39 classes in the current checkout,
covering Substrates and Serventis. This project keeps no benchmarks and publishes no standing
figures: numbers move with the host.

```bash
./scripts/benchmark.sh decision core      # 3 forks, 8 warmup + 10 measurement iterations
./scripts/benchmark.sh allocation core    # bytes per op, and the allocation sites
```

Read a result against perfkit's own criteria (its `BENCHMARKS.md`): a run is **invalid** below 3
fork series, with missing metrics, or above ~10% confidence error. Publish scores with error bars.
The paired `_control_` rows are diagnostic and are **never subtracted** from a target score.

Two properties worth knowing before optimising anything here:

- **Allocation on the emission path is one `Admission` per ingress emission, and nothing on
  transit.** A cross-thread carrier cannot be recycled (see above), so each emission from another
  thread allocates its node; a cascade emission on the worker is stored inline in the transit ring
  and allocates nothing.
- **Circuit lifecycle is deliberately unmeasured**, upstream included. perfkit excludes it because
  creation is dominated by virtual-thread startup and scheduler effects the harness cannot
  stabilise, and lifecycle costs sit orders of magnitude above steady-state. That is why circuits
  are designed to be long-lived.

## References

- [Substrates Specification](https://github.com/humainary-io/substrates-api-spec/blob/main/SPEC.md) — formal behavioural contracts
- [Design Rationale](https://github.com/humainary-io/substrates-api-spec/blob/main/RATIONALE.md) — why determinism over throughput
- [Substrates API](https://github.com/humainary-io/substrates-api-java) — API interfaces (Javadoc)
- [Serventis API](https://github.com/humainary-io/serventis-api-java) — semiotic observability instruments
- [Decisions](DECISIONS.md) — designs built, measured and rejected, and why the current ones stand
- [Conformance](CONFORMANCE.md) — TCK results and known gaps
