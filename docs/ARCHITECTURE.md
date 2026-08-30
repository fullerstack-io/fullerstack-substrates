# Fullerstack Substrates — Implementation Architecture

This document describes how Fullerstack implements the [Substrates Specification](https://github.com/humainary-io/substrates-api-spec). For what Substrates is and why it works this way, read the official [Specification](https://github.com/humainary-io/substrates-api-spec/blob/main/SPEC.md) and [Rationale](https://github.com/humainary-io/substrates-api-spec/blob/main/RATIONALE.md).

---

## Implementation Decisions

The spec is language-independent. These are our Java 26 projection choices:

| Spec Concept | Our Implementation | Why |
|---|---|---|
| Execution context | Virtual thread (one per circuit) | Lightweight, no platform thread exhaustion |
| Ingress queue | Custom `IngressQueue` (wait-free MPSC, intrusive-linked `Admission` per emission) | ~13ns emit, no CAS contention on producers |
| Transit queue | Custom `TransitQueue` (single-threaded power-of-2 ring) | Zero indirection on cascade hot path, automatic growth |
| Per-emission operators | `FsFiber` (immutable, reusable, 35+ ops) | Spec §6 — Fiber is the per-emission processing recipe |
| Memory ordering | `VarHandle` release/acquire (not volatile) | Cheaper than volatile for parked flag checks |
| False sharing | Padding hierarchy of `int` fields around `head`/`tail` | `@Contended` is inert without `-XX:-RestrictContended`; measured 20.5ns unpadded vs 13.5ns padded |
| Name interning | `ConcurrentHashMap` with hierarchical parent links | O(1) identity comparison via reference equality |
| Subject identity | `AtomicLong` counter (~5ns vs UUID's ~300ns) | Simple, fast, no collision risk in single JVM |
| Slot storage | Immutable `record` (Name + value + type) | Compact, no synchronization needed |
| Resource lifecycle | `Scope` with reverse-order close list | RAII-like structured cleanup |

## Class Map

30 classes in `io.fullerstack.substrates`:

```
FsCortexProvider (SPI entry point)
  └── FsCortex (entry point — creates circuits, scopes, names, states, slots, flows, fibers)
        └── FsCircuit (dual-queue sequential execution engine)
              ├── IngressQueue (wait-free MPSC — external emissions)
              │     └── Admission (one per emission; carries receiver, value, marker flag, next)
              ├── TransitQueue (single-threaded power-of-2 ring; cascade FIFO)
              ├── FsConduit (channel factory + subscriber management)
              │     ├── FsHub (subscriber list + version counter)
              │     ├── FsChannel (per-name dispatch — split: dispatch vs cascadeDispatch)
              │     │     └── FsPipe (async emission carrier — emit only)
              │     └── FsDerivedPool (derived view: pool(Function), pool(Flow), pool(Fiber))
              ├── FsBank (2.5 — closeable name-indexed conduit factory)
              ├── FsCell (2.7 — circuit-owned single-slot state; receptor-pipe + volatile)
              ├── FsPin (2.9 — circuit-owned, owner-context-guarded state handle;
              │          immediate get/set on the worker thread, ISE elsewhere)
              ├── FsPort (2.9 — circuit-owned queued mutation handle without read; holds three
              │           circuit-issued pipes, so replace/update/emit emit the value, the
              │           function and the target rather than wrapping each in a carrier)
              ├── FsFlow (type-changing composition: map / scan / window / flow / fiber / pipe —
              │           uniform Wrap[] storage; 2.6/2.7 adds scan, window(int), window(Duration, int),
              │           flow(Function<Subject, Flow>))
              ├── FsFiber (per-emission operators: ~42 — guard, diff, limit, peek, replace, ...,
              │           plus chance, change, deadband, delay, edge, every,
              │           hysteresis, inhibit, pulse, rolling, steady, tumble,
              │           plus 2.5: distinct, distinct(int), route, streak, tee, when,
              │           plus 2.7: every(Duration))
              ├── FsOperators (shared operator implementations consumed by FsFiber and FsFlow;
              │                includes 2.7 EveryTime for time-based rate limiting)
              ├── FsSubscriber (emission observer with lazy callback)
              │     └── FsSubscription (subscriber lifecycle handle)
              │           └── FsRegistrar (Consumer<Object> registration during callback)
              ├── FsTicker (2.8 — circuit-owned periodic emitter; grid-anchored fixed-rate
              │             schedule, gap-free Long sequence, bounded catch-up; backed by
              │             a lazy single-thread daemon ScheduledExecutorService shared
              │             across all tickers on the circuit, shut down on circuit close)
              ├── FsWindow (2.6 — strided view over a DelayLine ring; restriction ops share buffer)
              │     └── WindowLease (§6.4.1 (context, generation) lease; owns the ring reference)
              ├── FsSink (2.10 — capture-producing pool of channels; endpoint normalised through
              │           circuit.pipe(...) so a capture lands on this circuit before crossing)
              └── FsBasin (3.0 — bounded buffered emission capture, DelayLine-backed; replaced
                            FsReservoir, which 3.0 removed along with Tap/Source.tap)

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

Every Substrate impl uses an **eager-final `Subject` field built in the constructor** — there is no shared abstract base or lazy DCL pattern. (An earlier `FsSubstrate` helper was removed during the spec audit; an `FsFault` was removed in 2.4 once the API made `Fault` a `final class`.)

---

## Queue Architecture

The performance-critical core. Spec requirements:

- **One execution context per circuit** — all processing on a single virtual thread
- **Deterministic ordering** — emissions processed in strict enqueue order
- **Dual-queue model** — ingress (external) + transit (cascading) with transit priority
- **No call stack recursion** — cascading emissions enqueue, never invoke nested calls
- **Stack safe** — even deeply cascading chains don't overflow the stack
- **Wait-free producer** — callers never block when emitting

### Admission — the ingress carrier

`IngressQueue` mints one `Admission` per emission, carrying its own `next` link:

```java
final class Admission {
  Consumer<Object> receiver;   // the receptor this emission delivers to
  Object           value;      // the emission; null for a marker
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

**Consumer drain** runs every committed admission, interleaving transit after each:

```java
Admission t = tail;
Admission j = (Admission) NEXT.getAcquire ( t );
if ( j == null ) return false;          // nothing committed

do {
  t.next = null;                        // unlink the consumed node for GC
  t = j;
  run ( t, circuit );                   // dispatch, then drain transit to empty
  j = (Admission) NEXT.getAcquire ( t );
} while ( j != null );
tail = t;
```

Causal completion (§5.3): each admission's transit cascade is drained to empty before the next
ingress admission runs.

### TransitQueue — single-threaded ring

Cascading emissions (from within subscriber callbacks) go to the transit ring. No atomics — only the circuit thread accesses it. Two parallel arrays addressed by `head & mask` / `tail & mask`:

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
    r.accept(v);
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
- Transit drains with **priority** over ingress — all cascading effects complete before the next external emission

Transit priority ensures **causal completion** — all cascading effects of an emission resolve atomically before the next external emission is processed. This eliminates race conditions without locks.

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
final void submitIngress(Consumer<Object> receiver, Object value) {
  ingress.enqueue(receiver, value, false);
  if (parked) LockSupport.unpark(worker);
}
```

**Park until told:** the worker spins, then publishes `parked` and parks; a producer that admits work reads that flag after its exchange and unparks the worker. The pair is a Dekker handshake, so no admission can be left unnoticed, and the timeout is only a safety net for a wake that was somehow missed. The worker used to self-wake on a 1µs `parkNanos` with no producer-side coordination at all: measured, that cost 0.95 cores per *idle* circuit and delivered an emission into a quiet circuit in 648µs at the median and 3.8ms at p90, because the timer that had to wake it competed with the carrier it was spinning on. The producer now pays one byte load and a not-taken branch on a cache line it already reads (`closed` and `parked` are adjacent). See `docs/DECISIONS.md`.

**Callback isolation (spec §15.4):** `IngressQueue.drainBatchLoop` and `TransitQueue.drain` each wrap their `r.accept(v)` dispatch in a `try { … } catch (Throwable ignored) { }` so an uncaught client-callback exception cannot terminate the worker. `FsChannel`'s multi-consumer dispatch lambda wraps each sibling receptor invocation the same way — a throwing receptor doesn't block siblings on the same channel from receiving the emission (§16.1 #14). The subscriber callback in `FsChannel.rebuild` is similarly guarded and records an empty consumer list on throw so the callback is never retried for that subscription/channel pair (§16.1 #15).

### Marker class split — JIT monomorphism

User receptors flow through a single concrete class so the hot-path `r.accept(v)` site stays monomorphic:

```java
static final class ReceptorAdapter<E> implements Consumer<Object>, Receptor<E> {
  final Receptor<? super E> receptor;
  @SuppressWarnings("unchecked")
  public void accept(Object o) { receptor.receive((E) o); }
  public void receive(E emission) { receptor.receive(emission); }
}
```

Markers and circuit jobs go through their own concrete classes — distinct types from `ReceptorAdapter` so they never pollute the hot-path type profile:

```java
static final class AwaitMarker  implements Consumer<Object> { /* await marker */ }
static final class CloseMarker  implements Consumer<Object> { /* close marker */ }
static final class PulseProbe   implements Consumer<Object> { /* §5.7 round-trip probe */ }
```

The drain loop splits the call site: an `isMarker()` identity check (compares against the two pre-allocated marker references) routes markers through a separate cold path (`fireMarker`) so they keep their own type profile.

A fourth class, `CircuitJob`, used to wrap a `Runnable` for `FsConduit`'s subscribe/unsubscribe and for `FsPort`/`FsBasin`/`FsSubscription`. It is gone: each of those queued work whose only varying part *is* a value, so they now emit that value into a pipe the circuit issued them — the subscriber joining, the function to apply, the drain target. See [Capabilities](CAPABILITIES.md).

**Why all this?** Multiple lambda classes flowing through a single virtual call would cause bimorphic or megamorphic dispatch and class_check traps. Splitting by purpose keeps each call site monomorphic — C2 can devirtualise and inline.

---

## Async Model — Why Everything Is Async

Substrates is async-first — `pipe.emit(value)` enqueues and returns immediately. Processing happens later on the circuit's virtual thread. This is the opposite of RxJava (synchronous by default).

Spec §14 (Async Pipe Dispatch) formalises this for cyclic topologies via `circuit.pipe(target)` — a pipe that re-enters the circuit's queue rather than calling the target directly. This is what makes deeply recurrent networks stack-safe.

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

When signals cross circuit boundaries (e.g., a tap emitting from one circuit into another), you need to await both:

```java
var circuit1 = cortex().circuit(cortex().name("source"));
var circuit2 = cortex().circuit(cortex().name("target"));

// ... wire tap from circuit1 to circuit2 ...

pipe.emit("value");
circuit1.await();  // Drain source (tap fires)
circuit2.await();  // Drain target (tap emission processed)
```

For deep chains (3+ circuits), multiple rounds may be needed:

```java
for (int round = 0; round < 3; round++) {
    circuit1.await();
    circuit2.await();
    circuit3.await();
}
```

### How `await()` works

`await()` injects a marker into the ingress queue and parks the calling thread. When the circuit thread processes the marker (after all preceding nodes), it unparks the caller. This guarantees all emissions submitted before `await()` have been fully processed, including any cascading transit emissions they triggered.

The thread-identity check that prevents worker-thread callers from deadlocking is centralised in `checkExternalCaller(op)` and reused by `await()`, `pulse()`, and every `closeAwait()` (2.5). `closeAwait()` calls `checkExternalCaller` *before* any close side-effect, so an illegal call fails fast with no partial state change (spec §16.1 #13).

```java
public void await() {
  checkExternalCaller("await");
  if (closed) return;
  awaitImpl();
}

void checkExternalCaller(String op) {
  // §11.3's own idiom. The spec names comparing `Cortex.current()` against the
  // circuit's `current()` as the specified way to detect circuit-context re-entrancy,
  // and §5.7 makes this circuit's Current *be* its worker's, so the two agree by
  // construction. Every context decision in the provider is written this way —
  // FsPipe's §5.3 routing, FsPort, FsSink's §11.1 attribution, FsPin's §11.6 guard —
  // and the worker Thread is never consulted for identity.
  if (cortex.current() == current())
    throw new IllegalStateException("Cannot call Circuit::" + op + " from within a circuit's thread");
}

private void awaitImpl() {
  Thread current = Thread.currentThread();
  Thread existing = (Thread) AWAITER.compareAndExchange(this, null, current);
  if (existing != null) {
    // Piggyback on existing awaiter
    while (AWAITER.getOpaque(this) == existing) LockSupport.parkNanos(1_000_000);
    return;
  }
  // Inject marker and park
  submitIngress(awaitMarkerReceiver, null);
  LockSupport.unpark(worker);  // wake worker to process marker
  while (AWAITER.getOpaque(this) == current) LockSupport.park();
}

// Marker callback — runs on circuit thread, unparks awaiter
private void onAwaitMarker(Object ignored) {
  Thread awaiter = (Thread) AWAITER.getAndSet(this, null);
  if (awaiter != null) LockSupport.unpark(awaiter);
}
```

FIFO ordering guarantees all prior emissions complete before the marker executes.

### Summary

| | RxJava | Substrates |
|---|---|---|
| `emit()` | Blocks until callback completes | Returns immediately (~13ns) |
| Callbacks | Execute on calling thread | Execute on circuit virtual thread |
| Testing | Assert directly after emit | Must `circuit.await()` first |
| Ordering | Depends on scheduler | Deterministic FIFO + depth-first |
| Concurrency | Locks needed | Lock-free (single-threaded) |

---

## Context, Time and Temporal Validity

Three mechanisms answer questions an operator or a caller cannot answer locally. They are grouped
here because they share one root cause: **an operator reaches `FsOperators.Wrap.wrap(Consumer)`
with no reference to the circuit driving it.** The chain it builds is nested `Consumer.accept(E)`
calls, and `accept` takes one parameter — the value.

### Which context am I? (§11.3)

`cortex.current() == circuit.current()`, written at every site that makes a context decision:
`FsPipe`'s §5.3 ingress-versus-transit routing, `FsPort`, `FsSink`'s §11.1 capture attribution,
`FsPin`'s §11.6 owner guard, and `FsCircuit`'s `submit` and `checkExternalCaller`.

§11.3 names that comparison as the specified idiom, and §5.7 requires this circuit's `Current` to
**be** its worker's Current — otherwise the guard would always answer "not on the circuit", which
is the failure it exists to catch. The worker binds it on entry to `workerLoop`.

A raw `Thread.currentThread() == worker` answers the same question — §11.3 names
`Thread.currentThread()` as Current's Java projection — and the provider used to do exactly that
behind an `onWorker()` helper. It was replaced because the idiom costs nothing measurable
(`async_emit_admission_batch` did not regress) and because it removes a package-private accessor
that handed the worker `Thread` to five call sites, each of which could have parked or interrupted
it.

`Current` is a `ThreadLocal`. It was a `ConcurrentHashMap` keyed by `Thread.threadId()`, which
never evicted — a thread id is not a liveness signal, so every thread that ever asked for a Current
leaked one plus an interned `thread.<name>` node. Virtual threads made that unbounded. §11.3 asks
for interning "for that context's lifetime"; a ThreadLocal entry dies with its thread, which is
that lifetime rather than an approximation of it.

### What time is it? (§5.8)

§5.8: *"A single ingress item and the entire transit cascade it triggers share one processing-time
reading … it does **not** advance across the internal transit hops of one cascade."* A whole
cascade is one causal moment, so a transit hop that blocks for 25 ms must be invisible to a 5 ms
`window(Duration, capacity)`.

`FsCircuit.stimulus` is a two-field holder — value and validity — invalidated by a plain boolean
store once per ingress item in `IngressQueue.drainBatchLoop`, **before dispatch and deliberately
not inside `drainTransit()`**. The reading is established by the first time-aware operator that
asks, which is §5.8's own *"lazily established"* licence: an implementation need not read the clock
for ingress items no time-aware operator observes.

The three readers — `WindowTime`, `SteadyTime`, `EveryTime` — reach the holder through a
`ThreadLocal` the worker binds once at thread start. **That is the weak part of this design**: where
no binding exists the accessor falls back to a per-call clock read rather than failing, so the
non-conformance cannot announce itself. See [Conformance](CONFORMANCE.md).

### Is this window still mine? (§6.4.1)

`Flow.window` allocates **one** buffer per materialisation and rewrites it in place on every
emission; the `Window` handed downstream is a strided view over that live storage, not a snapshot.
That is what the spec sanctions — §6.2.3 puts the obligation on the caller (*"MUST NOT be retained
… callers that need values beyond the callback MUST copy them"*) and §6.4.1 then requires the
implementation to **detect** violations: *"undefined behavior is not an acceptable choice for this
type."*

`FsWindow.Lease` is that detector, and it is a **(context, generation)** pair. Both halves are
load-bearing:

- the **context** catches a window that left the worker — removing it fails
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

`FsName` interns all name segments and caches the hierarchical structure. Two names with the same path are the same object (reference equality). This makes name comparison O(1) — critical for `conduit.get(name)` which happens on every emission.

```
cortex.name("kafka.broker.1")
  → FsName["kafka"]
       └── FsName["broker"] (parent = kafka)
            └── FsName["1"] (parent = broker)
```

Subsequent calls to `cortex.name("kafka.broker.1")` return the same `FsName` instance.

## Flow and Fiber

Per-emission processing lives on `Fiber<E>` (since 2.3); `Flow<I,O>` is reduced to type transformation. 2.4 adds `Flow.fiber(Function<Subject<?>, Fiber<O>>)` — a per-attachment factory invoked once per `pipe(target)` call, materialised inline in `FsFlow.pipe`.

### `FsFiber` — per-emission operators

`FsFiber` is an immutable, reusable composition of operators (~41) that act on emissions of a single type. Each operator method returns a new fiber with the operator appended; the fiber value is reusable and may be materialised against multiple pipes, with each materialisation producing independent state.

Carryover operators (state classes shared with `FsFlow`):

- **diff()** — suppress unchanged values (Shannon's principle: only changes carry information)
- **guard(predicate)** — filter by predicate, with optional stateful bi-predicate
- **limit(n)** / **skip(n)** — windowing
- **peek(receptor)** — side-effect without consuming
- **reduce(initial, op)** / **integrate(...)** / **relate(...)** — running aggregation / windowed aggregation
- **replace(op)** — value transformation
- **takeWhile / dropWhile** — predicate-based windowing
- **above / below / clamp / range / max / min / high / low** — comparator-based sift

2.3-introduced operators (defined in `FsFiber`):

- **chance, change, deadband, delay, edge, every, hysteresis, inhibit, pulse, rolling, steady, tumble**

2.5-introduced operators (defined in `FsFiber`, classes in `FsOperators`):

- **distinct()** — unbounded duplicate suppression via `HashSet`
- **distinct(capacity)** — FIFO-windowed duplicate suppression via `LinkedHashSet`; suppressed duplicates do not refresh position
- **route(predicate, pipe)** — predicate-matched values diverted to a side pipe, non-matching pass through (demux)
- **streak(required, matches)** — emit Nth consecutive match, then re-arm; non-match resets counter. `required == 1` short-circuits to `guard(matches)` (no carried state)
- **tee(pipe)** — fan-out: side-pipe receives, value continues downstream
- **when(predicate, fiber)** — matching values traverse a pre-materialised sub-fiber chain that terminates at the same downstream; non-matching pass through unchanged. Empty sub-fiber → stage is identity (returned as-is)

### `FsFlow` — type transformation

`FsFlow<I,O>` provides only the type-changing surface: `map`, `flow`, `fiber`, `pipe`. `flow.fiber(fiber)` attaches a fiber at the output side; `flow.pipe(target)` materialises the chain into a new pipe whose terminal submits directly to the target's transit queue, bypassing the channel's version check on the cascade hot path (per spec §5.4.1 + §7.6.2 — subscriber state cannot change mid-cascade).

Both flow and fiber state are safe without synchronization because all processing runs on the circuit's single thread.

## Lazy Rebuild — the `dispatch` / `cascadeDispatch` split

When a subscriber is added or removed from a conduit, the change is not applied immediately. Instead, the conduit's hub version counter increments. On the next ingress emission, each channel checks if its cached subscriber list is stale (version mismatch) and rebuilds if needed.

Each named pipe in a conduit is fronted by an `FsChannel`. Channels expose two pre-built `Consumer<Object>` references after rebuild:

- **`dispatch`** — receptors only, no STEM walk. Used by ingress `receive()` (which adds the version check + STEM externally) and by `dispatchStem` when walking ancestors (so an ancestor's STEM walk is not retriggered).
- **`cascadeDispatch`** — receptors + STEM (if applicable). Submitted directly to transit by fiber/flow terminals to bypass the channel's version check on the cascade hot path.

The cascade-side bypass is sound by spec: §5.4.1 relation 3 + §7.6.2 guarantee that no subscriber-state change can interleave during a cascade — the version check therefore only needs to fire on ingress arrival, not on every transit step. `FsFlow.pipe(target)` and `FsFiber.pipe(target)` detect a same-circuit `FsPipe` whose receiver is an `FsChannel` and submit `channel.cascadeDispatch` straight to the transit ring.

For non-STEM channels, `cascadeDispatch == dispatch`. For STEM channels, `cascadeDispatch` wraps `dispatch` with the ancestor walk.

This avoids locking during subscription changes — the spec's "eventual consistency" model. A subscriber added between two emissions will see the second emission but not the first.

## Thread Safety Summary

| Component | Thread Safety | Why |
|---|---|---|
| `pipe.emit()` | Thread-safe (any thread) | Enqueues to IngressQueue via atomic getAndAdd |
| Flow operators | Circuit-thread only | State accessed only from circuit thread |
| Subscriber callbacks | Circuit-thread only | Invoked during circuit drain |
| `circuit.await()` | Thread-safe (any caller thread) | VarHandle park/unpark coordination; fails fast if called from worker |
| `circuit.close()` | Thread-safe, idempotent | Atomic flag + unpark |
| `resource.closeAwait()` (2.5) | Thread-safe; rejects worker thread | Fails fast via `checkExternalCaller` before any side effect, then close + await |
| `cortex.name()` | Thread-safe | ConcurrentHashMap interning |
| `scope.close()` | Not thread-safe | Close from owning thread only |

## Constraints

| Constraint | Reason |
|---|---|
| Virtual threads only | Scalability — thousands of circuits without platform thread exhaustion |
| Sequential execution | Thread safety guarantee — no locks needed in callbacks |
| Transit priority | Causality preservation — cascading effects complete atomically |
| Wait-free producer | Performance — `getAndAdd` always succeeds in one atomic operation |
| No node pooling | Adds contention, breaks wait-free property |
| Eager thread start | Circuit ready immediately on construction |
| Spin, then park until told | An idle circuit consumes nothing; a busy one never parks, so its producers never pay an unpark |

## Constants

| Constant | Value | Description |
|---|---|---|
| `TransitQueue.INITIAL_CAP` | 8 | Initial transit ring capacity (grows by doubling). Cyclic cascades alternate enqueue/dequeue on one thread, so steady-state max simultaneous entries ≈ 1; an 8-slot start covers any realistic multi-submit fiber without growth |
| `FsCircuit.SPIN_COUNT` | 1000 | Worker spin iterations before parking (~5µs with `Thread.onSpinWait`) |
| `FsCircuit.SPIN_COUNT` | 1000 | Worker spin before parking (~26µs). A circuit fed faster than this never parks. Override: `io.fullerstack.substrates.worker.spin` |
| `FsCircuit.AWAIT_SPIN_COUNT` | 50 | Awaiter spin before parking. A marker on a running worker fires in ~100ns, so a short budget catches it; behind a drain no budget catches it. 50 and 1000 measure the same on every row, 0 is 6-7x worse on tight awaits. Override: `io.fullerstack.substrates.await.spin` |
| `FsCircuit.PARK_NANOS` | 100,000,000 | Park timeout (100ms). A safety net only — producers unpark. Override: `io.fullerstack.substrates.worker.park` |

## Diagnostics

`Circuit.pulse()` (Substrates 2.4) returns an `Optional<Pulse>` snapshot of a no-op probe's round-trip through the ingress queue, exposing four timestamps (start / enqueued / dequeued / stop) for supervisory observers. See `FsCircuit.pulse()` and the `PulseProbe` inner class — the spec-level diagnostic surface. We previously carried a `CircuitStats` record with internal queue/drain counters; that has been removed since `Pulse` provides representative timing without polluting the per-emission hot path with counter writes.

The marker classes `AwaitMarker`, `CloseMarker`, `PulseProbe`, and `ReceptorAdapter` must remain distinct concrete classes — collapsing any of these into a shared base reintroduces a bimorphic call-site profile on `r.accept(v)` in the drain loop, measured at ~22 ns → 30+ on `async_emit_batch_await`. This used to be guarded by `FsCircuitMarkerInvariantTest`, deleted with the in-house suite; **no TCK covers it and neither do the local regression tests**, so the invariant remains documentation only. See [Conformance](CONFORMANCE.md).

## Performance

Measurement is [perfkit-java](https://github.com/humainary-io/perfkit-java), run against this
provider by Maven coordinate — 207 benchmark methods across 32 classes, covering Substrates and
Serventis. This project keeps no benchmarks and publishes no standing figures: numbers move with
the host, and the suite that used to live here contained rows that measured nothing at all.

```bash
./scripts/benchmark.sh decision core      # 3 forks, 8 warmup + 10 measurement iterations
./scripts/benchmark.sh allocation core    # bytes per op, and the allocation sites
```

Read a result against perfkit's own criteria (its `BENCHMARKS.md`): a run is **invalid** below 3
fork series, with missing metrics, or above ~10% confidence error. Publish scores with error bars.
The paired `_control_` rows are diagnostic and are **never subtracted** from a target score.

Two properties worth knowing before optimising anything here:

- **The emission path allocates nothing.** Measured at ~0 B/op — the recorded 0.003–0.016 B/op is
  noise, since one Java object would be ≥16 B. Whatever emission costs, it is not garbage, so GC
  tuning is wasted effort.
- **Circuit lifecycle is deliberately unmeasured**, upstream included. perfkit excludes it because
  creation is dominated by virtual-thread startup and scheduler effects the harness cannot
  stabilise, and lifecycle costs sit orders of magnitude above steady-state. That is why circuits
  are designed to be long-lived.

## References

- [Substrates Specification](https://github.com/humainary-io/substrates-api-spec/blob/main/SPEC.md) — formal behavioural contracts
- [Design Rationale](https://github.com/humainary-io/substrates-api-spec/blob/main/RATIONALE.md) — why determinism over throughput
- [Substrates API](https://github.com/humainary-io/substrates-api-java) — API interfaces (Javadoc)
- [Serventis API](https://github.com/humainary-io/serventis-api-java) — semiotic observability instruments
