# Fullerstack Substrates — Implementation Architecture

This document describes how Fullerstack implements the [Substrates Specification](https://github.com/humainary-io/substrates-api-spec). For what Substrates is and why it works this way, read the official [Specification](https://github.com/humainary-io/substrates-api-spec/blob/main/SPEC.md) and [Rationale](https://github.com/humainary-io/substrates-api-spec/blob/main/RATIONALE.md).

---

## Implementation Decisions

The spec is language-independent. These are our Java 26 projection choices:

| Spec Concept | Our Implementation | Why |
|---|---|---|
| Execution context | Virtual thread (one per circuit) | Lightweight, no platform thread exhaustion |
| Ingress queue | Custom `IngressQueue` (wait-free MPSC linked list of `QChunk`) | ~13ns emit, no CAS contention on producers |
| Transit queue | Custom `TransitQueueRing` (single-threaded power-of-2 ring) | Zero indirection on cascade hot path, automatic growth |
| Per-emission operators | `FsFiber` (immutable, reusable, 35+ ops) | Spec §6 — Fiber is the per-emission processing recipe |
| Memory ordering | `VarHandle` release/acquire (not volatile) | Cheaper than volatile for parked flag checks |
| False sharing | `@Contended` on `QChunk.claimed` | Isolate producer's atomic from consumer's cache line |
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
              │     └── QChunk (128-slot interleaved [receiver, value] array)
              ├── TransitQueueRing (single-threaded power-of-2 ring; cascade FIFO)
              ├── FsConduit (channel factory + subscriber management)
              │     ├── FsHub (subscriber list + version counter)
              │     ├── FsChannel (per-name dispatch — split: dispatch vs cascadeDispatch)
              │     │     └── FsPipe (async emission carrier — emit only)
              │     └── FsDerivedPool (derived view: pool(Function), pool(Flow), pool(Fiber))
              ├── FsBank (2.5 — closeable name-indexed conduit factory)
              ├── FsCell (2.7 — circuit-owned single-slot state; receptor-pipe + volatile)
              ├── FsPin (2.9 — circuit-owned, owner-context-guarded state handle;
              │          immediate get/set on the worker thread, ISE elsewhere)
              ├── FsPort (2.9 — circuit-owned queued mutation handle without read;
              │           emit/replace/update via CircuitJob submission)
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
              ├── FsTap (source emission transformation; tap(Function|Flow|Fiber))
              ├── FsTicker (2.8 — circuit-owned periodic emitter; grid-anchored fixed-rate
              │             schedule, gap-free Long sequence, bounded catch-up; backed by
              │             a lazy single-thread daemon ScheduledExecutorService shared
              │             across all tickers on the circuit, shut down on circuit close)
              ├── FsWindow (2.6 — strided view over a rolling buffer; restriction ops share buffer)
              └── FsReservoir (bounded buffered emission capture — 2.9 ArrayDeque with eviction)

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

### QChunk — the ingress storage unit

`IngressQueue` stores emissions in `QChunk` — a 128-slot array with interleaved `[receiver, value]` layout:

```java
final class QChunk {
  static final int CAPACITY  = 128;
  static final int ARRAY_LEN = CAPACITY << 1;  // 256

  final    Object[] slots = new Object[ARRAY_LEN];  // [r0,v0,r1,v1,...]
  volatile QChunk   next;                           // link to next chunk

  @Contended
  volatile int claimed;                             // ingress: atomic getAndAdd
  QChunk freeNext;                                  // free list link
}
```

**Why interleaved?** Receiver and value land in adjacent positions for spatial cache locality. No wrapper object per emission — the chunk IS the storage.

**Why 128?** Tuned from a benchmark sweep. Doubling capacity halves the per-chunk-transition overhead (release-store fence on `next`, free-list pop) at the cost of slightly larger chunks. On the cyclic deep-cascade benchmark, 128 won.

### IngressQueue — wait-free MPSC

External threads enqueue emissions via atomic `getAndAdd` on the chunk's `claimed` counter:

```java
public void enqueue(Consumer<Object> receiver, Object value) {
  QChunk chunk = tail;                                    // volatile read
  int slot = (int) QChunk.CLAIMED.getAndAdd(chunk, 1);   // wait-free claim
  if (slot < QChunk.CAPACITY) {
    int base = slot << 1;
    chunk.slots[base + 1] = value;                        // plain store (value first)
    QChunk.SLOTS.setRelease(chunk.slots, base, receiver); // release store (commit)
  } else {
    enqueueSlow(receiver, value, chunk);                  // cold path: 1 in 64 emits
  }
}
```

**Key properties:**
- `getAndAdd` always succeeds — no CAS retry loop, true wait-free
- Receiver written with `setRelease` — acts as commit signal for the consumer
- Value written before receiver — consumer sees value when it sees receiver
- When chunk fills (every 128th emit), a new chunk is linked or recycled from a Treiber stack free list
- `@Contended` on `tail` and `freeHead` prevents false sharing with consumer reads

**Consumer drain** processes committed slots with interleaved transit drain:

```java
// Simplified — actual code handles chunk transitions and markers
Consumer<Object> r = (Consumer<Object>) QChunk.SLOTS.getAcquire(slots, base);
if (r == null) break;  // not yet committed
Object v = slots[base + 1];
slots[base] = null;     // clear for GC
slots[base + 1] = null;
r.accept(v);            // execute emission

// Depth-first: drain all transit work before next ingress slot
if (circuit.transitHasWork()) {
  do {} while (circuit.drainTransit());
}
```

### TransitQueueRing — single-threaded ring

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
private void workerLoop() {
  final IngressQueue q = ingress;

  for (;;) {
    boolean didWork = q.drainBatch(this);  // drain ingress + interleaved transit
    if (didWork) continue;
    if (shouldExit) return;

    // Spin before parking
    Object found = null;
    for (int i = 0; i < SPIN_COUNT && found == null; i++) {
      Thread.onSpinWait();
      found = q.peek();
    }

    if (found == null) {
      LockSupport.parkNanos(PARK_NANOS);   // self-waking timed park
    }
  }
}
```

**Self-waking design:** the worker uses `parkNanos` instead of `park`. Producers never call `unpark` — the worker wakes itself after the timeout. This eliminates the cost of producer-side park/unpark coordination on the hot path. The only explicit `unpark` calls are from `await()` and `close()` (cold paths).

**Callback isolation (spec §15.4):** `IngressQueue.drainBatchLoop` and `TransitQueueRing.drain` each wrap their `r.accept(v)` dispatch in a `try { … } catch (Throwable ignored) { }` so an uncaught client-callback exception cannot terminate the worker. `FsChannel`'s multi-consumer dispatch lambda wraps each sibling receptor invocation the same way — a throwing receptor doesn't block siblings on the same channel from receiving the emission (§16.1 #14). The subscriber callback in `FsChannel.rebuild` is similarly guarded and records an empty consumer list on throw so the callback is never retried for that subscription/channel pair (§16.1 #15).

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
static final class CircuitJob   implements Consumer<Object> { /* one-shot Runnable */ }
```

The drain loop splits the call site: an `isMarker()` identity check (compares against the two pre-allocated marker references) routes markers through a separate cold path (`fireMarker`) so they keep their own type profile. `CircuitJob` is used by `FsConduit` for `subscribe`/`unsubscribe` and similar circuit-thread-only work, again avoiding lambda capture pollution at `ReceptorAdapter.accept`.

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
  if (Thread.currentThread() == worker)
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
| Self-waking park | Producers never pay unpark cost on hot path |

## Constants

| Constant | Value | Description |
|---|---|---|
| `QChunk.CAPACITY` | 128 | Slots per ingress chunk (receiver+value pairs); tuned 64→128 from a sweep |
| `QChunk.ARRAY_LEN` | 256 | Array length (128 × 2 for interleaving) |
| `TransitQueueRing.INITIAL_CAP` | 8 | Initial transit ring capacity (grows by doubling). Cyclic cascades alternate enqueue/dequeue on one thread, so steady-state max simultaneous entries ≈ 1; an 8-slot start covers any realistic multi-submit fiber without growth |
| `FsCircuit.SPIN_COUNT` | 1000 | Worker spin iterations before parking (~5µs with `Thread.onSpinWait`) |
| `FsCircuit.AWAIT_SPIN_COUNT` | 1000 | Awaiter spin-before-park budget (~2µs window). Catches the marker fire in tight ping-pong (sync-bridge / shallow cyclic) without paying the virtual-thread park/unpark round-trip; falls back to `LockSupport.park()` for longer waits. Tuned via sweep — 500 falls below the cliff, 5000+ wastes spin on deep cascades. |
| `FsCircuit.PARK_NANOS` | 1,000 | Timed park interval (1µs — virtual-thread-friendly) |

## Diagnostics

`Circuit.pulse()` (Substrates 2.4) returns an `Optional<Pulse>` snapshot of a no-op probe's round-trip through the ingress queue, exposing four timestamps (start / enqueued / dequeued / stop) for supervisory observers. See `FsCircuit.pulse()` and the `PulseProbe` inner class — the spec-level diagnostic surface. We previously carried a `CircuitStats` record with internal queue/drain counters; that has been removed since `Pulse` provides representative timing without polluting the per-emission hot path with counter writes.

The `FsCircuitMarkerInvariantTest` structural tests assert that `AwaitMarker`, `CloseMarker`, `CircuitJob`, and `ReceptorAdapter` remain distinct concrete classes — collapsing any of these into a shared base reintroduces a bimorphic call-site profile on `r.accept(v)` in the drain loop and regresses `PipeOps.async_emit_batch_await` from ~22 ns to ~30+ ns.

## Performance

See [Benchmark Comparison](BENCHMARK-COMPARISON.md) for full JMH results across 14 groups. Those cross-platform numbers were collected with mismatched hardware and warmup parameters and are due for re-measurement on a quiet host.

Most-recent figure (JDK 26, GitHub Codespaces 2 vCPU, 10-iteration warmup):

| Operation | ns/op | What it measures |
|---|---:|---|
| `cyclic_emit_deep_await_batch` | ~12.9 | Per-cycle cost of a deep cascade through cyclic pipe networks |

## References

- [Substrates Specification](https://github.com/humainary-io/substrates-api-spec/blob/main/SPEC.md) — formal behavioural contracts
- [Design Rationale](https://github.com/humainary-io/substrates-api-spec/blob/main/RATIONALE.md) — why determinism over throughput
- [Substrates API](https://github.com/humainary-io/substrates-api-java) — API interfaces (Javadoc)
- [Serventis API](https://github.com/humainary-io/serventis-api-java) — semiotic observability instruments
