# Decisions

Designs that were built, measured, and rejected — and the findings that outlived them.

Source javadoc explains what the code *does*. This file records what we *tried*, what it cost,
and why it lost, so a rejected design is not rediscovered as a good idea. Each entry names the
mechanism, not just the number: a timing without a mechanism is not a finding.

All figures from a 2 vCPU workspace, JDK 26, G1, compact object headers off. Allocation counts
are exact. Timings carry ±5–11% on this hardware and are quoted only where the mechanism, rather
than the magnitude, carries the argument.

---

## Ingress queue

### Chunked MPSC — `IngressQueue` + `QChunk` (superseded 2026-08-27)

A node held 128 `(receiver, value)` entries in an interleaved `Object[]`, so one allocation
covered 128 emissions: **~10 B/op against `JobQueue`'s 24**.

It lost on three counts, and the first is the one that decided it.

- **Secondary supertype check.** Batching heterogeneous work into one node forces an untyped
  `Object[]`, so reading a receiver back is a cast to an *interface*. perfasm measured that as
  the hottest instruction in the drain profile — roughly 11%, a `popcntq` and a hashed probe of
  the secondary-super table, with a stub call on a miss. A typed `Job` field is a *class*, so
  there is no check and dispatch is `invokevirtual` rather than `invokeinterface`.
- **Marker branch.** The chunked drain asked `isMarker(r)` per item. With a job hierarchy a
  marker is just another subclass and the vtable does the work.
- **Promotion under backpressure.** One surviving node per 128 emissions becomes one per
  emission, but a chunk that survives a young collection promotes 128 entries with it.

**Do not revisit** without a way to keep entries typed inside a batched node.

### Free-list recycling of chunks — REFUTED, and it was a correctness bug

Recycling a chunk across the thread boundary is an ABA: a producer can claim a node the consumer
has already freed. It **lost emissions**, and a lost await-marker left `await` parked forever.

The general rule this bought: **a carrier that crosses threads cannot be recycled.** Worker-confined
carriers can be, and are. See *Transit* below.

### Steal-the-head Treiber MPSC — `JobQueueSteal` — REFUTED, **3.7× slower**

`JobQueue` is Vyukov: publish-then-link, wait-free for producers, FIFO, but with a window where a
published node is not yet linked — so the consumer must re-read `next` with acquire semantics per
item. The idea was to close that window with Treiber ordering (link-then-publish, so a reachable
node is always fully linked) and let the consumer take the entire chain in one exchange.

It failed for a reason the design did not anticipate: the consumer's `getAndSet` of the head
operates on **the same word the producers CAS**, so stealing couples the two ends rather than
decoupling them. Producers also CAS instead of exchanging, so they retry under contention and are
no longer wait-free. The steal-and-reverse pass to restore FIFO order is pure additional work.

Making `Job.next` non-volatile for the steal path was measured separately and was worth ~13% of
that profile — see *Job.next* below — and even with it the design remained far behind.

---

## Transit queue

Transit is worker-confined: one thread enqueues, the same thread drains, nothing else can observe
a node. Recycling is therefore safe here for exactly the reason it is not safe on ingress.

### `QChunk`-backed transit — `TransitQueue` (superseded)

Reused one pre-allocated chunk per drain cycle: no CAS, no free list, zero steady-state
allocation. It lost to a cost it inherited rather than chose: **`QChunk.next` must be `volatile`
because the MPSC ingress queue requires it**, so linking and following an overflow chunk cost a
volatile store and load that a single-threaded queue has no use for.

The lesson generalises past this queue: **sharing a carrier type across the thread boundary
forces the strictest member's memory ordering onto every user of it.** Carriers and their
worker-confined counterparts should share a vocabulary and no representation.

### Reusable node chain — `TransitJobQueue` — **OPEN, not decided**

A chain of mutable nodes, grown to the deepest cascade seen and then reused forever. Four
interleaved reps, five forks, `-prof gc`, TCK green throughout:

| benchmark | ring | node |
|---|---:|---:|
| `cyclic_emit_deep_100k` | 13.279 | 12.500 |
| `cyclic_emit_direct` | 17.602 | 16.605 |
| `cyclic_emit_registrar` | 16.842 | 17.165 |

Lower on three rows of four, but **no row is significant**: the deltas (0.32–1.00 ns) sit inside
the within-arm ranges (1.3–6.2 ns), and `registrar` flipped sign between two reps and four.

The one signal that was not noise was allocation on deep cascades — the ring managed
**0.024 B/op with zero collections** against 0.055 B/op and five, because a node chain extends to
cascade depth while a ring does not.

**This entry is not a verdict.** `TransitQueueRing` is what is currently wired, but that was a
choice of which arm to run a benchmark suite against, not a decision between the two designs. The
timings above cannot separate them, and the allocation difference is 0.03 B/op — real, but tiny
in absolute terms and confined to deep cascades. The node version is also the more readable of
the two: its append is `writeNode = writeNode.next` with no index arithmetic and no growth check
on the common path.

Both designs stay in the tree until a measurement that can actually separate them says otherwise.
See *What an accurate transit comparison needs* below.

---

## Job

### `Job.next` is deliberately non-volatile

It was `volatile` for the queue's benefit until the resulting `StoreLoad` fence was measured at
~13% of a profile. Ordering now comes from the access mode at the use site — `NEXT.setRelease`
on publish, `NEXT.getAcquire` on read — which a `VarHandle` provides independently of how the
field is declared. **Do not restore the modifier**; it buys nothing the access modes do not
already give, and costs a fence per emission.

---

## Window backing

### Shift-backed buffer — the shipped default until measured

`WindowCountWrap` evicts with `System.arraycopy(buffer, 1, buffer, 0, n-1)` on every emission
once full — O(n) where a ring is O(1). perfasm put `oop_disjoint_arraycopy_stub` at **7.05% of
the profile at capacity 4 and 15.64% at capacity 64**, roughly 2.8 ns and 7.4 ns per emission.

The cost is dominated by the stub **call**, not the copy length, which is why a 4 → 64 capacity
sweep looks nearly flat (31.7 → 34.4 → 36.3 ns) and hides it. A capacity sweep that fails to rise
is evidence of a fixed per-call cost, not evidence of an absent one.

The same `arraycopy` shift is present in `FsOperators.Rolling`, which has no benchmark coverage.

### Ring vs intrusive node chain — a tie on append

Two O(1) backings measured against the shipped shift, one binary, one property flip:

| | legacy | ring | node |
|---|---:|---:|---:|
| `window_64` (append only) | 43.71 ± 4.48 | 34.82 ± 3.04 | 34.75 ± 4.10 |
| `window_for_each` (walks 16) | 72.87 ± 15.83 | 55.81 ± 6.24 | 60.45 ± 4.52 |

Both remove the `arraycopy` and both beat the shift by **~8.9 ns at capacity 64**. On append they
are indistinguishable — 0.07 ns apart on a ±3–4 ns measurement.

On traversal the ring leads by 4.64 ns, the direction predicted by cache density (4 bytes per
element in an `Object[]` against 24 in a chain), but the error bars overlap and it is **not** a
significant result on this hardware.

### The index mask in a field — REFUTED, and it costs a range check per element

`FsWindow.at` derives its mask as `buffer.length - 1` on every call. Holding it in a `final int`
field instead looks free — the buffer is fixed for the view's life, the field is loop-invariant,
and with compact headers it lands inside the padding the object already carries (37 bytes round
to 40, as 33 did).

It is not free, and the direction is the opposite of the intuition. The derived form is what
makes the range check **provable**: C2 types `x & (len - 1)` as `[0, len - 1]` and drops the
check against `len`. A field is opaque — nothing tells C2 it holds `len - 1` — so the check
comes back. In the compiled body of `forEach` (`CompileCommand=print`, JDK 26, C2, unrolled by
two), one element reads:

```
derived   lea (%rdx,%r9),%r8d ; mov %r11d,%esi ; and %r8d,%esi ; mov 0xc(%r10,%rsi,4),%eax
field     lea (%rdx,%r14),%edi ; mov %edi,%ecx ; and %ebx,%ecx ; cmp %r9d,%ecx ; jae <trap>
          ; mov 0xc(%r10,%rcx,4),%ecx
```

Both the mask and the array length hoist out of the loop either way, so the field saves no load
and adds a compare and a branch to **every element read** — `forEach`, `all`, `any`, `none`,
`count`, `fold`, `reduce`, and every derived view.

**How it nearly shipped, which is the more useful half.** A before/after pair of decision runs
showed most window rows faster and the change was written up as "~5% across every traversal row,
and the uniformity identifies a per-element cost". The runs were not comparable: several rows
carried errors as large as their means (`window_skip` 114.07 ± 36.51, `window_fold` 97.64 ±
49.54), and `window_size` — which returns `length` and never calls `at` — moved -32%. The
"uniformity" was machine state, not a per-element cost.

**The general rule:** a per-element claim on this box cannot be settled by a pair of JMH runs.
Read it off the compiled code or the allocation counter, where the answer does not depend on
what else the machine was doing.

---

## Worker idle policy

### Self-waking 1µs timed park — REFUTED: a core per idle circuit *and* millisecond wakes

The worker used to spin 1000 times on an empty queue and then `parkNanos(1µs)`, with no
producer-side coordination at all — "producers never pay unpark cost". Neither half of that
sentence survived measurement.

Nothing in the JMH suite could see it: every row there saturates the worker, so the idle policy
never runs. A standalone probe (process CPU accounting, plus emit-to-receptor latency where the
receptor stamps its own arrival and the producer never spins) reports both halves. One circuit,
nothing admitted, 2 vCPU box:

| worker idle policy | idle CPU | delivery after a 0.1 ms gap, p50 / p90 |
|---|---:|---|
| spin 1000 → park 1µs (shipped) | **0.95 cores** | 648 µs / 3.8 ms |
| never park (pure spin) | 0.99 cores | 0.5 µs / 1.1 µs |
| spin 0 → park 1µs | 1.05 cores | 133 µs / 3.5 ms |
| spin 0 → park 1 ms | 0.085 cores | 919 µs / 946 µs |

Three findings, and the third is the one that decided it.

- **A 1µs timed park on a virtual thread costs more CPU than spinning.** Compare rows two and
  three: parking on every idle round *raised* consumption. Each park unmounts a continuation and
  schedules a timer task, and at microsecond periods that is more work than `pause`.
- **An idle circuit consumed a core.** For a design whose doctrine is one circuit per logical
  actor, that is not a tuning defect, it is a deployment ceiling: two idle actors saturate this
  box.
- **The self-wake was also the slow one.** Because nothing unparks the worker, an emission into a
  quiet circuit waits for the timer — and the timer thread competes with the carrier the worker
  is spinning on, so the wait is 648 µs at the median and 3.8 ms at p90. The design burned a core
  *and* delivered late. Row four shows the same latency without the core, which is what any
  longer park buys when no producer wakes anyone.

### Spin, then park until told — the shipped design

The worker still spins (`SPIN_COUNT`, ~26 µs), so a circuit fed faster than the spin lasts never
parks and its producers never pay anything. When the spin finds nothing the worker publishes
`parked` and re-checks the queue; a producer reads that flag after its exchange and unparks. The
pair is a Dekker handshake — the exchange is a full fence and the worker's re-check follows an
explicit `fullFence` — so an admission cannot be missed by both sides. The 100 ms park timeout is
a safety net for a wake lost some other way, not the wake mechanism.

A wake that finds nothing re-parks rather than re-spinning, so the safety timeout costs one
loop iteration rather than a 26 µs spin every 100 ms.

Measured on the same probe:

| | before | after |
|---|---:|---:|
| idle CPU, one circuit | 0.95 cores | **0.004 cores** |
| idle CPU, four circuits | ~4 cores' worth of demand on 2 vCPUs | **0.010 cores** |
| delivery after 0.1 ms gap, p50 / p90 | 648 µs / 3.8 ms | 48 µs / 592 µs |
| delivery after 1 ms gap, p50 / p90 | 392 µs / 2.5 ms | 39 µs / **82 µs** |
| delivery after 5 ms gap, p50 / p90 | 8 µs / 1.9 ms | 39 µs / **62 µs** |
| back-to-back delivery, p50 | 0.08–0.7 µs | 0.3 µs |

The one regression is the p50 after a *long* quiet period — 8 µs → 39 µs — and it is the honest
price: a circuit that quiet is now parked, and a wake costs what a wake costs. Everything at p90
improves by 6–30×, because the old design's median was fast only while it held a core and its
tail was the timer it depended on.

**Liveness, stressed rather than argued.** With `worker.spin=0` (park on every idle round) and a
2 s safety timeout, 13,000 admissions across one and three producers were all delivered, and the
worst delivery was 4.8–12.8 ms — nowhere near the timeout, so no wake was missed. The control
that never parks at all recorded a *worse* worst case (9.5 ms) on the same box, which is what
identifies that tail as the environment — G1 and a 2 vCPU cloud VM — rather than the protocol.

The producer pays one byte load and a not-taken branch, on a cache line it already reads —
`closed` at offset 9 and `parked` at offset 10 of the circuit:

```
movzbl 0xa(%r11), %r10d ; testl %r10d, %r10d ; je <return>
```

`SPIN_COUNT` is the CPU/latency dial and is overridable
(`io.fullerstack.substrates.worker.spin`): raise it to keep a sporadic feed hot at the cost of a
core, or set it very large to restore the old always-spinning behaviour.

### The park decision reads the producer's head, not just the link

`peek()` alone cannot carry it. A producer publishes in two steps — exchange the head, then link
the previous node — and only the link is visible to `peek`. Between them the queue looks empty,
and the producer's own read of `parked` may be reordered ahead of its link on any store-buffered
machine, x86 included: a release store followed by a load is exactly the pair TSO is allowed to
reorder. Both sides could then conclude nothing was pending — the worker parks, the producer
does not wake it — and the emission would wait for the safety timeout.

The head closes it, because its write is the exchange: a locked read-modify-write, globally
ordered. If a producer's flag read preceded the worker's flag write, that producer's exchange
preceded it too, so the worker sees `head != tail` and does not park. The check is on the park
path only and costs the emission path nothing.

### What the wake path costs, and the question it leaves open

Unparking a parked thread on this box, measured in isolation (park, unpark from another thread,
timestamp the resume):

| worker thread | p50 | p90 | p99 |
|---|---:|---:|---:|
| virtual | 41 µs | 401 µs | 2.4 ms |
| platform | 19 µs | 25 µs | 1.7 ms |

Both are slow in absolute terms — this is a 2 vCPU cloud VM, where waking a descheduled thread is
expensive — but the *shape* differs: the virtual path's p90 is 16× the platform path's. Now that
a wake sits on the critical path of a sporadic feed, the worker's thread kind is a live question
rather than a free choice. It has not been decided here: it needs the same treatment this entry
gave the park, including what N platform threads cost when N circuits are open.

---

## Boundaries

### A per-emission envelope only allocates because it crosses a queue

`FsFlow.pipe(target)` terminates a chain with `v -> circuit.submit(target, v)`, which on the
worker is a transit enqueue. The emitted value is therefore queued, many can be in flight, and
each must be a distinct object.

Measured with the same window at the same capacity, differing only in where the value is
consumed:

| shape | ns/op | B/op |
|---|---:|---:|
| `window(16)` → pipe → receptor | 39.46 | **64.0** |
| `window(16).map(Window::size)` → pipe | 28.50 | **24.0** |

24 B/op is the bare ingress `Job`. Consumed inside the chain the `Window` never crosses a queue,
so escape analysis removes it — **all 40 bytes were the boundary, not the window**. Identical in
both the array and node backings, so neither is responsible for any of it.

This is why the doctrine's "express a pipeline as one chain" is a cost rule and not only a style
rule, and it should apply to every per-emission envelope the API mints — `Capture`, `Run`,
`Change` — not only to `Window`.

---

## Open questions

### What an accurate transit comparison needs

Every transit measurement so far has been taken on a 2 vCPU workspace where a batch benchmark
also spins a full core inside `circuit.await()` — 10–23% of all CPU samples — while the worker
drains on the other core and G1 collects at 1.2–2.2 GB/s. Three CPU consumers, two cores. That is
why deltas of 0.3–1.0 ns sit inside within-arm ranges of 1.3–6.2 ns, and why one row flipped sign
between reps.

A comparison that could decide it needs, at minimum:

- **More forks, not more iterations.** The variance is between forks, not within them.
- **The cascade rows only.** `cyclic_emit_deep_100k` is where the two designs actually differ —
  a node chain extends to cascade depth, a ring does not. Shallow rows measure nothing relevant.
- **Interleaved arms.** Run A/B/A/B rather than all of A then all of B, so machine drift cannot
  masquerade as an effect.
- **Nothing else touching the machine** for the duration.
- **A pre-registered margin.** State beforehand what difference would count, so a null result is
  read as a tie rather than argued into a preference.

Until that runs, the choice is open and both implementations stay.

---

## Measurement practice

- **Do not poll the machine during a run.** Interactive commands against the workspace produced
  3× iteration spikes and 5–10× variance, and inverted one result outright.
- **A capacity or size sweep that looks flat may be hiding a fixed per-call cost.** Check the
  profile, not the slope.
- **Prefer a combiner that does not allocate when measuring storage.** `rolling(n, Integer::sum, 0)`
  and `window.fold(0L, Long::sum)` box per element — 208 and 312 B/op of `valueOf` — and the row
  then measures boxing rather than the structure under test.
