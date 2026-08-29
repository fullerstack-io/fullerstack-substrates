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

## Operators

### `every(n)`: a countdown, and a hardware divide that cost nothing

`Every.accept` was `if (++count % n == 0)`. Two things were wrong with it and only one of them
was what it looked like.

**The real defect is overflow.** `count` grew without bound, so at 2^31 admissions it wrapped
negative and the interval's phase shifted by one. At this implementation's rates that is about
twenty seconds of saturated emission, not a theoretical horizon. A countdown cannot overflow.

**The speed argument was refuted by its own measurement.** `n` is a non-constant field, so C2 had
nothing to strength-reduce against and emitted the full sequence — a divide-by-zero test, the
`INT_MIN / -1` special case, then `cltd; idivl`. Confirmed in the compiled body, and `idivl` goes
2 -> 0 with the countdown. The prediction was ~5 ns of `every_batch`'s 17 ns, since a Zen 3
32-bit divide is around fourteen cycles and the operator's whole budget over baseline was 5.6 ns.

Measured A/B/A, same session, with two matched controls:

| ns/op | new-1 | old-1 | new-2 |
|---|---:|---:|---:|
| `every_batch` | 17.689 ± 1.35 | 18.104 ± 1.31 | 17.760 ± 1.24 |
| `every_duration_batch` (control) | 41.189 | 41.957 | 41.068 |
| `baseline_plain_batch` (control) | 12.611 | 12.358 | 12.209 |

**0.4 ns, inside the error bars.** The controls did not move, so the comparison was sound and the
prediction was simply wrong.

The mechanism of the miss is worth keeping: the divide feeds a branch that `every(2)` makes
perfectly predictable, so the core speculates past it and the fourteen cycles retire in the shadow
of the emit-and-hop path that actually bounds the row. **Latency on a dependency chain nothing
waits on is free.** Counting instructions — even expensive, unmistakable ones — does not predict
time; only the critical path does. Compare the `awaitImpl` spin, where 10-23% of profile samples
also turned out to cost no wall-clock for the same class of reason.

The change is kept for the overflow fix and because it is strictly less work, not for a speed
claim that did not survive.

## Attachment

### A materialised pipe's subject is derived on demand, not at attachment — 48 B/op

`Flow.pipe(target)` and `Fiber.pipe(target)` return a materialised pipe whose §4.3 enclosure is
the pipe it feeds. Both built that subject eagerly, at attachment, whether or not anything would
ever read it: an `FsSubject` (32 B) holding an `FsId` (16 B), the id coming from a process-global
`AtomicLong.getAndIncrement`. Because the subject is stored into the returned pipe it escapes, so
no escape analysis can remove it.

`FsPipe` already had the lazy machinery — `subject()` derives under a monitor and caches — so the
change is to record the *parent* rather than the finished subject, and let `derive()` hang the
subject from it on first ask. The recorded-enclosure branch must LEAD in `derive()`, or a
materialised pipe falls through to the circuit's subject as parent and the §4.3 path flattens.

Measured with `-prof gc`, before -> after, every attachment row in the suite:

| row | before | after |
|---|---:|---:|
| `FlowOps.pipe_create_map` | 104.00 | **56.00** |
| `FiberOps.pipe_create_fiber` | 104.00 | **56.00** |
| `ScanOps.pipe_create_scan` | 112.00 | **64.00** |
| `RelateOps.pipe_create_relate` | 112.00 | **64.00** |
| `RunChangeOps.pipe_create_run` / `_change` | 120.00 | **72.00** |
| `ScanOps.pipe_create_flow_factory` | 152.00 | **104.00** |
| `FlowOps.pipe_create_fiber_factory` | 152.00 | **104.00** |
| `WindowOps.pipe_create_window` | 248.00 | **200.00** |
| `WindowOps.pipe_create_window_duration` | 400.00 | **352.00** |

Exactly 48 on all ten, which is its own check: `FsPipe` gained a field, and had that pushed the
object into another size class the delta would have read 40. The fourth oop landed in padding the
object already carried.

**Why this was the riskiest item in the plan, and why it passed.** Deferring the subject changes
*when* ids are minted, so the global id sequence differs from eager minting even though each
chain's shape does not. `FlowContractTest` walks nested pipe subjects
(`map_chainedPipes_nestSubjects`): under lazy derivation, asking the head for its subject forces
the middle, which forces the tail, so the identical chain is built in the identical order, only
later. TCK 960/0/0 and 1227/0/0.

While here, `FsFlow.pipe` stopped calling `target.subject()` a second time when it already held
the value in a local — javap had the duplicate invokeinterface at bci 35 and bci 125.

### Two candidates declined without building them

- **Collapsing `Diff`/`Heartbeat`'s `has` flag into a null check on `prev`.** The invariant is real
  (`has == (prev != null)` on every reachable path, emissions being non-null per §1.2) and it
  removes one field load per emission. Declined on this cycle's own evidence: removing a
  **fourteen-cycle hardware divide** from `Every` moved its row 0.4 ns, inside the error bars. A
  one-cycle load cannot matter where a divide did not, and it would trade a local invariant for a
  global one — a user type violating `equals(null) == false` would silently lose its first
  emission. `Change.has` and `SteadyPredicate.has` are not collapsible in any case: the first has
  a key function that may legitimately return null, the second starts with a null `prev`.
- **`Rolling` reading the ring directly instead of through `at()`.** Its author pre-registered the
  prediction as EXACTLY ZERO — the fixture's combiner is a static method reference that inlines,
  so C2 already hoists what the change would hoist by hand. A pre-registered zero is a result.

## Window layout

### The ring reference belongs to the lease, not to every view — 40 B to 32 B

A `Window` is minted per emission and again per restriction, while the ring it views over is a
property of the *stage*: one materialisation, one `DelayLine`, one `WindowLease`. `FsWindow` was
carrying its own reference to that one array, which under compact object headers is what pushed
its fields from 29 bytes to 33 — and 33 pads to 40 where 29 pads to 32. Moving the reference into
the lease, which every view already holds, takes 8 bytes off every window the circuit emits. The
lease absorbs it free: 20 bytes padded to 24 becomes 24, and it is allocated once per stage.

Measured with `-prof gc`, before and after, whole numbers:

| row class | before | after |
|---|---:|---:|
| single window (13 rows) | 64.00 | **56.00** |
| derived view (`prefix`/`suffix`/`slice`/`skip`/`trim`) | 104.00 | **88.00** |
| `filter_then_window` (half the emissions survive) | 44.00 | **40.00** |
| `fold` / `reduce` (the rest is the benchmark's boxing) | 272.00 | **264.00** |
| `pipe_create_window` — MATCHED CONTROL | 248.00 | 248.00 |
| `pipe_create_window_duration` — MATCHED CONTROL | 400.00 | 400.00 |

The derived-view rows fall by 16 rather than 8 because both windows shrink — JFR names
`FsWindow.view` as a second real 40-byte site. The two attachment rows are the control: they mint
a lease and a line per operation but no window, and the lease did not grow, so they must not move.
They did not.

**The risk this carried, and why it did not fire.** `at()` now reaches the ring through one more
indirection — `this.lease` then `lease.buffer` — which on a sixteen-element traversal would cost
far more than the 8 bytes are worth if it happened per element. It does not. Both fields are
final, so C2 hoists them: the compiled `forEach` loads `lease.buffer` and `buffer.length` once
before the loop and the per-element body is `leal / andl / movl`, unchanged, still with no bounds
check. That reading was the gate on this change, not the byte count.

No timing claim is made and none should be. 40 bytes of queued object cost 2.26 ns on this box
(`WindowBoundaryOps`), so 8 bytes is worth about 0.45 ns — below anything this machine can
resolve. The result is 12.5% less allocation per window emission, which is a GC-pressure result,
not a latency one.

**The regression scare, and what it cost to settle.** Two `-prof gc` runs taken hours apart showed
the window rows 7-16% slower after the change, which would have outweighed the allocation win
several times over. It was noise. An interleaved A/B/A against the parent commit:

| ns/op | new-1 | old-1 | new-2 |
|---|---:|---:|---:|
| `window_16_batch` | 30.429 | 31.527 | 34.093 |
| `window_fold_batch` | 82.276 | 79.036 | 91.066 |
| `window_for_each_batch` | 51.644 | 61.630 | 58.642 |
| `window_size_batch` | 32.964 | 39.536 | 39.496 |

On every row the two *same-arm* runs differ by more than either differs from the old arm, and the
sequence drifts monotonically slower across the whole run. The arms cannot be separated. This is
the third time in one session that a pair of runs taken minutes or hours apart manufactured a
double-digit effect that an interleaved comparison dissolved — the first killed a mask field, the
second nearly killed this change. **On this machine a cross-run pair is not evidence, whatever the
percentage says.**

## Ring index masks

### The derived-mask rule does NOT generalise to the retention or transit rings — REFUTED

`FsWindow.at` derives its mask as `buffer.length - 1` because that is what lets C2 prove the index
in range and drop the bounds check; holding the identical number in a field costs a `cmp/jae` per
element (commit c37ff14). `DelayLine` and `TransitQueueRing` both hold a `mask` field, so applying
the same rule to them looks like housekeeping. It was built, TCK-verified green, and measured — and
it buys nothing at either site.

What actually happens is that the guard **changes shape rather than disappearing**. Compiled
`DelayLine.at`, standalone:

```
field    add / and(field) / load values / load len / cmpl len,idx / jae   -> 7 insns
derived  load values / load len / add / leal -1 / and / testl len,len / jbe -> 8 insns
```

C2 does tie the mask to the array and drop the *index* comparison, but it cannot prove the array
non-empty — a zero-length array would make the mask `-1` and the index unbounded — so it emits a
length-zero guard in its place. One fused compare-and-branch either way, plus an extra `leal`.

Inlined into `FsOperators$Rolling.accept`, which is the shape that matters, the fold loop reads
`and / cmpl len,idx / jae` before and `and / testl len,len / jbe` after: same instruction count,
same fused pair, the only difference being that the surviving branch is loop-invariant rather than
index-dependent. Both are never taken and both predict perfectly.

`TransitQueueRing` is the same result with the sign made obvious by size:

| | before | after |
|---|---|---|
| `enqueue` | 9 `jae`, 0 `jbe`, 809 insns | 2 `jae`, 6 `jbe`, **823** insns |
| `drain` | 7 `jae`, 0 `jbe`, 613 insns | 2 `jae`, 4 `jbe`, **619** insns |

Seven index checks became six length guards and the body grew by fourteen instructions. Part of
that is inherent to the ring: it indexes two parallel arrays, so masking each with its own length
costs an extra `and` where one shared mask served both.

**Why `FsWindow` is different, which is the transferable part.** There the view's own `length` field
bounds the loop (`i < length`) and the buffer is a final field of a power-of-two ring, so C2 has
everything it needs to fold the check away entirely. In the rings the array reference is mutable
(`TransitQueueRing.grow` replaces both arrays) or the emptiness is simply not provable, and a
derived mask can only convert an index check into a length check.

**The rule is therefore about what C2 can prove at a particular site, not about where masks live.**
Do not apply it anywhere else without reading the compiled body first; "it worked in FsWindow" is
not an argument. Both edits were reverted. TCK was 960/0/0 and 1227/0/0 throughout, so this is a
performance refutation and nothing more.

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
price: a circuit that quiet is now parked, and a wake costs what a wake costs.

**Read the p90 column with its spread.** The p50s above are stable across repeats — 39–53 µs
after an idle gap, against 392–648 µs before — but p90 is not: repeating the identical build
minutes later gave 1.20 ms and 1.53 ms where the table shows 592 µs and 82 µs. What survives
repetition is that the tail moved from milliseconds-always to milliseconds-sometimes, on a box
whose own GC and hypervisor produce multi-millisecond stalls in a control that never parks at all
(9.5 ms, above). A tighter claim than that needs a quieter machine.

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

### The awaiter's spin: worth having, at a twentieth of its length

`AwaitOps` exists because every other class ends an invocation with one `await` amortised over
thousands of emissions, which can only ever show the spin's cost and never its benefit. Swept in
one session, three forks per arm, on those rows plus the cheapest batch rows — where an awaiter
spinning through a 10,000-item drain would surface if it surfaced anywhere:

| ns/op | spin 1000 | spin 50 | spin 0 |
|---|---:|---:|---:|
| `await_empty` | 87.4 | 95.2 | **614.3** |
| `await_one` | 103.1 | 102.6 | **668.2** |
| `await_after_batch` | 13464 | 13055 | 15487 |
| `async_emit_batch` | 11.64 | 11.03 | 11.32 |
| `async_emit_admission_batch` | 10.26 | 10.10 | 9.82 |
| `empty_emit_batch` | 11.61 | 11.61 | 11.81 |

50 and 1000 are the same measurement on every row; 0 is 6-7× worse on the tight rows. A marker on
a running worker fires in about a hundred nanoseconds — inside the first handful of iterations —
and behind a drain no spin catches it at all. The budget therefore wants to be just longer than
the fire time: **the default is now 50**, which keeps the win and spends a twentieth of the CPU
when there is none to be had.

**Two earlier readings this supersedes, and one refuted prediction.**

- The sweep that chose 1000 found a cliff there, but it was measuring the old worker, which
  self-woke on a 1µs timer: a marker on a quiet circuit took microseconds to fire rather than the
  ~100 ns a running worker takes now. The cliff was a property of the design that has since gone.
- perfasm put `awaitImpl` at 10-23% of samples across the batch families. True, and **not** a
  wall-clock cost: the awaiter is the producer thread, idle during the drain, so on a two-core
  box it burns its own core rather than the worker's. The prediction that a shorter spin would
  return 5-20% to the batch rows is refuted by the flat rows above — the correct instrument for
  "does this cost time" was never the sample profile.
- The awaiter's park round trip measured **~520 ns** here, from the `spin 0` arm — not the
  19-41µs an isolated park/unpark probe reported. That probe slept a millisecond between samples
  and paid for waking descheduled CPUs; a warm handoff is two orders of magnitude cheaper. Wake
  cost is not one number, and which one applies depends on whether the machine is warm.

### What the wake path costs, and the question it leaves open

Unparking a parked thread on this box, measured in isolation (park, unpark from another thread,
timestamp the resume):

| worker thread | p50 | p90 | p99 |
|---|---:|---:|---:|
| virtual | 41 µs | 401 µs | 2.4 ms |
| platform | 19 µs | 25 µs | 1.7 ms |

**These are cold figures and must not be read as the price of a wake.** The probe sleeps a
millisecond between samples, so each one pays to wake CPUs the OS has already let go idle. A warm
handoff is two orders of magnitude cheaper: the awaiter's park round trip read **~520 ns** off the
`spin 0` arm of the await sweep, on a machine that was busy. The number that matters for a
sporadic feed is the circuit's own after-idle delivery, measured above at ~39-48 µs p50 with a
virtual worker.

A first warm data point, same probe and same session, says the switch is not the free win the
cold table suggests — it is a trade, and the half it loses is the half everything else measures:

| | virtual worker | platform worker |
|---|---:|---:|
| idle CPU | 0.010 cores | 0.000 cores |
| back-to-back delivery, p50 | **107 ns** | 7,654 ns |
| after 0.1 ms gap, p50 / p90 | 52.9 µs / 1.20 ms | **8.5 µs / 15.3 µs** |
| after 1 ms gap, p50 / p90 | 43.4 µs / 1.53 ms | **16.7 µs / 38.8 µs** |
| after 5 ms gap, p50 / p90 | 38.6 µs / 714 µs | **23.8 µs / 467 µs** |

The platform worker wakes 2-6× faster and far more steadily — its p90 after an idle gap is tens of
microseconds where the virtual worker's is over a millisecond. It also delivers a *burst* far
worse: 7.7 µs against 107 ns back-to-back, which is the shape every benchmark row in the suite
measures and the one the library leads with. Whether that gap is per-emission cost or queueing
that the burst inherits from a slow first wake has not been separated, and it must be before this
question can be reopened.

What the table does support is a difference in *shape*: the virtual path's p90 is 16× the
platform path's on the same cold probe. Now that a wake sits on the critical path of a sporadic
feed, the worker's thread kind is a live question rather than a free choice. It has not been
decided here: it needs the same treatment this entry gave the park — a warm measurement of
after-idle delivery on both, the hot rows re-run, and what N platform threads cost when N
circuits are open.

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
