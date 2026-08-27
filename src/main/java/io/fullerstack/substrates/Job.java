package io.fullerstack.substrates;

import java.util.function.Consumer;

/// One unit of queued work, and its own queue node.
///
/// A single carrier serves both queues. They differ in how they *use* it, not in what it is:
///
/// - [JobQueue] allocates one per admission and discards it. A carrier crossing the thread
///   boundary cannot be recycled — a producer can claim a node the consumer has already freed,
///   which is the ABA that lost emissions and left `await` parked. See `docs/DECISIONS.md`.
/// - `TransitJobQueue` builds a chain once, grows it to the deepest cascade seen, and overwrites
///   the slots forever. It is worker-confined, so nothing can observe a node mid-reuse.
///
/// **Recycling policy belongs to the queue, not to the carrier**, which is why one type is
/// enough. An earlier design had four — an abstract base, a receptor subclass, a marker
/// subclass and a separate transit node — on the reasoning that the two queues had different
/// memory obligations. They do not: the obligations live at the access site, below.
///
/// Nothing here is polymorphic. Markers are distinguished by [#marker] rather than by a
/// subclass, so a drain reads a field instead of dispatching a virtual `run` that the JIT was
/// declining to inline.
final class Job {

  /// The receptor this work delivers to.
  ///
  /// Not `final`, because the transit chain overwrites it. That costs nothing in publication
  /// safety: the producer writes this before `HEAD.getAndSet`, which is a full fence, and the
  /// consumer reaches the node only through an acquire load of [#next] that pairs with the
  /// producer's release. The release/acquire pair carries these fields across, not a final-field
  /// freeze.
  Consumer < Object > receiver;

  /// The emission being delivered. Nullable — markers carry a barrier, or nothing.
  Object value;

  /// Whether this is an await/close marker rather than user work.
  ///
  /// A marker skips the §15.4 isolation wrapper and the transit drain, and it is rare: the type
  /// profile of a batch run put markers at 33 admissions in 361,719. A predicted branch on the
  /// common path is the cheaper way to express something that never happens.
  boolean marker;

  /// The following node in FIFO order. Deliberately **not** `volatile`.
  ///
  /// Ordering comes from the access mode at the point of use, not from this declaration.
  /// [JobQueue] publishes with `NEXT.setRelease` and reads with `NEXT.getAcquire`; the transit
  /// chain is single-threaded and uses plain access. A modifier would force the stronger of the
  /// two on both.
  ///
  /// Declaring it `volatile` cost a StoreLoad fence on every plain-source store —
  /// `lock addl $0, (%rsp)` per item in the unlink — and perfasm measured atomics and fences at
  /// 40.89% of one profile against 8.22% once the modifier was removed.
  Job next;
}
