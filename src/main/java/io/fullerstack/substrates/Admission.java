package io.fullerstack.substrates;

import java.util.function.Consumer;

/// One emission admitted to a circuit, and its own queue node.
///
/// Used only by [IngressQueue], which allocates one per admission and discards it. A carrier
/// crossing the thread boundary cannot be recycled — a producer can claim a node the consumer has
/// already freed, which is the ABA that lost emissions and left `await` parked. See
/// `docs/DECISIONS.md`.
///
/// [TransitQueue] mints nothing: it is worker-confined, so it holds the same `(receiver, value)`
/// pair inline across two parallel arrays and needs no node at all. A node-based transit queue was
/// built and measured against it — the timings tied and the ring won on allocation, so it was
/// deleted rather than left in the tree as a fifth buffer implementation.
///
/// An earlier design had four carrier types — an abstract base, a receptor subclass, a marker
/// subclass and a separate transit node — on the reasoning that the queues had different memory
/// obligations. They do not: the obligations live at the access site, below.
///
/// Nothing here is polymorphic. Markers are distinguished by [#marker] rather than by a
/// subclass, so a drain reads a field instead of dispatching a virtual `run` that the JIT was
/// declining to inline.
final class Admission {

  /// The receptor this emission delivers to.
  ///
  /// Not `final`: publication rests on the access mode rather than on a final-field freeze, so
  /// nothing here depends on it. That costs nothing in publication safety: the producer writes this before `HEAD.getAndSet`, which is a full fence, and the
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
  /// [IngressQueue] publishes with `NEXT.setRelease` and reads with `NEXT.getAcquire`; the transit
  /// chain is single-threaded and uses plain access. A modifier would force the stronger of the
  /// two on both.
  ///
  /// Declaring it `volatile` cost a StoreLoad fence on every plain-source store —
  /// `lock addl $0, (%rsp)` per item in the unlink — and perfasm measured atomics and fences at
  /// 40.89% of one profile against 8.22% once the modifier was removed.
  Admission next;
}
