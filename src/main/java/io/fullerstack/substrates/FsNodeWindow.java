package io.fullerstack.substrates;

import io.humainary.substrates.api.Substrates.NotNull;
import io.humainary.substrates.api.Substrates.Window;

import java.util.NoSuchElementException;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Predicate;

import io.humainary.substrates.api.Substrates;
import io.humainary.substrates.api.Substrates.Fault;

import static java.util.Objects.requireNonNull;

/// **FsNodeWindow** — intrusively-linked view over a stable emission set.
///
/// The measured alternative to [FsWindow]'s array backing. Same `Window<E>` contract, same
/// [FsWindow.Lease] semantics, same 40-byte view object — the only difference is where the
/// values live: a fixed circular doubly-linked chain of [Node], built once per materialisation
/// and cycled, instead of an `Object[]`.
///
/// ## Why this exists
///
/// It is the shape the ingress and transit queues settled on, and the question it answers is
/// whether that carries over to a window. It does not obviously: a queue's node is *mandatory*
/// (a producer handing off a receiver/value pair has to allocate a carrier either way), whereas
/// a window element is a bare value that an `Object[]` stores in four bytes. Here the node is
/// pure overhead — 24 bytes per element against 4 — and the window's hot operations are
/// sequential walks over every element. That is a prediction about cache density, and a
/// prediction is worth exactly what the benchmark says it is.
///
/// ## Representation
///
/// `(head, tail, length, reversed)`, where `head`/`tail` bound the view inclusively and
/// `reversed` selects the direction encounter order walks:
/// - `reversed=false` — `head` → `tail` following `next`
/// - `reversed=true`  — `tail` → `head` following `prev`
///
/// The chain is doubly linked precisely so `reverse()` stays a field flip rather than a copy;
/// that is the extra four bytes a node costs over a singly-linked one.
///
/// Terminal operations walk the chain directly rather than going through positional access —
/// an indexed `at(i)` on a list is O(i), so a traversal built on it would be O(n²). Positional
/// access survives only where the view algebra genuinely needs it, and there it is paid once
/// per view construction rather than once per element.
///
/// Restriction operations produce new views over the **same chain** — no value copies,
/// allocation only of the new view object, exactly as [FsWindow] does.
@SuppressWarnings ( "unchecked" )
final class FsNodeWindow < E > implements Window < E > {

  /// A slot in the chain. Mutable and recycled: the chain is allocated once per
  /// materialisation and never grows, so steady-state emission allocates only the view.
  static final class Node {
    Object value;
    Node   next;
    Node   prev;
  }

  /// Builds a circular doubly-linked chain of `size` nodes and returns its first node.
  static Node chain ( int size ) {
    final Node first = new Node ();
    Node       last  = first;
    for ( int i = 1; i < size; i++ ) {
      final Node node = new Node ();
      node.prev = last;
      last.next = node;
      last      = node;
    }
    last.next  = first;
    first.prev = last;
    return first;
  }

  private final Node             head;
  private final Node             tail;
  private final int              length;
  private final boolean          reversed;
  private final FsWindow.Lease   lease;
  private final long             generation;

  FsNodeWindow ( Node head, Node tail, int length, boolean reversed,
                 FsWindow.Lease lease, long generation ) {
    this.head       = head;
    this.tail       = tail;
    this.length     = length;
    this.reversed   = reversed;
    this.lease      = lease;
    this.generation = generation;
  }

  /// A restriction of this window: same chain, same lease, same generation.
  private FsNodeWindow < E > view ( Node head, Node tail, int length, boolean reversed ) {
    return new FsNodeWindow <> ( head, tail, length, reversed, lease, generation );
  }

  /// §6.4.1: every operator entry signals when the lease no longer holds.
  private void requireLease ( String operation ) {
    if ( !lease.valid ( generation ) ) {
      throw new Fault ( Substrates.cortex ().current ().subject (), operation,
        "window used outside the callback that produced it; §6.4 makes a window "
        + "callback-scoped and callers needing values beyond it MUST copy them" );
    }
  }

  private static Node forward ( Node node, int steps ) {
    for ( int i = 0; i < steps; i++ ) node = node.next;
    return node;
  }

  private static Node backward ( Node node, int steps ) {
    for ( int i = 0; i < steps; i++ ) node = node.prev;
    return node;
  }

  /// First node of encounter order.
  private Node origin () {
    return reversed ? tail : head;
  }

  /// Successor of `node` in encounter order.
  private Node step ( Node node ) {
    return reversed ? node.prev : node.next;
  }

  // ─── Terminal operations ──────────────────────────────────────────────────

  @Override
  public int size () {
    requireLease ( "size" );
    return length;
  }

  @Override
  public boolean isEmpty () {
    requireLease ( "isEmpty" );
    return length == 0;
  }

  @NotNull
  @Override
  public E first () {
    requireLease ( "first" );
    if ( length == 0 ) throw new NoSuchElementException ( "empty window" );
    return (E) origin ().value;
  }

  @NotNull
  @Override
  public E last () {
    requireLease ( "last" );
    if ( length == 0 ) throw new NoSuchElementException ( "empty window" );
    return (E) ( reversed ? head : tail ).value;
  }

  @Override
  public void forEach ( @NotNull Consumer < ? super E > action ) {
    requireLease ( "forEach" );
    requireNonNull ( action, "action" );
    Node node = origin ();
    for ( int i = 0; i < length; i++ ) {
      action.accept ( (E) node.value );
      node = step ( node );
    }
  }

  @Override
  public boolean all ( @NotNull Predicate < ? super E > predicate ) {
    requireLease ( "all" );
    requireNonNull ( predicate, "predicate" );
    Node node = origin ();
    for ( int i = 0; i < length; i++ ) {
      if ( !predicate.test ( (E) node.value ) ) return false;
      node = step ( node );
    }
    return true;
  }

  @Override
  public boolean any ( @NotNull Predicate < ? super E > predicate ) {
    requireLease ( "any" );
    requireNonNull ( predicate, "predicate" );
    Node node = origin ();
    for ( int i = 0; i < length; i++ ) {
      if ( predicate.test ( (E) node.value ) ) return true;
      node = step ( node );
    }
    return false;
  }

  @Override
  public boolean none ( @NotNull Predicate < ? super E > predicate ) {
    requireLease ( "none" );
    requireNonNull ( predicate, "predicate" );
    Node node = origin ();
    for ( int i = 0; i < length; i++ ) {
      if ( predicate.test ( (E) node.value ) ) return false;
      node = step ( node );
    }
    return true;
  }

  @Override
  public int count ( @NotNull Predicate < ? super E > predicate ) {
    requireLease ( "count" );
    requireNonNull ( predicate, "predicate" );
    Node node  = origin ();
    int  total = 0;
    for ( int i = 0; i < length; i++ ) {
      if ( predicate.test ( (E) node.value ) ) total++;
      node = step ( node );
    }
    return total;
  }

  @Override
  public < R > R fold ( @NotNull R seed,
                        @NotNull BiFunction < ? super R, ? super E, ? extends R > op ) {
    requireLease ( "fold" );
    requireNonNull ( seed, "seed" );
    requireNonNull ( op,   "op" );
    Node node = origin ();
    R    acc  = seed;
    for ( int i = 0; i < length; i++ ) {
      acc  = op.apply ( acc, (E) node.value );
      node = step ( node );
    }
    return acc;
  }

  @Override
  public E reduce ( @NotNull E identity, @NotNull BinaryOperator < E > op ) {
    requireLease ( "reduce" );
    requireNonNull ( identity, "identity" );
    requireNonNull ( op,       "op" );
    Node node = origin ();
    E    acc  = identity;
    for ( int i = 0; i < length; i++ ) {
      acc  = op.apply ( acc, (E) node.value );
      node = step ( node );
    }
    return acc;
  }

  // ─── Restriction operations (return views; no value copies) ────────────────

  @NotNull
  @Override
  public Window < E > prefix ( int count ) {
    requireLease ( "prefix" );
    if ( count < 0 ) throw new IllegalArgumentException ( "count must be >= 0" );
    if ( count >= length ) return this;
    if ( count == 0 ) return view ( head, tail, 0, reversed );
    // First `count` of encounter order.
    if ( reversed ) return view ( backward ( tail, count - 1 ), tail, count, true );
    return view ( head, forward ( head, count - 1 ), count, false );
  }

  @NotNull
  @Override
  public Window < E > suffix ( int count ) {
    requireLease ( "suffix" );
    if ( count < 0 ) throw new IllegalArgumentException ( "count must be >= 0" );
    if ( count >= length ) return this;
    if ( count == 0 ) return view ( head, tail, 0, reversed );
    // Last `count` of encounter order.
    if ( reversed ) return view ( head, forward ( head, count - 1 ), count, true );
    return view ( backward ( tail, count - 1 ), tail, count, false );
  }

  @NotNull
  @Override
  public Window < E > skip ( int count ) {
    requireLease ( "skip" );
    if ( count < 0 ) throw new IllegalArgumentException ( "count must be >= 0" );
    if ( count == 0 ) return this;
    if ( count >= length ) return view ( head, tail, 0, reversed );
    // Drop first `count` of encounter order.
    if ( reversed ) return view ( head, backward ( tail, count ), length - count, true );
    return view ( forward ( head, count ), tail, length - count, false );
  }

  @NotNull
  @Override
  public Window < E > trim ( int count ) {
    requireLease ( "trim" );
    if ( count < 0 ) throw new IllegalArgumentException ( "count must be >= 0" );
    if ( count == 0 ) return this;
    if ( count >= length ) return view ( head, tail, 0, reversed );
    // Drop last `count` of encounter order.
    if ( reversed ) return view ( forward ( head, count ), tail, length - count, true );
    return view ( head, backward ( tail, count ), length - count, false );
  }

  @NotNull
  @Override
  public Window < E > slice ( int offset, int count ) {
    requireLease ( "slice" );
    if ( offset < 0 ) throw new IllegalArgumentException ( "offset must be >= 0" );
    if ( count  < 0 ) throw new IllegalArgumentException ( "count must be >= 0" );
    if ( offset >= length ) return view ( head, tail, 0, reversed );
    final int effective = Math.min ( count, length - offset );
    if ( offset == 0 && effective == length ) return this;
    if ( effective == 0 ) return view ( head, tail, 0, reversed );
    // `effective` values starting at encounter-order index `offset`.
    if ( reversed ) {
      final Node last = backward ( tail, offset );
      return view ( backward ( last, effective - 1 ), last, effective, true );
    }
    final Node start = forward ( head, offset );
    return view ( start, forward ( start, effective - 1 ), effective, false );
  }

  @NotNull
  @Override
  public Window < E > reverse () {
    requireLease ( "reverse" );
    return view ( head, tail, length, !reversed );
  }
}
