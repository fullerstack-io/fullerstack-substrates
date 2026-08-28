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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import static java.util.Objects.requireNonNull;

/// **WindowView** — the `Window` implementation: a strided view over a [DelayLine]'s ring.
///
/// §6.2.3 emits a window on every accepted input, and the spec is explicit that this is a view
/// rather than a copy — "emitted windows are temporal views over a worker-thread-local ring".
/// Nothing is copied here: a view is `(buffer, start, length, reversed)`, and every restriction
/// (`prefix`, `suffix`, `skip`, `trim`, `slice`, `reverse`) computes another view over the same
/// storage.
///
/// ## Encounter order
///
/// Index `i` maps to a physical slot as:
/// - `reversed = false` — `buffer[ start + i ]`
/// - `reversed = true`  — `buffer[ start + length - 1 - i ]`
///
/// both taken modulo the buffer, because the ring's `start` wraps. See [#at].
///
/// ## Lifetime
///
/// A view is valid only inside the callback that received it (§6.4). §6.4.1 requires that to be
/// detected rather than left undefined, by name and with the cost argument spelled out —
/// "detection via a per-operator lease check imposes one branch per operator entry". Every
/// operator below opens with that check; [WindowLease] is the mechanism, and a derived view
/// inherits its root's lease and stamp so a whole family expires together.
@SuppressWarnings ( "unchecked" )
final class WindowView < E > implements Window < E > {

  private final Object[] buffer;
  private final int      start;
  private final int      length;
  private final boolean  reversed;
  private final WindowLease lease;

  /// The generation this window was minted in. A derived view inherits its root's, so a
  /// whole family of views expires together with the callback that produced the root.
  private final long     generation;

  WindowView ( Object[] buffer, int start, int length, boolean reversed, WindowLease lease, long generation ) {
    this.buffer     = buffer;
    this.start      = start;
    this.length     = length;
    this.reversed   = reversed;
    this.lease      = lease;
    this.generation = generation;
  }

  /// A restriction of this window: same buffer, same lease, same generation. Sharing the
  /// stamp is the point — a view must not outlive the callback its root belongs to.
  private WindowView < E > view ( int start, int length, boolean reversed ) {
    return new WindowView <> ( buffer, start, length, reversed, lease, generation );
  }

  /// §6.4.1: every operator entry — on the root window and on every derived
  /// view alike — signals when the lease no longer holds.
  private void requireLease ( String operation ) {
    if ( !lease.valid ( generation ) ) {
      // §15.3: name the context that made the illegal call rather than describing it. The
      // `Current` lookup is affordable here and nowhere else — this path has already failed.
      throw new Fault ( Substrates.cortex ().current ().subject (), operation,
        "window used outside the callback that produced it; §6.4 makes a window "
        + "callback-scoped and callers needing values beyond it MUST copy them" );
    }
  }

  /// Encounter-order accessor.
  ///
  /// The physical index is taken modulo the buffer, which is why every buffer handed to this
  /// type MUST have a power-of-two length (`FsFlow.physical`). Two reasons, and the second is
  /// the one that pays: a ring-backed window presents a `start` that wraps, so the modulo is
  /// load-bearing there; and on every backing strategy the masked index is *provably* within
  /// the array, so C2 drops the bounds check it cannot drop for a bare `start + idx`.
  ///
  /// `buffer.length` is not an extra load — the bounds check needs the array length anyway,
  /// so the mask reuses a value already in a register.
  private E at ( int idx ) {
    final int mask = buffer.length - 1;
    if ( reversed ) {
      return (E) buffer[ ( start + length - 1 - idx ) & mask ];
    }
    return (E) buffer[ ( start + idx ) & mask ];
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
    return at ( 0 );
  }

  @NotNull
  @Override
  public E last () {
    requireLease ( "last" );
    if ( length == 0 ) throw new NoSuchElementException ( "empty window" );
    return at ( length - 1 );
  }

  @Override
  public void forEach ( @NotNull Consumer < ? super E > action ) {
    requireLease ( "forEach" );
    requireNonNull ( action, "action" );
    for ( int i = 0; i < length; i++ ) action.accept ( at ( i ) );
  }

  @Override
  public boolean all ( @NotNull Predicate < ? super E > predicate ) {
    requireLease ( "all" );
    requireNonNull ( predicate, "predicate" );
    for ( int i = 0; i < length; i++ ) {
      if ( !predicate.test ( at ( i ) ) ) return false;
    }
    return true;
  }

  @Override
  public boolean any ( @NotNull Predicate < ? super E > predicate ) {
    requireLease ( "any" );
    requireNonNull ( predicate, "predicate" );
    for ( int i = 0; i < length; i++ ) {
      if ( predicate.test ( at ( i ) ) ) return true;
    }
    return false;
  }

  @Override
  public boolean none ( @NotNull Predicate < ? super E > predicate ) {
    requireLease ( "none" );
    requireNonNull ( predicate, "predicate" );
    for ( int i = 0; i < length; i++ ) {
      if ( predicate.test ( at ( i ) ) ) return false;
    }
    return true;
  }

  @Override
  public int count ( @NotNull Predicate < ? super E > predicate ) {
    requireLease ( "count" );
    requireNonNull ( predicate, "predicate" );
    int c = 0;
    for ( int i = 0; i < length; i++ ) {
      if ( predicate.test ( at ( i ) ) ) c++;
    }
    return c;
  }

  @Override
  public < R > R fold ( @NotNull R seed,
                        @NotNull BiFunction < ? super R, ? super E, ? extends R > op ) {
    requireLease ( "fold" );
    requireNonNull ( seed, "seed" );
    requireNonNull ( op,   "op" );
    R acc = seed;
    for ( int i = 0; i < length; i++ ) acc = op.apply ( acc, at ( i ) );
    return acc;
  }

  @Override
  public E reduce ( @NotNull E identity, @NotNull BinaryOperator < E > op ) {
    requireLease ( "reduce" );
    requireNonNull ( identity, "identity" );
    requireNonNull ( op,       "op" );
    E acc = identity;
    for ( int i = 0; i < length; i++ ) acc = op.apply ( acc, at ( i ) );
    return acc;
  }

  // ─── Restriction operations (return views; no value copies) ────────────────

  @NotNull
  @Override
  public Window < E > prefix ( int count ) {
    requireLease ( "prefix" );
    if ( count < 0 ) throw new IllegalArgumentException ( "count must be >= 0" );
    if ( count >= length ) return this;
    // First `count` elements of encounter order.
    if ( reversed ) {
      // Reversed encounter visits buffer[start+length-1 .. start]; the first
      // `count` of that is buffer[start+length-count .. start+length-1].
      return view ( start + length - count, count, true );
    }
    return view ( start, count, false );
  }

  @NotNull
  @Override
  public Window < E > suffix ( int count ) {
    requireLease ( "suffix" );
    if ( count < 0 ) throw new IllegalArgumentException ( "count must be >= 0" );
    if ( count >= length ) return this;
    // Last `count` elements of encounter order.
    if ( reversed ) {
      // Last `count` of reversed = buffer[start .. start+count-1].
      return view ( start, count, true );
    }
    return view ( start + length - count, count, false );
  }

  @NotNull
  @Override
  public Window < E > skip ( int count ) {
    requireLease ( "skip" );
    if ( count < 0 ) throw new IllegalArgumentException ( "count must be >= 0" );
    if ( count == 0 ) return this;
    if ( count >= length ) return view ( start, 0, reversed );
    // Drop first `count` of encounter order.
    if ( reversed ) {
      // Reversed: dropping first `count` = dropping last `count` of buffer.
      return view ( start, length - count, true );
    }
    return view ( start + count, length - count, false );
  }

  @NotNull
  @Override
  public Window < E > trim ( int count ) {
    requireLease ( "trim" );
    if ( count < 0 ) throw new IllegalArgumentException ( "count must be >= 0" );
    if ( count == 0 ) return this;
    if ( count >= length ) return view ( start, 0, reversed );
    // Drop last `count` of encounter order.
    if ( reversed ) {
      // Reversed: dropping last `count` = dropping first `count` of buffer.
      return view ( start + count, length - count, true );
    }
    return view ( start, length - count, false );
  }

  @NotNull
  @Override
  public Window < E > slice ( int offset, int count ) {
    requireLease ( "slice" );
    if ( offset < 0 ) throw new IllegalArgumentException ( "offset must be >= 0" );
    if ( count  < 0 ) throw new IllegalArgumentException ( "count must be >= 0" );
    if ( offset >= length ) return view ( start, 0, reversed );
    final int effective = Math.min ( count, length - offset );
    if ( offset == 0 && effective == length ) return this;
    // Take `effective` values starting at encounter-order index `offset`.
    if ( reversed ) {
      // Reversed encounter at logical index `offset` is buffer[start + length - 1 - offset].
      // The slice's last element (logical offset + effective - 1) is
      // buffer[start + length - 1 - (offset + effective - 1)] = buffer[start + length - offset - effective].
      // So physical range = [start + length - offset - effective .. start + length - offset - 1]
      return view ( start + length - offset - effective, effective, true );
    }
    return view ( start + offset, effective, false );
  }

  @NotNull
  @Override
  public Window < E > reverse () {
    requireLease ( "reverse" );
    return view ( start, length, !reversed );
  }
}
