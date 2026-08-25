package io.fullerstack.substrates;

import io.humainary.substrates.api.Substrates.NotNull;
import io.humainary.substrates.api.Substrates.Window;

import java.util.NoSuchElementException;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

/// **FsWindow** — array-backed view over a stable emission set.
///
/// Implements `Substrates.Window<E>` (new in 2.6). A Window is callback-scoped:
/// instances are only valid during the callback that received them. Restriction
/// operations (`prefix`, `suffix`, `skip`, `trim`, `slice`, `reverse`) produce
/// new view instances over the **same underlying buffer** — no value copies.
///
/// ## Strided view representation
///
/// `(buffer, start, length, reversed)`:
/// - `buffer` — the underlying Object[] holding values
/// - `start`  — physical offset of the leftmost value in this view's bounds
/// - `length` — number of values visible through this view
/// - `reversed` — when true, encounter order traverses [start+length-1 .. start]
///                instead of [start .. start+length-1]
///
/// Encounter-order index `i` maps to physical buffer index:
/// - reversed=false: `buffer[start + i]`
/// - reversed=true:  `buffer[start + length - 1 - i]`
///
/// Restriction operations compute the equivalent (start, length, reversed)
/// for the requested sub-view; no element copies, allocation only of the
/// new FsWindow record.
@SuppressWarnings ( "unchecked" )
final class FsWindow < E > implements Window < E > {

  /// The §6.4.1 temporal lease, shared by a root window and every view derived
  /// from it.
  ///
  /// §6.4 makes `Window` callback-scoped, and §6.4.1 withdraws the
  /// performance escape clause for this type by name: "Implementations MUST
  /// detect and signal `Window` temporal contract violations; undefined
  /// behavior is not an acceptable choice for this type." Its own cost
  /// argument is the design here — "one branch per operator entry … entirely
  /// off the framework emission hot path".
  ///
  /// The lease names the execution context that minted the current window. It
  /// is re-latched on every emission rather than captured once, because the
  /// operator array reaches `wrap` without a circuit reference and the stage
  /// always runs on the context that will deliver the window; latching there
  /// gets the owner without threading a circuit through materialisation.
  ///
  /// **Scope, stated rather than implied.** This detects every use that leaves
  /// the owning execution context — another thread, another circuit, any read
  /// after the circuit has moved on and the caller has come back for it, which
  /// is the escape the TCK exercises. It does **not** detect a window retained
  /// into a later callback *on the same worker*: naming that boundary needs a
  /// per-ingress-chain counter on the circuit (what the Rust projection reads
  /// off its `Turn`), and closing the lease around the operator instead would
  /// fault every receptor the transit hop legitimately delivers to afterwards.
  static final class Lease {

    private Thread owner;

    /// Binds the lease to the context minting the current window.
    void latch () {
      owner = Thread.currentThread ();
    }

    boolean valid () {
      return Thread.currentThread () == owner;
    }

  }

  private final Object[] buffer;
  private final int      start;
  private final int      length;
  private final boolean  reversed;
  private final Lease    lease;

  FsWindow ( Object[] buffer, int start, int length, boolean reversed, Lease lease ) {
    this.buffer   = buffer;
    this.start    = start;
    this.length   = length;
    this.reversed = reversed;
    this.lease    = lease;
  }

  /// Convenience form that mints a lease bound to the calling context. Used
  /// where a window is built outside an operator materialisation.
  FsWindow ( Object[] buffer, int start, int length, boolean reversed ) {
    this ( buffer, start, length, reversed, own () );
  }

  private static Lease own () {
    final Lease lease = new Lease ();
    lease.latch ();
    return lease;
  }

  /// §6.4.1: every operator entry — on the root window and on every derived
  /// view alike — signals when the lease no longer holds.
  private void requireLease ( String operation ) {
    if ( !lease.valid () ) {
      throw FsOperators.fault ( operation,
        "window used outside the circuit context that produced it" );
    }
  }

  /// Encounter-order accessor.
  private E at ( int idx ) {
    if ( reversed ) {
      return (E) buffer[ start + length - 1 - idx ];
    }
    return (E) buffer[ start + idx ];
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
      return new FsWindow <> ( buffer, start + length - count, count, true, lease );
    }
    return new FsWindow <> ( buffer, start, count, false, lease );
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
      return new FsWindow <> ( buffer, start, count, true, lease );
    }
    return new FsWindow <> ( buffer, start + length - count, count, false, lease );
  }

  @NotNull
  @Override
  public Window < E > skip ( int count ) {
    requireLease ( "skip" );
    if ( count < 0 ) throw new IllegalArgumentException ( "count must be >= 0" );
    if ( count == 0 ) return this;
    if ( count >= length ) return new FsWindow <> ( buffer, start, 0, reversed, lease );
    // Drop first `count` of encounter order.
    if ( reversed ) {
      // Reversed: dropping first `count` = dropping last `count` of buffer.
      return new FsWindow <> ( buffer, start, length - count, true, lease );
    }
    return new FsWindow <> ( buffer, start + count, length - count, false, lease );
  }

  @NotNull
  @Override
  public Window < E > trim ( int count ) {
    requireLease ( "trim" );
    if ( count < 0 ) throw new IllegalArgumentException ( "count must be >= 0" );
    if ( count == 0 ) return this;
    if ( count >= length ) return new FsWindow <> ( buffer, start, 0, reversed, lease );
    // Drop last `count` of encounter order.
    if ( reversed ) {
      // Reversed: dropping last `count` = dropping first `count` of buffer.
      return new FsWindow <> ( buffer, start + count, length - count, true, lease );
    }
    return new FsWindow <> ( buffer, start, length - count, false, lease );
  }

  @NotNull
  @Override
  public Window < E > slice ( int offset, int count ) {
    requireLease ( "slice" );
    if ( offset < 0 ) throw new IllegalArgumentException ( "offset must be >= 0" );
    if ( count  < 0 ) throw new IllegalArgumentException ( "count must be >= 0" );
    if ( offset >= length ) return new FsWindow <> ( buffer, start, 0, reversed, lease );
    final int effective = Math.min ( count, length - offset );
    if ( offset == 0 && effective == length ) return this;
    // Take `effective` values starting at encounter-order index `offset`.
    if ( reversed ) {
      // Reversed encounter at logical index `offset` is buffer[start + length - 1 - offset].
      // The slice's last element (logical offset + effective - 1) is
      // buffer[start + length - 1 - (offset + effective - 1)] = buffer[start + length - offset - effective].
      // So physical range = [start + length - offset - effective .. start + length - offset - 1]
      return new FsWindow <> ( buffer, start + length - offset - effective, effective, true, lease );
    }
    return new FsWindow <> ( buffer, start + offset, effective, false, lease );
  }

  @NotNull
  @Override
  public Window < E > reverse () {
    requireLease ( "reverse" );
    return new FsWindow <> ( buffer, start, length, !reversed, lease );
  }
}
