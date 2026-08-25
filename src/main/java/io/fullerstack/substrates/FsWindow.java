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
  /// The lease is a **(context, generation)** pair, opened afresh on every emission rather
  /// than captured once, because the operator array reaches `wrap` without a circuit
  /// reference and the stage always runs on the context that will deliver the window;
  /// latching there gets the owner without threading a circuit through materialisation.
  ///
  /// **Both halves are load-bearing, and the second one was missing.**
  ///
  /// The *context* catches a window that has left the worker — another thread, another
  /// circuit, a caller that came back for it after the circuit moved on. That is the escape
  /// the TCK exercises.
  ///
  /// The *generation* catches a window retained into a later callback **on the same worker**,
  /// which a bare thread check cannot see because the worker is still the worker. This is not
  /// a stale read: `Flow.window` rewrites one buffer in place on every emission, so the
  /// retained view reports whatever is in that buffer *now* while presenting itself as the
  /// window from an earlier callback. §6.4.1 withdraws the performance escape clause for this
  /// type by name — "Implementations MUST detect and signal `Window` temporal contract
  /// violations; undefined behavior is not an acceptable choice for this type" — and a silent
  /// wrong answer is the worst reading of that.
  ///
  /// Closing the lease around the operator call instead — latch, `accept`, release — does not
  /// work: a transit hop queues the window and the receptor runs after `accept` has returned,
  /// so release would fault every delivery the hop legitimately makes.
  static final class Lease {

    private static final VarHandle GENERATION;

    static {
      try {
        GENERATION = MethodHandles.lookup ()
          .findVarHandle ( Lease.class, "generation", long.class );
      } catch ( ReflectiveOperationException error ) {
        throw new ExceptionInInitializerError ( error );
      }
    }

    /// The context that minted the current window.
    private Thread owner;

    /// Which mint the current window belongs to. Monotonic, and the half of the identity
    /// that distinguishes one callback from the next on the SAME worker — the case a bare
    /// thread check cannot see, because the worker is still the worker.
    private long generation;

    /// Opens a new generation and binds it to the minting context.
    ///
    /// Returns the generation the caller must stamp on the window it is about to emit.
    /// `owner` is written first and published by the RELEASE store below, so a reader that
    /// sees this generation also sees this owner. Release rather than volatile because this
    /// runs on the emission path once per window, and on x86 a release store is a plain
    /// store while a volatile store is a locked instruction.
    long latch () {
      final long next = generation + 1;
      owner = Thread.currentThread ();
      GENERATION.setRelease ( this, next );
      return next;
    }

    /// §6.4.1: is `stamp`'s window still the one this lease describes?
    ///
    /// Two questions, and both are needed. The generation catches a window retained into a
    /// LATER callback — the buffer has been rewritten under it, so the values it would return
    /// are not the ones it was handed. The owner catches a window that has left the context
    /// entirely, which the generation alone cannot see while the circuit is idle and the
    /// generation has not moved.
    ///
    /// The acquire load orders the `owner` read after it, which is what makes the pair sound
    /// from a thread that never latched.
    boolean valid ( long stamp ) {
      return (long) GENERATION.getAcquire ( this ) == stamp
             && owner == Thread.currentThread ();
    }

  }

  private final Object[] buffer;
  private final int      start;
  private final int      length;
  private final boolean  reversed;
  private final Lease    lease;

  /// The generation this window was minted in. A derived view inherits its root's, so a
  /// whole family of views expires together with the callback that produced the root.
  private final long     generation;

  FsWindow ( Object[] buffer, int start, int length, boolean reversed, Lease lease, long generation ) {
    this.buffer     = buffer;
    this.start      = start;
    this.length     = length;
    this.reversed   = reversed;
    this.lease      = lease;
    this.generation = generation;
  }

  /// A restriction of this window: same buffer, same lease, same generation. Sharing the
  /// stamp is the point — a view must not outlive the callback its root belongs to.
  private FsWindow < E > view ( int start, int length, boolean reversed ) {
    return new FsWindow <> ( buffer, start, length, reversed, lease, generation );
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
