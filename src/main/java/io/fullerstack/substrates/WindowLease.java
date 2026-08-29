package io.fullerstack.substrates;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/// **WindowLease** — the §6.4.1 temporal lease, shared by a root window and every view derived
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
/// a stale read: `Flow.window` overwrites the retained entries in place on every emission, so the
/// retained view reports whatever is in that buffer *now* while presenting itself as the
/// window from an earlier callback. §6.4.1 withdraws the performance escape clause for this
/// type by name — "Implementations MUST detect and signal `Window` temporal contract
/// violations; undefined behavior is not an acceptable choice for this type" — and a silent
/// wrong answer is the worst reading of that.
///
/// Closing the lease around the operator call instead — latch, `accept`, release — does not
/// work: a transit hop queues the window and the receptor runs after `accept` has returned,
/// so release would fault every delivery the hop legitimately makes.
final class WindowLease {

  private static final VarHandle GENERATION;

  static {
    try {
      GENERATION = MethodHandles.lookup ()
        .findVarHandle ( WindowLease.class, "generation", long.class );
    } catch ( ReflectiveOperationException error ) {
      throw new ExceptionInInitializerError ( error );
    }
  }

  /// The ring every window minted under this lease is a view over.
  ///
  /// It lives here rather than in each view because it is a property of the STAGE — one
  /// materialisation, one line, one lease — while views are minted per emission and per
  /// restriction. Holding it once takes a compressed oop out of `FsWindow`, whose fields then
  /// sum to 29 bytes and pad to 32 rather than 33 padding to 40.
  final Object[] buffer;

  WindowLease ( Object[] buffer ) {
    this.buffer = buffer;
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
