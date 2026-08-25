package io.fullerstack.substrates;

import io.humainary.substrates.api.Substrates.Idempotent;
import io.humainary.substrates.api.Substrates.Name;
import io.humainary.substrates.api.Substrates.Provided;
import io.humainary.substrates.api.Substrates.Queued;
import io.humainary.substrates.api.Substrates.Subject;
import io.humainary.substrates.api.Substrates.Subscription;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/// A cancellable handle representing an active subscription to a source.
///
/// Subscription is returned by [Source#subscribe(Subscriber)] and allows
/// the subscriber to cancel interest in future events. Each subscription has its
/// own identity (via [Substrate]) and can be closed to unregister.
///
/// ## Lifecycle
///
/// A subscription progresses through these states:
/// 1. **Active**: Created by `subscribe()`, subscriber receives callbacks
/// 2. **Closed**: After `close()` is called, no more callbacks occur
///
/// ## Three routes in, one latch (§7.5, §16.3)
///
/// §16.3: the `onClose` callback "fires exactly once when the returned
/// subscription is terminated — whether by explicit close, subscriber close, or
/// **source close**". All three arrive at [#terminate()], and the
/// compare-and-set there is the whole of "exactly once": however many callers
/// race, one wins and every loser returns having done nothing.
///
/// @see FsConduit#subscribe(Subscriber)
/// @see FsSubscriber
@Provided
public final class FsSubscription implements Subscription {

  /// Lazy subject - only allocated when subject() is called.
  /// Saves FsSubject + FsId + AtomicLong CAS per subscribe in the common case.
  private final    Name                     name;
  private final    FsSubject < ? >          parent;
  private volatile Subject < Subscription > subject;

  /// Retires the registration: unsubscribes from the source and untracks this
  /// subscription there. Runs for exactly one caller — the one that wins the latch.
  private final Consumer < FsSubscription > detach;

  /// User-supplied close callback — fires exactly once when subscription terminates.
  private final Consumer < ? super Subscription > onCloseCallback;

  /// Circuit reference — needed by closeAwait() to block until the
  /// queued unsubscribe job has been processed, and to dispatch `onClose`
  /// into the circuit context.
  private final FsCircuit circuit;

  /// §7.5's idempotence and §16.3's exactly-once, as one word. A plain
  /// check-then-set would let two racing closers both fire the callback.
  private final AtomicBoolean closed = new AtomicBoolean ();

  /// Creates a new subscription with lazy subject creation.
  ///
  /// @param name    the name for the subscription subject
  /// @param parent  the parent subject for hierarchy
  /// @param circuit the owning circuit, used for [#closeAwait()]
  /// @param detach  retires the registration when the subscription terminates
  FsSubscription ( Name name, FsSubject < ? > parent, FsCircuit circuit, Consumer < FsSubscription > detach ) {
    this ( name, parent, circuit, detach, null );
  }

  /// Creates a new subscription with lazy subject creation and an onClose callback.
  ///
  /// @param name            the name for the subscription subject
  /// @param parent          the parent subject for hierarchy
  /// @param circuit         the owning circuit, used for [#closeAwait()]
  /// @param detach          retires the registration when the subscription terminates
  /// @param onCloseCallback user-supplied callback fired exactly once on termination, or null
  FsSubscription ( Name name, FsSubject < ? > parent, FsCircuit circuit, Consumer < FsSubscription > detach,
                   Consumer < ? super Subscription > onCloseCallback ) {
    this.name = name;
    this.parent = parent;
    this.circuit = circuit;
    this.detach = detach;
    this.onCloseCallback = onCloseCallback;
  }

  /// Returns the subject identity of this subscription. Lazy creation with
  /// double-checked locking — FsSubject's constructor calls
  /// ID_COUNTER.getAndIncrement(), so an unsynchronised race would mint
  /// distinct ids and orphan one of them.
  @Override
  public Subject < Subscription > subject () {
    Subject < Subscription > s = subject;
    if ( s == null ) {
      synchronized ( this ) {
        s = subject;
        if ( s == null ) {
          s = new FsSubject <> ( name, parent, Subscription.class );
          subject = s;
        }
      }
    }
    return s;
  }

  /// Closes this subscription, unregistering from the source.
  /// Idempotent - repeated calls have no effect.
  @Idempotent
  @Queued
  @Override
  public void close () {
    terminate ();
  }

  /// Whether this subscription has accepted close, by any of the three routes.
  boolean isClosed () {
    return closed.get ();
  }

  /// The single termination path, shared by explicit close, subscriber close and
  /// source close (§7.5, §16.3).
  ///
  /// The latch comes first, so the two things after it — retiring the
  /// registration and firing `onClose` — happen for exactly one caller.
  void terminate () {
    if ( closed.getAndSet ( true ) ) return;

    detach.accept ( this );

    final Consumer < ? super Subscription > callback = onCloseCallback;
    if ( callback == null ) return;

    // §16.3: the callback "receives the subscription being closed and executes
    // within the circuit context". Admitted to ingress *behind* the retire, so it
    // observes a topology this subscription has already left. Running it on the
    // closing caller's thread satisfies neither property.
    //
    // §16.3 also covers the shutdown case: "If the owning circuit has already
    // terminated and cannot accept the cleanup work, the callback is not required
    // to run" — an admission after the close marker is simply never drained.
    circuit.submitIngress (
      new FsCircuit.CircuitJob ( () -> {
        try {
          callback.accept ( this );
        } catch ( Throwable ignored ) {
          // §15.4: an onClose callback is external code. Its failure is isolated
          // here so a sibling subscription's cleanup still runs (§15.4 #2).
        }
      } ),
      null
    );
  }

  @Idempotent
  @Override
  public void closeAwait () {
    circuit.checkExternalCaller ( "closeAwait" );
    close ();
    circuit.await ();
  }

  /// Package-internal accessor for the owning circuit, used by FsSubscriber
  /// to thread an await through closeAwait().
  FsCircuit awaitCircuit () {
    return circuit;
  }

}
