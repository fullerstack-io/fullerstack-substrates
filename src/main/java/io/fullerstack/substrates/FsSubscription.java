package io.fullerstack.substrates;

import io.humainary.substrates.api.Substrates.Idempotent;
import io.humainary.substrates.api.Substrates.Name;
import io.humainary.substrates.api.Substrates.Provided;
import io.humainary.substrates.api.Substrates.Queued;
import io.humainary.substrates.api.Substrates.Subject;
import io.humainary.substrates.api.Substrates.Subscription;

import java.util.List;
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
/// ## Three routes in, each its own job, exactly-once decided on the worker (§7.5, §7.6.1, §16.3)
///
/// §16.3: the `onClose` callback "fires exactly once when the returned
/// subscription is terminated — whether by explicit close, subscriber close, or
/// **source close**", and §7.6.1: the window "ends at the earliest of three close jobs:
/// its own, its subscriber's, and its source's". So every close that is called queues its
/// **own** job — [#close()] here, [FsSubscriber#close()], [FsConduit#close()] — and each
/// job carries every subscription that close covers. None is filtered out on the caller's
/// side because another route "got there first": a route that had only *accepted* a close
/// has no position yet, and a caller-side winner would leave the other close without one.
///
/// Which job ends the subscription is decided where the jobs execute: the first to reach it
/// on the worker finds it not [#retired], takes it off the roster and runs `onClose`; the
/// others find it retired and do nothing. That is "the earliest of three" and "exactly once"
/// as one worker-confined flag.
///
/// Retiring and `onClose` share a job on purpose. As two admissions, another caller's
/// registration job could be admitted between them, and `onClose` would then run after
/// the registration job of a window that was already empty — against §7.6.1's "no
/// later than the point its registration job executes".
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

  /// The source this subscription was made on. It holds the roster this subscription sits in
  /// and the pipe that carries its own close job.
  final FsConduit < ? > source;

  /// The subscriber this subscription was made from. §7.3 keys the callback on the
  /// subscription/channel *pair*, so the channel-side memo is keyed by this subscription
  /// (see [FsHub], [FsChannel]) and must be able to reach the callback from it.
  final FsSubscriber < ? > subscriber;

  /// User-supplied close callback — fires exactly once when subscription terminates.
  private final Consumer < ? super Subscription > onCloseCallback;

  /// Circuit reference — lifecycle authority only: [#closeAwait()] blocks on it until the
  /// queued close job has been processed.
  private final FsCircuit circuit;

  /// §7.5's idempotence for this route only: a second [#close()] queues no second job. Guarded
  /// by this object's monitor, together with the admission of the job, so a caller that finds
  /// it set can rely on the job already being queued (§9.1 `closeAwait`).
  private boolean closeAccepted;

  /// Whether a close job has EXECUTED for this subscription on the circuit worker — the close end
  /// of the §7.6.1 window in processing order. Worker-confined: written by a close job, read by
  /// close jobs and by the conduit's registration job.
  private boolean retired;

  /// Whether `onClose` has run. Worker-confined. Separate from [#retired] only because a job that
  /// ends several subscriptions retires all of them before it runs any `onClose`.
  private boolean notified;

  /// Creates a new subscription with lazy subject creation and an onClose callback.
  ///
  /// @param name            the name for the subscription subject
  /// @param parent          the parent subject for hierarchy
  /// @param circuit         the owning circuit, used for [#closeAwait()]
  /// @param source          the conduit this subscription was made on
  /// @param subscriber      the subscriber this subscription was made from
  /// @param onCloseCallback user-supplied callback fired exactly once on termination, or null
  FsSubscription ( Name name, FsSubject < ? > parent, FsCircuit circuit, FsConduit < ? > source,
                   FsSubscriber < ? > subscriber, Consumer < ? super Subscription > onCloseCallback ) {
    this.name = name;
    this.parent = parent;
    this.circuit = circuit;
    this.source = source;
    this.subscriber = subscriber;
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

  /// Closes this subscription: its own close job, queued on the source's circuit (§7.5).
  /// Idempotent - repeated calls have no effect.
  @Idempotent
  @Queued
  @Override
  public void close () {
    synchronized ( this ) {
      if ( closeAccepted ) return;
      closeAccepted = true;
      // Admitted under the monitor: a wait-free enqueue, no external code. A second closer
      // returns only once this job has a position.
      source.unsubscribe ( this );
    }
  }

  boolean isRetired () {
    return retired;
  }

  /// Worker side of a close job, first half: if no earlier close job has, end the window here
  /// (§7.6.1). After this the registration job, if it has not run yet, takes no effect, and no
  /// channel selects this subscription at a later position.
  ///
  /// The tracking lists are trimmed here and not by the caller that asked: until its close job
  /// runs, a subscription must stay visible to the *other* routes, so that a subscriber's or a
  /// source's close queued in the meantime carries it too.
  void retire () {
    if ( retired ) return;
    retired = true;
    source.hub.removeSubscription ( this );
    source.untrackSubscription ( this );
    subscriber.untrackSubscription ( this );
  }

  /// Worker side of a close job, second half: `onClose`, once, in the circuit context (§16.3),
  /// observing a topology this subscription has already left.
  void notifyClosed () {
    if ( notified ) return;
    notified = true;
    final Consumer < ? super Subscription > callback = onCloseCallback;
    if ( callback == null ) return;
    try {
      callback.accept ( this );
    } catch ( Throwable ignored ) {
      // §15.4: an onClose callback is external code. Its failure is isolated here so a
      // sibling subscription's cleanup, in the same close job, still runs (§15.4 #2).
    }
  }

  /// A close job for many subscriptions at once — a subscriber's, or a source's (§7.6.1: "its
  /// subscriber's, and its source's"). Every window still open ends before any `onClose` runs,
  /// so whatever an `onClose` raises is positioned behind the close of all of them.
  static void closeAll ( List < FsSubscription > subscriptions ) {
    for ( int i = 0, n = subscriptions.size (); i < n; i++ ) subscriptions.get ( i ).retire ();
    for ( int i = 0, n = subscriptions.size (); i < n; i++ ) subscriptions.get ( i ).notifyClosed ();
  }

  @Idempotent
  @Override
  public void closeAwait () {
    circuit.checkExternalCaller ( "closeAwait" );
    close ();
    circuit.await ();
  }

}
