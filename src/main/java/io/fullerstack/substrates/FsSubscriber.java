package io.fullerstack.substrates;

import io.humainary.substrates.api.Substrates.Idempotent;
import io.humainary.substrates.api.Substrates.Pipe;
import io.humainary.substrates.api.Substrates.Provided;
import io.humainary.substrates.api.Substrates.Registrar;
import io.humainary.substrates.api.Substrates.Subject;
import io.humainary.substrates.api.Substrates.Subscriber;
import io.humainary.substrates.api.Substrates.Subscription;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

/// A subscriber that holds a callback to be invoked when pipes are activated.
///
/// In 2.0, subscriber callbacks receive Subject<Pipe<E>> (was Subject<Channel<E>>).
///
/// @param <E> the emission type
@Provided
public final class FsSubscriber < E > implements Subscriber < E > {

  private final Subject < Subscriber < E > > subject;

  /// Callback invoked when a pipe is activated.
  /// In 2.0: Subject<Pipe<E>> instead of Subject<Channel<E>>.
  private final BiConsumer < ? super Subject < Pipe < E > >, ? super Registrar < E > > callback;

  private volatile boolean closed;

  private List < Subscription > subscriptions;

  /// Circuit captured on first trackSubscription, used by [#closeAwait()].
  /// All subscriptions of a subscriber share the same circuit (spec invariant).
  private FsCircuit awaitCircuit;

  public FsSubscriber ( Subject < Subscriber < E > > subject, BiConsumer < ? super Subject < Pipe < E > >, ? super Registrar < E > > callback ) {
    this.subject  = subject;
    this.callback = callback;
  }

  @Override
  public Subject < Subscriber < E > > subject () {
    return subject;
  }

  /// Package-internal accessor for the closed flag, used by FsConduit/FsTap
  /// when checking the closed-substrate-argument rule on subscribe (§9.1).
  boolean isClosed () {
    return closed;
  }

  /// Invokes the callback for a pipe activation.
  public void activate ( Subject < Pipe < E > > pipeSubject, Registrar < E > registrar ) {
    if ( !closed ) {
      callback.accept ( pipeSubject, registrar );
    }
  }

  void trackSubscription ( FsSubscription subscription ) {
    boolean alreadyClosed;
    synchronized ( this ) {
      alreadyClosed = closed;
      if ( !alreadyClosed ) {
        if ( subscriptions == null ) {
          subscriptions = new ArrayList <> ();
        }
        subscriptions.add ( subscription );
        if ( awaitCircuit == null ) {
          awaitCircuit = subscription.awaitCircuit ();
        }
      }
    }
    // Outside the monitor: terminating a subscription reaches back into the
    // source to retire the registration, and §15.4 #2 forbids holding a lock
    // across that.
    if ( alreadyClosed ) subscription.terminate ();
  }

  /// Untracks a subscription that terminated by one of the other two routes
  /// (explicit close, or source close), so a long-lived subscriber does not
  /// accumulate dead handles.
  synchronized void untrackSubscription ( FsSubscription subscription ) {
    if ( subscriptions != null ) subscriptions.remove ( subscription );
  }

  /// §16.3: "closing a subscriber cancels all subscriptions derived from it."
  @Idempotent
  @Override
  public void close () {
    List < Subscription > live;
    synchronized ( this ) {
      if ( closed ) return;
      closed = true;
      live = subscriptions;
      subscriptions = null;
    }
    if ( live == null ) return;
    // Each termination is independent: one that fails must not strand its
    // siblings (§15.4 #2).
    for ( Subscription s : live ) {
      try {
        s.close ();
      } catch ( Throwable ignored ) {
        // §15.4 — suppressed; remaining subscriptions still terminate.
      }
    }
  }

  @Idempotent
  @Override
  public void closeAwait () {
    FsCircuit c = awaitCircuit;
    if ( c != null ) c.checkExternalCaller ( "closeAwait" );
    close ();
    if ( c != null ) c.await ();
  }

}
