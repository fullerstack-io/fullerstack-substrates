package io.fullerstack.substrates;

import io.fullerstack.substrates.FsCircuit.ReceptorAdapter;

import io.humainary.substrates.api.Substrates.Idempotent;
import io.humainary.substrates.api.Substrates.Pipe;
import io.humainary.substrates.api.Substrates.Provided;
import io.humainary.substrates.api.Substrates.Registrar;
import io.humainary.substrates.api.Substrates.Subject;
import io.humainary.substrates.api.Substrates.Subscriber;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

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

  /// Close **accepted**, written on the caller's side. Read by the synchronous guards only
  /// (`subscribe` with a closed subscriber, §9.1); no queued job consults it.
  private volatile boolean closed;

  /// Live subscriptions made from this subscriber. Guarded by [#lock].
  private List < FsSubscription > subscriptions;

  /// A private monitor rather than `this`: a close job trims the list on the circuit worker, and
  /// a caller holding the subscriber's own monitor across `await` would otherwise stall it.
  final Object lock = new Object ();

  /// The circuit this subscriber was created from. §7.2 confines every subscription made from it
  /// to that circuit ("A subscriber MUST NOT be subscribed to a source belonging to a different
  /// circuit"), so its close is one job on one circuit, and [#closeAwait()] has one circuit to
  /// wait for.
  private final FsCircuit circuit;

  /// The subscriber's close job (§7.6.1 "its subscriber's"): one admission whose value is the
  /// subscriptions that close ends. A `ReceptorAdapter`, like every other job the circuit's own
  /// components queue, so the drain's dispatch site sees no new receiver type.
  private static final Consumer < Object > CLOSE_ALL =
    new ReceptorAdapter < List < FsSubscription > > ( FsSubscription::closeAll );

  public FsSubscriber ( Subject < Subscriber < E > > subject, FsCircuit circuit,
                        BiConsumer < ? super Subject < Pipe < E > >, ? super Registrar < E > > callback ) {
    this.subject  = subject;
    this.circuit  = circuit;
    this.callback = callback;
  }

  @Override
  public Subject < Subscriber < E > > subject () {
    return subject;
  }

  /// Package-internal accessor for the closed flag, used by FsConduit
  /// when checking the closed-substrate-argument rule on subscribe (§9.1).
  boolean isClosed () {
    return closed;
  }

  /// Invokes the callback for a pipe activation.
  ///
  /// Not gated on [#closed]. §7.6.1 (3.15.3): "A subscription's window ends at the earliest of
  /// three close jobs: its own, its subscriber's, and its source's" — a position in the circuit's
  /// processing order, whereas `closed` is set the moment the caller calls [#close()]. A callback
  /// that emits, closes this subscriber, and emits again leaves transit holding
  /// `[1, close(subscriber), 2]`; the channel processes `1` ahead of the close job, the
  /// subscription is still effective there, and §7.3 requires its callback. The roster is the
  /// gate: a channel only activates subscriptions [FsHub] still lists, and the one close job
  /// [#close()] queues is what removes them, at its own position.
  public void activate ( Subject < Pipe < E > > pipeSubject, Registrar < E > registrar ) {
    callback.accept ( pipeSubject, registrar );
  }

  /// Records a live subscription so that [#close()] carries it. Called by `subscribe` with
  /// [#lock] held and the closed flag already checked under it.
  void trackSubscription ( FsSubscription subscription ) {
    if ( subscriptions == null ) {
      subscriptions = new ArrayList <> ();
    }
    subscriptions.add ( subscription );
  }

  /// Drops a subscription a close job has ended, so a long-lived subscriber does not accumulate
  /// dead handles. Called on the worker, by that job.
  void untrackSubscription ( FsSubscription subscription ) {
    synchronized ( lock ) {
      if ( subscriptions != null ) subscriptions.remove ( subscription );
    }
  }

  /// §16.3: "closing a subscriber cancels all subscriptions derived from it" — in **one** close
  /// job (§7.6.1). It carries every subscription tracked at this moment; on the worker every one
  /// of those windows still open ends at that single position, and then each `onClose` runs. A
  /// per-subscription job would let another caller's emission land between two of them, ending
  /// one subscriber's windows at two positions.
  @Idempotent
  @Override
  public void close () {
    synchronized ( lock ) {
      if ( closed ) return;
      closed = true;
      List < FsSubscription > live = subscriptions;
      subscriptions = null;
      // Admitted under the lock: a wait-free enqueue, no external code. A second closer, and
      // its closeAwait, return only once this job has a position.
      if ( live != null ) circuit.submit ( CLOSE_ALL, live );
    }
  }

  /// §9.1: rejected before close is requested when called from the owning circuit, whether or not
  /// this subscriber has subscriptions yet.
  @Idempotent
  @Override
  public void closeAwait () {
    circuit.checkExternalCaller ( "closeAwait" );
    close ();
    circuit.await ();
  }

}
