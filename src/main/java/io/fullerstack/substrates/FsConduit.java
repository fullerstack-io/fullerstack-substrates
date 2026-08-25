package io.fullerstack.substrates;

import io.humainary.substrates.api.Substrates.Conduit;
import io.humainary.substrates.api.Substrates.Fault;
import io.humainary.substrates.api.Substrates.Fiber;
import io.humainary.substrates.api.Substrates.Flow;
import io.humainary.substrates.api.Substrates.Idempotent;
import io.humainary.substrates.api.Substrates.Name;
import io.humainary.substrates.api.Substrates.New;
import io.humainary.substrates.api.Substrates.NotNull;
import io.humainary.substrates.api.Substrates.Pipe;
import io.humainary.substrates.api.Substrates.Pool;
import io.humainary.substrates.api.Substrates.Provided;
import io.humainary.substrates.api.Substrates.Queued;
import io.humainary.substrates.api.Substrates.Receptor;
import io.humainary.substrates.api.Substrates.Routing;
import io.humainary.substrates.api.Substrates.Subject;
import io.humainary.substrates.api.Substrates.Subscriber;
import io.humainary.substrates.api.Substrates.Subscription;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.humainary.substrates.api.Substrates.cortex;
import static java.util.Objects.requireNonNull;

/// Conduit — pools named channels and manages subscriptions.
///
/// Pool side: maps names to channels. conduit.get(name) returns channel.pipe.
/// Source side: delegates to Hub for subscriber management.
@Provided
public final class FsConduit < E > implements Conduit < E > {

  private final FsCircuit                circuit;
  /// §10.3: true when this conduit was created with `Routing.STEM`, the
  /// optional hierarchical-routing extension. Per-pipe routing is the default.
  private final boolean                  stem;
  final FsHub < E >                      hub;
  private final Subject < Conduit < E > > subject;

  /// Channels by name — copy-on-write for thread-safe reads.
  private volatile Map < Name, FsChannel < E > > channels;

  /// Last lookup cache.
  private Name              lastLookupName;
  private FsChannel < E >   lastLookupChannel;

  /// Closed flag — set by `close()`. Subscribe jobs check this on the
  /// circuit thread and silently drop (per Resource §9.1 queued-drop semantics).
  /// Written under `subscriptionLock` so a subscribe racing a close either
  /// registers before the drain or is terminated by it, never neither.
  private volatile boolean closed;

  /// Live subscriptions issued by this source, so §9.1's "closing a source
  /// cancels any outstanding subscriptions to it" has something to cancel.
  /// Guarded by `subscriptionLock`; lazily allocated.
  private List < FsSubscription > subscriptions;

  /// A dedicated monitor rather than `this`: `createChannel` already
  /// synchronises on the conduit, and a subscription's termination reaches back
  /// in here while a caller may be materialising a channel.
  private final Object subscriptionLock = new Object ();

  public FsConduit ( FsSubject < ? > parent, Name name, FsCircuit circuit ) {
    this ( parent, name, circuit, Routing.PIPE );
  }

  @SuppressWarnings ( "unchecked" )
  public FsConduit ( FsSubject < ? > parent, Name name, FsCircuit circuit, Routing routing ) {
    this.circuit = circuit;
    this.stem    = routing == Routing.STEM;
    this.hub     = new FsHub <> ();
    this.subject = (Subject < Conduit < E > >) (Subject < ? >) new FsSubject <> ( name, parent, Conduit.class );
  }

  @Override
  public Subject < Conduit < E > > subject () {
    return subject;
  }

  // ─── Pool<Pipe<E>> ───

  @NotNull
  @Override
  public Pipe < E > get ( @NotNull Name name ) {
    Name last = lastLookupName;
    if ( last != null && name == last ) {
      return lastLookupChannel.pipe;
    }
    Map < Name, FsChannel < E > > map = channels;
    if ( map != null ) {
      FsChannel < E > cached = map.get ( name );
      if ( cached != null ) {
        lastLookupName = name;
        lastLookupChannel = cached;
        return cached.pipe;
      }
    }
    return createChannel ( name ).pipe;
  }

  @NotNull
  @Override
  public < U > Pool < U > pool ( @NotNull Function < ? super Pipe < E >, ? extends U > fn ) {
    return new FsDerivedPool <> ( this, fn );
  }

  /// 2.3: derived pool for flow preprocessing.
  /// Each pipe `get(name)` returned by the derived pool emits T; the flow
  /// transforms T → E before reaching this conduit's pipes.
  @NotNull
  @Override
  public < T > Pool < Pipe < T > > pool ( @NotNull Flow < T, E > flow ) {
    requireNonNull ( flow );
    if ( !( flow instanceof FsFlow < T, E > fsFlow ) ) {
      throw new IllegalArgumentException ( "flow must be an FsFlow instance" );
    }
    return new FsDerivedPool <> ( this, p -> fsFlow.pipe ( p ) );
  }

  /// 2.3: derived pool for fiber preprocessing.
  /// Each pipe `get(name)` returned by the derived pool applies the fiber
  /// before reaching this conduit's pipe (same emission type).
  @NotNull
  @Override
  public Pool < Pipe < E > > pool ( @NotNull Fiber < E > fiber ) {
    requireNonNull ( fiber );
    if ( !( fiber instanceof FsFiber < E > fsFiber ) ) {
      throw new IllegalArgumentException ( "fiber must be an FsFiber instance" );
    }
    return new FsDerivedPool <> ( this, p -> fsFiber.pipe ( p ) );
  }

  private FsChannel < E > createChannel ( Name name ) {
    synchronized ( this ) {
      FsChannel < E > channel = materialize ( name );
      lastLookupName = name;
      lastLookupChannel = channel;
      return channel;
    }
  }

  /// Materialises the channel for `name` — and, under §10.3 hierarchical
  /// routing, every ancestor channel on its dispatch path first.
  ///
  /// §10.3: "Emissions propagate from the target named pipe upward through
  /// **all** ancestor names in the hierarchy, leaf-first." The ancestor names
  /// are therefore part of a hierarchical channel's routing path, not merely
  /// channels that a caller may or may not have asked for, so they are
  /// materialised together with the leaf rather than resolved — and skipped
  /// when absent — at dispatch time. Two consequences make that the only
  /// workable reading:
  ///
  /// - A subscriber discovers an ancestor through the ancestor's own subject
  ///   (§10.2), and there is no subject without a channel. Propagating only to
  ///   ancestors someone had already `get`-ed would make hierarchical
  ///   observation depend on unrelated lookups — "without explicit subscriber
  ///   wiring at each level" is precisely what §10.3 promises it does not.
  /// - The chain cannot be captured at subscribe time either: a subscriber
  ///   registered on a parent **before** a leaf exists must still receive that
  ///   leaf's future emissions (§7.6.1 visibility windows). Resolution is per
  ///   channel and permanent; the subscriber set stays late-bound behind
  ///   [FsHub]'s version counter.
  ///
  /// A name's ancestry is immutable, so the chain built here is complete and
  /// final for the life of the channel and the dispatch path never allocates or
  /// looks anything up. Per-pipe conduits build no chain at all.
  ///
  /// Caller must hold this conduit's monitor; recurses once per ancestor level.
  private FsChannel < E > materialize ( Name name ) {
    if ( channels == null ) {
      channels = new IdentityHashMap <> ();
    }
    FsChannel < E > cached = channels.get ( name );
    if ( cached != null ) return cached;

    FsChannel < E >[] ancestors = stem ? ancestorChain ( name ) : FsChannel.none ();

    FsSubject < Pipe < E > > pipeSubject = new FsSubject <> ( name, (FsSubject < ? >) subject, Pipe.class );

    FsChannel < E > channel = new FsChannel <> ( pipeSubject, circuit, hub, ancestors );

    Map < Name, FsChannel < E > > newMap = new IdentityHashMap <> ( channels );
    newMap.put ( name, channel );
    channels = newMap;

    return channel;
  }

  /// Builds the leaf-first ancestor chain for `name`: its direct parent, then
  /// that parent's own chain (§10.3 dispatch order). Materialises each level
  /// on the way, so a chain is only ever built once per name.
  @SuppressWarnings ( "unchecked" )
  private FsChannel < E >[] ancestorChain ( Name name ) {
    Optional < Name > enclosure = name.enclosure ();
    if ( enclosure.isEmpty () ) return FsChannel.none ();

    FsChannel < E > parent = materialize ( enclosure.get () );
    FsChannel < E >[] above = parent.ancestors;

    FsChannel < E >[] chain = (FsChannel < E >[]) new FsChannel[above.length + 1];
    chain[0] = parent;
    System.arraycopy ( above, 0, chain, 1, above.length );
    return chain;
  }

  // ─── Source<E, Conduit<E>> ───

  @New
  @NotNull
  @Queued
  @Override
  public Subscription subscribe (
    @NotNull Subscriber < E > subscriber ) {
    return subscribe ( subscriber, ignored -> { } );
  }

  @New
  @NotNull
  @Queued
  @Override
  public Subscription subscribe (
    @NotNull Subscriber < E > subscriber,
    @NotNull @Queued Consumer < ? super Subscription > onClose ) {

    requireNonNull ( subscriber, "subscriber must not be null" );
    // §15.2: the onClose callback is a required argument of this overload.
    requireNonNull ( onClose, "onClose must not be null" );

    requireOpen ( "subscribe" );

    FsSubject < ? > subSubject = (FsSubject < ? >) subscriber.subject ();
    FsSubject < ? > subscriberCircuit = subSubject.findCircuitAncestor ();
    if ( subscriberCircuit != null && subscriberCircuit != circuit.subject () ) {
      // §15.3 two-subject attribution: the receiver is this conduit, and the
      // offending argument is rendered into the message.
      throw new Fault ( subject, "subscribe",
        "Subscriber belongs to a different circuit: " + subSubject );
    }

    FsSubscriber < E > fs = (FsSubscriber < E >) subscriber;
    // §15.3: "If the failed operation was rejected because a substrate argument
    // was closed, the error MUST also identify that argument's subject." §16.3
    // fixes which subject is which: "A closed-subscriber rejection is attributed
    // to the *source's* subject and identifies the *subscriber's* subject as the
    // offending argument."
    if ( fs.isClosed () ) {
      throw new Fault ( subject, "subscribe",
        "subscriber is closed: " + subSubject );
    }

    FsSubscription subscription = new FsSubscription ( subscriber.subject ().name (),
      (FsSubject < ? >) subject, circuit, s -> retire ( s, fs ), onClose );

    trackSubscription ( subscription );
    fs.trackSubscription ( subscription );

    // Use CircuitJob (distinct class) so subscribe/unsubscribe lambdas don't
    // pollute ReceptorAdapter.accept's type profile on the hot path.
    // If the conduit is closed by the time this runs on the circuit thread,
    // silently drop per Resource §9.1 queued-operation semantics.
    circuit.submitIngress (
      new FsCircuit.CircuitJob ( () -> { if ( !closed ) hub.addSubscriber ( fs ); } ),
      null
    );

    return subscription;
  }

  /// Closes the conduit. Sets the closed flag so any pending or future
  /// subscribe jobs are silently dropped on the circuit thread, and queues
  /// a job to release the subscriber list. Idempotent (Resource §9.1).
  /// §9.1 open-required guard. Rejects if either this conduit or its owning
  /// circuit has accepted close — circuit-closed leaves work undrainable, so
  /// the conduit is effectively closed too.
  private void requireOpen ( String op ) {
    if ( closed || circuit.isClosed () )
      throw new Fault ( subject, op, "conduit is closed" );
  }

  /// §9.1: closing a source "cancels any outstanding subscriptions to it", and
  /// §16.3 makes source close one of the three routes that fires a subscription's
  /// `onClose`. Terminating each live subscription is what discharges both.
  @Idempotent
  @Queued
  @Override
  public void close () {
    List < FsSubscription > live;
    synchronized ( subscriptionLock ) {
      if ( closed ) return;
      closed = true;
      live = subscriptions;
      subscriptions = null;
    }

    if ( live != null ) {
      // Outside the lock, and one guard per subscription: §15.4 #2 — a failing
      // termination must not strand its siblings' cleanup.
      for ( FsSubscription s : live ) {
        try {
          s.terminate ();
        } catch ( Throwable ignored ) {
          // §15.4 — suppressed; remaining subscriptions still terminate.
        }
      }
    }

    circuit.submitIngress (
      new FsCircuit.CircuitJob ( () -> {
        if ( hub.subscribersList != null ) {
          hub.subscribersList.clear ();
          hub.subscriberVersion++;
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

  /// Records a live subscription so that source close can terminate it (§9.1).
  /// A subscribe that loses the race with close terminates immediately rather
  /// than leaving a handle nobody will ever close.
  private void trackSubscription ( FsSubscription subscription ) {
    boolean alreadyClosed;
    synchronized ( subscriptionLock ) {
      alreadyClosed = closed;
      if ( !alreadyClosed ) {
        if ( subscriptions == null ) subscriptions = new ArrayList <> ();
        subscriptions.add ( subscription );
      }
    }
    if ( alreadyClosed ) subscription.terminate ();
  }

  /// Retires one subscription: drops it from both tracking lists and queues the
  /// hub unsubscribe. Runs once per subscription — [FsSubscription#terminate()]
  /// holds the latch.
  private void retire ( FsSubscription subscription, FsSubscriber < E > subscriber ) {
    synchronized ( subscriptionLock ) {
      if ( subscriptions != null ) subscriptions.remove ( subscription );
    }
    subscriber.untrackSubscription ( subscription );
    enqueueUnsubscribe ( subscriber );
  }

  private void enqueueUnsubscribe ( FsSubscriber < E > subscriber ) {
    circuit.submitIngress ( new FsCircuit.CircuitJob ( () -> hub.removeSubscriber ( subscriber ) ), null );
  }

}
