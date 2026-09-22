package io.fullerstack.substrates;

import io.humainary.substrates.api.Substrates.Pipe;
import io.humainary.substrates.api.Substrates.Receptor;
import io.humainary.substrates.api.Substrates.Subject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/// Channel — the per-name dispatch point on the routing path.
///
/// Cortex → Circuit → Conduit → **Channel** → Pipe → Receptor
///
/// The channel is the receiver of every emission addressed to its name, on
/// ingress and on transit alike: it implements Consumer<Object>, it is what
/// `conduit.get(name)`'s pipe and every fiber/flow terminal aimed at that pipe
/// submit, and on dequeue [#receive] checks the hub version and dispatches.
/// The check runs at the position where the emission is processed because
/// that is where §7.6.1 selects the recipients — "exactly those subscriptions
/// effective at that position" — and a subscribe or close raised inside a
/// cascade is transit that takes effect within the cascade (§5.3, §7.6.1,
/// §7.6.2). There is no second, check-free dispatch path for transit: one
/// existed (a `cascadeDispatch` consumer captured at the last rebuild and
/// submitted by the terminals) and it missed every topology change still
/// ahead of the delivery in transit.
///
/// The dispatch is Consumer<Object> throughout — same type as the
/// transit queue, the flow chain, and the registrar's stored consumers.
/// A registered receptor is invoked during the walk; a registered pipe is
/// stored as one [FsRegistrar.Admit] and *submitted* to its own circuit
/// (§5.3 — transit behind the cascade's accepted work, or ingress to a
/// foreign owner), never invoked here. See [FsRegistrar#register(Pipe)].
///
/// Subscriber state is memoised per **subscription** (§7.3 "exactly once per
/// subscription/channel pair"), so a subscription closed and re-made from the
/// same subscriber is a fresh pair: its callback runs again and the old pair's
/// registrations leave the dispatch list.
final class FsChannel < E > implements Receptor < E >, Consumer < Object > {

  /// Shared empty ancestor chain. Every per-pipe-routed channel and every
  /// root-named hierarchical channel shares it, so neither allocates.
  private static final FsChannel < ? >[] NONE = new FsChannel < ? >[0];

  @SuppressWarnings ( "unchecked" )
  static < E > FsChannel < E >[] none () {
    return (FsChannel < E >[]) NONE;
  }

  private final Subject < Pipe < E > > subject;
  private final FsHub < E >            hub;

  /// §10.3 hierarchical routing: this channel's ancestor channels, ordered
  /// nearest-first (direct parent, then its parent, … up to the root name).
  ///
  /// The chain is resolved once, by [FsConduit], when the channel is
  /// materialised — a name's ancestry is immutable, so it is complete and final
  /// for the life of the channel and dispatch never has to look anything up.
  /// It is deliberately **not** a snapshot of "ancestors that happened to exist
  /// at subscribe time": §10.3 propagates through *all* ancestor names, and a
  /// subscriber registered before a leaf existed must still see that leaf's
  /// future emissions at the ancestor.
  ///
  /// Empty under per-pipe routing, and empty for a root name (one with no
  /// enclosure) even under hierarchical routing — §10.3: "Implementations that
  /// do not provide it MUST behave as if per-pipe routing were always in
  /// effect", which is exactly what an empty chain does.
  final FsChannel < E >[] ancestors;

  /// `ancestors.length != 0`, hoisted: the per-pipe path then costs one field
  /// read instead of a field read plus an array-length load.
  private final boolean stem;

  /// The upstream pipe — what conduit.get(name) returns.
  final FsPipe < E > pipe;

  /// Downstream dispatch — receptors only, no STEM. Used by [#receive] (after
  /// the version check) and by dispatchStem when walking ancestors (an ancestor
  /// must not trigger its own STEM walk — the chain is already flattened, so
  /// walking again would double-deliver at every level above).
  Consumer < Object > dispatch;

  /// Version this channel was last built at.
  int builtVersion = -1;

  /// Per-subscription registrations — lazy, circuit-thread only. Keyed by the
  /// subscription (the §7.3 pair), not the subscriber: the entry is what says
  /// "this channel has rebuilt for this subscription", and it leaves with the
  /// subscription's roster entry, never with the subscriber.
  Map < FsSubscription, List < Consumer < Object > > > subscriberReceptors;

  FsChannel (
    Subject < Pipe < E > > subject,
    FsCircuit circuit,
    FsHub < E > hub,
    FsChannel < E >[] ancestors
  ) {
    this.subject = subject;
    this.hub = hub;
    this.ancestors = ancestors;
    this.stem = ancestors.length != 0;
    this.pipe = new FsPipe <> ( this, circuit );
  }

  Subject < Pipe < E > > subject () {
    return subject;
  }

  // ─── Ingress receiver ───

  @Override
  @SuppressWarnings ( "unchecked" )
  public void accept ( Object o ) {
    receive ( (E) o );
  }

  // ─── Dispatch (every path — version check) ───
  //
  // Called from the ingress drain and from the transit drain alike. The check
  // is one int compare against the hub, and it has to be here rather than at
  // any earlier point: §7.6.1 selects the recipients "effective at that
  // position", the position at which the channel processes the emission, and
  // under 3.3.0 a registration or close job can sit anywhere in transit ahead
  // of this delivery — including one raised in the same callback that emitted
  // it (§5.3 "Transit is FIFO over every kind of operation it carries").

  @Override
  public void receive ( E emission ) {
    if ( builtVersion != hub.subscriberVersion ) rebuild ();
    if ( stem ) {
      dispatchStem ( emission );
      return;
    }
    Consumer < Object > d = dispatch;
    if ( d != null ) d.accept ( emission );
  }

  /// §10.3 hierarchical routing: "Emissions propagate from the target named
  /// pipe upward through all ancestor names in the hierarchy, **leaf-first**."
  ///
  /// This channel's own receptors run first, then each ancestor's in
  /// nearest-to-root order. Each ancestor is version-checked here rather than
  /// in [#receive]: an ancestor may never have received a direct emission, so
  /// this can be the first time its subscribers are activated (§10.2 — the
  /// subscriber callback is invoked lazily when a named pipe receives its first
  /// emission, and a propagated emission is that pipe's emission).
  private void dispatchStem ( E emission ) {
    deliver ( dispatch, emission );
    FsChannel < E >[] chain = ancestors;
    int version = hub.subscriberVersion;
    for ( int i = 0, len = chain.length; i < len; i++ ) {
      FsChannel < E > ancestor = chain[i];
      if ( ancestor.builtVersion != version ) ancestor.rebuild ();
      deliver ( ancestor.dispatch, emission );
    }
  }

  /// §15.4 #2 liveness: a receptor that throws at one level of the hierarchy
  /// must not cost the remaining levels their delivery of the same emission.
  /// The per-pipe path leaves this to the drain's trust boundary because there
  /// is nothing after it to protect; a hierarchical dispatch has the rest of
  /// the chain still to run.
  private static void deliver ( Consumer < Object > d, Object emission ) {
    if ( d == null ) return;
    try {
      d.accept ( emission );
    } catch ( Throwable ignored ) {
      // §15.4 #4: observability of a failed external callback is
      // implementation-defined; continue up the chain.
    }
  }

  // ─── Rebuild (cold path) ───

  @SuppressWarnings ( "unchecked" )
  private void rebuild () {
    FsSubscription[] currentSubs = hub.ensureSnapshot ();

    if ( subscriberReceptors == null ) {
      subscriberReceptors = new IdentityHashMap <> ();
    }

    Set < FsSubscription > activeSet = Collections.newSetFromMap ( new IdentityHashMap <> () );
    for ( FsSubscription sub : currentSubs ) {
      activeSet.add ( sub );
    }

    subscriberReceptors.keySet ().removeIf ( sub -> !activeSet.contains ( sub ) );

    for ( FsSubscription subscription : currentSubs ) {
      // §7.3 step 3: rebuild when the pipe "has not yet rebuilt for this subscription".
      if ( !subscriberReceptors.containsKey ( subscription ) ) {
        FsRegistrar < E > registrar = new FsRegistrar <> ();
        try {
          ( (FsSubscriber < E >) subscription.subscriber ).activate ( subject, registrar );
          subscriberReceptors.put ( subscription, registrar.consumers () );
        } catch ( Throwable ignored ) {
          // §15.4 #3: "A failing subscriber callback is still considered
          // consumed for that subscription/channel pair: registration calls
          // completed before the failure remain registered, and the callback
          // MUST NOT be retried for that subscription/channel pair." So keep
          // whatever the callback managed to register before it threw — and
          // still record an entry, which is what suppresses the retry.
          subscriberReceptors.put ( subscription, registrar.consumers () );
        }
      }
    }

    // Build receptor-only dispatch — used by ingress receive() and by
    // dispatchStem when walking ancestors.
    //
    // Iterate via `currentSubs` (the snapshot taken from FsHub.subscribersList,
    // which is an ArrayList preserving subscription order) — NOT via
    // `subscriberReceptors.values()`. `subscriberReceptors` is an
    // IdentityHashMap whose iteration order depends on System.identityHashCode,
    // which is randomized per JVM start. Iterating in registration order keeps
    // dispatch deterministic across runs.
    List < Consumer < Object > > all = new ArrayList <> ();
    for ( FsSubscription sub : currentSubs ) {
      List < Consumer < Object > > list = subscriberReceptors.get ( sub );
      if ( list != null ) all.addAll ( list );
    }

    if ( all.isEmpty () ) {
      dispatch = null;
    } else if ( all.size () == 1 ) {
      dispatch = all.getFirst ();
    } else {
      Consumer < Object >[] arr = all.toArray ( new Consumer[0] );
      // §15.4 #2 liveness: per-consumer try/catch so a failing receptor
      // does not block siblings on the same channel from receiving.
      dispatch = v -> {
        for ( int i = 0, len = arr.length; i < len; i++ ) {
          try { arr[i].accept ( v ); } catch ( Throwable ignored ) { /* §15.4 — continue with next sibling */ }
        }
      };
    }

    builtVersion = hub.subscriberVersion;
  }
}
