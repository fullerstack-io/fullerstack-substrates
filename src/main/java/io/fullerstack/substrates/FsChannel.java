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
/// The channel has two roles:
/// - **Ingress receiver**: implements Consumer<Object>. Stored in the
///   ingress queue. On dequeue, checks version and dispatches.
/// - **Dispatch builder**: after rebuild, sets the dispatch Consumer
///   which is used directly on the transit hot path. `cascadeDispatch`
///   already includes STEM propagation if applicable, so transit
///   cascades can call it directly without going through the
///   channel — bypassing the version check (which the spec guarantees
///   is stable mid-cascade per §5.4.1 + §7.6.2).
///
/// The dispatch is Consumer<Object> throughout — same type as the
/// transit queue, the flow chain, and the registrar's stored receivers.
/// No lambda wrappers on the hot path.
final class FsChannel < E > implements Receptor < E >, Consumer < Object > {

  /// Shared empty ancestor chain. Every per-pipe-routed channel and every
  /// root-named hierarchical channel shares it, so neither allocates.
  private static final FsChannel < ? >[] NONE = new FsChannel < ? >[0];

  @SuppressWarnings ( "unchecked" )
  static < E > FsChannel < E >[] none () {
    return (FsChannel < E >[]) NONE;
  }

  private final Subject < Pipe < E > > subject;
  private final FsCircuit              circuit;
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

  /// The constant `cascadeDispatch` of a hierarchical channel. Held in a field
  /// so a rebuild does not allocate a fresh method reference; null for per-pipe
  /// channels, whose cascade dispatch is the receptor consumer itself.
  private final Consumer < Object > stemCascade;

  /// The upstream pipe — what conduit.get(name) returns.
  final FsPipe < E > pipe;

  /// Downstream dispatch — receptors only, no STEM. Used by ingress receive()
  /// (which adds the version check) and by dispatchStem when walking ancestors
  /// (an ancestor must not trigger its own STEM walk — the chain is already
  /// flattened, so walking again would double-deliver at every level above).
  Consumer < Object > dispatch;

  /// Transit-side cascade dispatch — receptors + STEM (if applicable).
  /// Submitted directly to transit by fiber/flow terminals to bypass the
  /// version check on the cascade hot path. For per-pipe channels this is
  /// the same reference as `dispatch`. For hierarchical channels it is
  /// [#stemCascade], which fires this channel's receptors then walks the chain.
  Consumer < Object > cascadeDispatch;

  /// Version this channel was last built at.
  int builtVersion = -1;

  /// Per-subscriber receptor registrations — lazy, circuit-thread only.
  Map < FsSubscriber < E >, List < Consumer < Object > > > subscriberReceptors;

  FsChannel (
    Subject < Pipe < E > > subject,
    FsCircuit circuit,
    FsHub < E > hub,
    FsChannel < E >[] ancestors
  ) {
    this.subject = subject;
    this.circuit = circuit;
    this.hub = hub;
    this.ancestors = ancestors;
    this.stem = ancestors.length != 0;
    this.stemCascade = this.stem ? this::cascade : null;
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

  // ─── Dispatch (ingress path — version check) ───
  //
  // Called only from the ingress drain. The version check fires here because
  // an ingress emission may follow a subscriber change (which is itself
  // queued in ingress order). Per §5.4.1 relation 3 + §7.6.2, no subscriber
  // change can interleave during a cascade, so transit-side dispatch goes
  // straight to `cascadeDispatch` and skips this method entirely.

  @Override
  @jdk.internal.vm.annotation.ForceInline
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

  /// Transit-side entry point for a hierarchical channel — [#receive] without
  /// the version check (§5.4.1 + §7.6.2: subscriber state cannot change
  /// mid-cascade, and this channel was rebuilt before the cascade began).
  @SuppressWarnings ( "unchecked" )
  private void cascade ( Object emission ) {
    dispatchStem ( (E) emission );
  }

  // ─── Rebuild (cold path) ───

  @SuppressWarnings ( "unchecked" )
  private void rebuild () {
    FsSubscriber < E >[] currentSubs = hub.ensureSnapshot ();

    if ( subscriberReceptors == null ) {
      subscriberReceptors = new IdentityHashMap <> ();
    }

    Set < FsSubscriber < E > > activeSet = Collections.newSetFromMap ( new IdentityHashMap <> () );
    for ( FsSubscriber < E > sub : currentSubs ) {
      activeSet.add ( sub );
    }

    subscriberReceptors.keySet ().removeIf ( sub -> !activeSet.contains ( sub ) );

    for ( FsSubscriber < E > subscriber : currentSubs ) {
      if ( !subscriberReceptors.containsKey ( subscriber ) ) {
        FsRegistrar < E > registrar = new FsRegistrar <> ();
        try {
          subscriber.activate ( subject, registrar );
          subscriberReceptors.put ( subscriber, registrar.consumers () );
        } catch ( Throwable ignored ) {
          // §15.4 #3: "A failing subscriber callback is still considered
          // consumed for that subscription/channel pair: registration calls
          // completed before the failure remain registered, and the callback
          // MUST NOT be retried for that subscription/channel pair." So keep
          // whatever the callback managed to register before it threw — and
          // still record an entry, which is what suppresses the retry.
          subscriberReceptors.put ( subscriber, registrar.consumers () );
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
    for ( FsSubscriber < E > sub : currentSubs ) {
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

    // What transit cascade terminals submit. A hierarchical channel always
    // publishes its stem walk, even with no receptors of its own — an ancestor
    // may have some. A per-pipe channel publishes the receptor consumer
    // directly, and null (no receptors) sends the terminal down its fallback
    // path of submitting the channel itself.
    cascadeDispatch = stem ? stemCascade : dispatch;

    builtVersion = hub.subscriberVersion;
  }
}
