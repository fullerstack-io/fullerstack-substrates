package io.fullerstack.substrates;

import io.humainary.substrates.api.Substrates.Capture;
import io.humainary.substrates.api.Substrates.Current;
import io.humainary.substrates.api.Substrates.Name;
import io.humainary.substrates.api.Substrates.NotNull;
import io.humainary.substrates.api.Substrates.Pipe;
import io.humainary.substrates.api.Substrates.Pool;
import io.humainary.substrates.api.Substrates.Provided;
import io.humainary.substrates.api.Substrates.Sink;
import io.humainary.substrates.api.Substrates.State;
import io.humainary.substrates.api.Substrates.Subject;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.function.Function;

import static io.humainary.substrates.api.Substrates.cortex;

/// 2.10 Sink — output-closed dual of Conduit (SPEC §6883-6887).
///
/// A pool of named pipes whose destination is a single endpoint pipe fixed
/// at creation. Each emission into a sink channel is wrapped in a `Capture`
/// (bearing the channel's subject, the emitting `Current`, the value, and
/// the provider's per-emission `State`) and forwarded to the endpoint.
///
/// Unlike `Conduit`, a Sink has no subscriber set — the destination is the
/// endpoint, not a list of subscribers. It is a `Substrate`, not a `Resource`:
/// no `close()`. The channels are valid for the circuit's lifetime; the
/// endpoint is caller-owned.
@Provided
public final class FsSink < E > implements Sink < E > {

  private final FsCircuit            circuit;
  private final Pipe < Capture < E > > endpoint;
  private final Subject < Sink < E > > subject;

  /// Channels by name — copy-on-write for thread-safe reads, matching FsConduit.
  private volatile Map < Name, Pipe < E > > channels;

  /// Last-lookup cache.
  private Name        lastLookupName;
  private Pipe < E >  lastLookupPipe;

  @SuppressWarnings ( "unchecked" )
  public FsSink ( FsSubject < ? > parent, Name name, FsCircuit circuit,
                  Pipe < Capture < E > > endpoint ) {
    this.circuit  = circuit;
    this.endpoint = endpoint;
    this.subject  = (Subject < Sink < E > >) (Subject < ? >) new FsSubject <> ( name, parent, Sink.class );
  }

  @Override
  public Subject < Sink < E > > subject () {
    return subject;
  }

  // ─── Pool<Pipe<E>> ───

  @NotNull
  @Override
  public Pipe < E > get ( @NotNull Name name ) {
    Name last = lastLookupName;
    if ( last != null && name == last ) {
      return lastLookupPipe;
    }
    Map < Name, Pipe < E > > map = channels;
    if ( map != null ) {
      Pipe < E > cached = map.get ( name );
      if ( cached != null ) {
        lastLookupName = name;
        lastLookupPipe = cached;
        return cached;
      }
    }
    return createChannel ( name );
  }

  @NotNull
  @Override
  public < U > Pool < U > pool ( @NotNull Function < ? super Pipe < E >, ? extends U > fn ) {
    return new FsDerivedPool <> ( this, fn );
  }

  private synchronized Pipe < E > createChannel ( Name name ) {
    if ( channels == null ) {
      channels = new IdentityHashMap <> ();
    }
    Pipe < E > cached = channels.get ( name );
    if ( cached != null ) {
      lastLookupName = name;
      lastLookupPipe = cached;
      return cached;
    }

    FsSubject < Pipe < E > > pipeSubject =
      new FsSubject <> ( name, (FsSubject < ? >) subject, Pipe.class );

    Pipe < E > pipe = new SinkPipe <> ( pipeSubject, endpoint, circuit );

    Map < Name, Pipe < E > > newMap = new IdentityHashMap <> ( channels );
    newMap.put ( name, pipe );
    channels = newMap;

    lastLookupName = name;
    lastLookupPipe = pipe;

    return pipe;
  }

  /// Sink channel pipe. On emit, mints a Capture and forwards to the endpoint.
  /// The endpoint's own pipe semantics handle queuing — this side just builds
  /// the carrier.
  private static final class SinkPipe < E > implements Pipe < E > {

    private final Subject < Pipe < E > > subject;
    private final Pipe < Capture < E > > endpoint;
    private final FsCircuit              circuit;

    SinkPipe ( Subject < Pipe < E > > subject, Pipe < Capture < E > > endpoint, FsCircuit circuit ) {
      this.subject  = subject;
      this.endpoint = endpoint;
      this.circuit  = circuit;
    }

    @Override
    public void emit ( @NotNull E emission ) {
      // Capture.current() per spec §392-414:
      //   - for a value emitted on a transit cascade inside the circuit worker,
      //     the circuit's own context (== Circuit#current())
      //   - for a value admitted from outside the circuit (external thread,
      //     or another circuit's worker), the caller's context
      // Thread identity distinguishes the two cases. cortex().current() is
      // per-thread, so it gives the caller's context for external threads and
      // the worker's thread context otherwise; we explicitly substitute
      // circuit.current() on the cascade path so it coincides with the
      // circuit identity as the spec requires.
      Subject < Current > current = circuit.worker () == Thread.currentThread ()
        ? circuit.current ().subject ()
        : cortex ().current ().subject ();
      State state = cortex ().state ();
      endpoint.emit ( new SinkCapture <> ( emission, subject, current, state ) );
    }

    @Override
    public Subject < Pipe < E > > subject () {
      return subject;
    }
  }

  /// Immutable capture carrier for sink emissions. The four-tuple matches the
  /// 2.10 Capture surface (`emission`, `subject`, `current`, `state`).
  private record SinkCapture < E > (
    E                            emission,
    Subject < Pipe < E > >       subject,
    Subject < Current >          current,
    State                        state
  ) implements Capture < E > {}
}
