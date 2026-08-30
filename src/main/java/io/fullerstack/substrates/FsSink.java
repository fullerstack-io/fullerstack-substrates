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

  /// Channels by name — the `Pool<Pipe<E>>` half of this type.
  ///
  /// Obtained from `Cortex.pool(Function)`, the API's prescribed factory, rather than written
  /// here. See [FsConduit] and `docs/CAPABILITIES.md`.
  private final Pool < Pipe < E > > channels = cortex ().pool ( this::materialize );

  @SuppressWarnings ( "unchecked" )
  public FsSink ( FsSubject < ? > parent, Name name, FsCircuit circuit,
                  Pipe < Capture < E > > endpoint ) {
    this.circuit  = circuit;
    // A pipe is owned by its circuit and emits to its circuit — including this sink's channels,
    // whose captures must therefore land on *this* circuit before going anywhere else. Normalising
    // the endpoint through `Circuit.pipe(Pipe)` is what the spec calls "the usual same-circuit pipe
    // optimization": a same-circuit endpoint comes back as-is, and a foreign one is wrapped in a
    // pipe owned here that forwards to it. Held raw, a cross-circuit endpoint meant a channel
    // emission bypassed this circuit entirely.
    this.endpoint = circuit.pipe ( endpoint );
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
    return channels.get ( name );
  }

  @NotNull
  @Override
  public < U > Pool < U > pool ( @NotNull Function < ? super Pipe < E >, ? extends U > fn ) {
    return new FsDerivedPool <> ( this, fn );
  }

  /// Builds one channel pipe — the pool owns storage, re-check and publication.
  private Pipe < E > materialize ( Name name ) {

    final FsSubject < Pipe < E > > pipeSubject =
      new FsSubject <> ( name, (FsSubject < ? >) subject, Pipe.class );

    return new SinkPipe <> ( pipeSubject, endpoint, circuit );
  }

  /// Sink channel pipe. On emit, mints a Capture and forwards to the endpoint.
  /// The endpoint's own pipe semantics handle queuing — this side just builds
  /// the carrier.
  static final class SinkPipe < E > implements Pipe < E > {

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
      // §11.1: `current()` is the caller's context for a value admitted from outside the circuit,
      // and the circuit's own for one emitted from within circuit processing. Both are already
      // `cortex().current()`: it is per-thread, and `workerLoop` binds this circuit's Current to
      // its worker as its first act, so on the worker the two are the same object. This once
      // branched on `circuit.onWorker()` to substitute `circuit.current()` — re-deriving by hand
      // a decision the runtime had already made, and getting the same answer either way.
      endpoint.emit ( new SinkCapture <> (
        emission, subject, cortex ().current ().subject (), cortex ().state () ) );
    }

    @Override
    public Subject < Pipe < E > > subject () {
      return subject;
    }

    /// The circuit that owns this channel. `Flow.pipe`/`Fiber.pipe` need it to honour their
    /// "on that pipe's circuit" contract, and a sink channel is the one pipe this provider mints
    /// that is not an [FsPipe] — it must stamp its Capture before the queue hop, which is what
    /// gives §11.1 the *caller's* context for an external emission.
    FsCircuit circuit () {
      return circuit;
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
