package io.fullerstack.substrates;

import static java.util.Objects.requireNonNull;

import io.humainary.substrates.api.Substrates.Fault;
import io.humainary.substrates.api.Substrates.Name;
import io.humainary.substrates.api.Substrates.NotNull;
import io.humainary.substrates.api.Substrates.Pipe;
import io.humainary.substrates.api.Substrates.Port;
import io.humainary.substrates.api.Substrates.Subject;

import java.util.function.BiFunction;
import java.util.function.UnaryOperator;

/// **FsPort** — 2.9 circuit-owned, queued mutation handle without read.
///
/// Each Port operation emits the work into the owning circuit's task pipe. When
/// called from outside the worker the admission is routed via the ingress queue;
/// when called from the worker (transit work, spec §5437-5443) the admission is
/// routed via the transit queue, so it lands *after* pending cascade work
/// rather than jumping ahead of it.
///
/// The stored value is read and written **only** on the worker thread, so
/// the field is plain — no volatile, no atomics. Initial-state publication
/// to the first worker access is provided by the queue's enqueue → dequeue
/// happens-before edge.
///
/// Transform failures (§15.4):
///
/// - `update` fn throws → the throwable propagates out of the queued task
///   to the IngressQueue's outer catch, where it is isolated as an external
///   callback failure. The previous value is retained.
/// - `update` fn returns null → surfaced as a `Fault(subject(), "update", …)`
///   so structured observers can distinguish framework-detected contract
///   violations from arbitrary user throws (§5454-5459). The previous value
///   is retained; the Fault itself rides the same isolation path.
@SuppressWarnings ( "unchecked" )
public final class FsPort < E > implements Port < E > {

  private final Subject < Port < E > > subject;

  /// The three capabilities a Port needs, each issued by the owning circuit through
  /// `Circuit.pipe(Receptor)` — the API's own way to obtain one. A Port holds these and not the
  /// circuit that issued them: a `Pipe` can do exactly one thing, where a circuit reference also
  /// carries `await`, `close`, `isClosed` and `pulse`. See `docs/CAPABILITIES.md`.
  ///
  /// Each operation is an **emission** whose value is the only part that varies between calls —
  /// the new value for `replace`, the caller's own function for `update`, the target for `emit`.
  /// The receptor is fixed and built once, so three of the four operations allocate nothing
  /// beyond what the caller already holds. The earlier shape wrapped every call in a carrier
  /// that existed only to hold work the value slot was already there to carry.
  private final Pipe < E >                replacements;
  private final Pipe < UnaryOperator < E > > updates;
  private final Pipe < Pipe < ? super E > >  emissions;

  private E value;

  public FsPort ( FsSubject < ? > parent, Name name, FsCircuit circuit, E initial ) {
    this.subject      = (Subject < Port < E > >) (Subject < ? >) new FsSubject <> ( name, parent, Port.class );
    this.value        = initial;
    this.replacements = circuit.pipe ( replacement -> value = replacement );
    this.updates      = circuit.pipe ( this::transform );
    this.emissions    = circuit.< Pipe < ? super E > >pipe ( target -> target.emit ( value ) );
  }

  /// Applies one queued transform on the worker. §15.4: a null result is a framework-detected
  /// contract violation rather than an arbitrary user throw, so it surfaces as a Fault and the
  /// previous value is retained.
  private void transform ( UnaryOperator < E > fn ) {
    final E next = fn.apply ( value );
    if ( next == null ) throw new Fault ( subject, "update", "transform returned null" );
    value = next;
  }

  @NotNull
  @Override
  public Subject < Port < E > > subject () {
    return subject;
  }

  @Override
  public void emit ( @NotNull Pipe < ? super E > pipe ) {
    requireNonNull ( pipe );
    // §15.1 provider mismatch. Tested by provider, not by concrete class: a Sink's channel is
    // provider-supplied but is not an FsPipe — it mints a Capture at the emit site, which a
    // queued receptor cannot do. Only `emit` is used below, so the class never mattered.
    if ( FsOperators.foreign ( pipe ) ) {
      throw new Fault ( subject, "emit", "target pipe is not from this runtime provider" );
    }
    emissions.emit ( pipe );
  }

  @Override
  public void replace ( @NotNull E value ) {
    requireNonNull ( value );
    replacements.emit ( value );
  }

  @Override
  public void update ( @NotNull UnaryOperator < E > fn ) {
    requireNonNull ( fn );
    updates.emit ( fn );
  }

  /// The one operation that still allocates: two things vary per call, and an emission carries
  /// one. The capture is the carrier, and it replaces the carrier the other three shed.
  @Override
  public < A > void update ( @NotNull A arg, @NotNull BiFunction < ? super E, ? super A, ? extends E > fn ) {
    requireNonNull ( arg );
    requireNonNull ( fn );
    updates.emit ( current -> fn.apply ( current, arg ) );
  }

}
