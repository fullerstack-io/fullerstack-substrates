package io.fullerstack.substrates;

import static io.humainary.substrates.api.Substrates.cortex;
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
/// Each Port operation enqueues a `CircuitJob` on the owning circuit. When
/// called from outside the worker the job is routed via the ingress queue;
/// when called from the worker (transit work, spec §5437-5443) the job is
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
/// - `update` fn throws → the throwable propagates out of the `CircuitJob`
///   to the IngressQueue's outer catch, where it is isolated as an external
///   callback failure. The previous value is retained.
/// - `update` fn returns null → surfaced as a `Fault(subject(), "update", …)`
///   so structured observers can distinguish framework-detected contract
///   violations from arbitrary user throws (§5454-5459). The previous value
///   is retained; the Fault itself rides the same isolation path.
@SuppressWarnings ( "unchecked" )
public final class FsPort < E > implements Port < E > {

  private final Subject < Port < E > > subject;
  private final FsCircuit              circuit;

  private E value;

  public FsPort ( FsSubject < ? > parent, Name name, FsCircuit circuit, E initial ) {
    this.subject = (Subject < Port < E > >) (Subject < ? >) new FsSubject <> ( name, parent, Port.class );
    this.circuit = circuit;
    this.value   = initial;
  }

  @NotNull
  @Override
  public Subject < Port < E > > subject () {
    return subject;
  }

  @Override
  public void emit ( @NotNull Pipe < ? super E > pipe ) {
    requireNonNull ( pipe );
    if ( ! ( pipe instanceof FsPipe < ? > ) ) {
      throw new Fault ( subject, "emit", "target pipe is not from this runtime provider" );
    }
    final Pipe < ? super E > target = pipe;
    submit ( () -> target.emit ( value ) );
  }

  @Override
  public void replace ( @NotNull E value ) {
    requireNonNull ( value );
    submit ( () -> this.value = value );
  }

  @Override
  public void update ( @NotNull UnaryOperator < E > fn ) {
    requireNonNull ( fn );
    submit ( () -> {
      E next = fn.apply ( value );
      if ( next == null ) throw new Fault ( subject, "update", "transform returned null" );
      value = next;
    } );
  }

  @Override
  public < A > void update ( @NotNull A arg, @NotNull BiFunction < ? super E, ? super A, ? extends E > fn ) {
    requireNonNull ( arg );
    requireNonNull ( fn );
    submit ( () -> {
      E next = fn.apply ( value, arg );
      if ( next == null ) throw new Fault ( subject, "update", "transform returned null" );
      value = next;
    } );
  }

  /// Spec §11.6: ops from the worker are transit work, ops from any other
  /// context are ingress work. After the owning circuit accepts close, ops
  /// MUST NOT throw and MUST NOT take effect — silently drop, same as
  /// `FsPipe.emit`.
  private void submit ( Runnable action ) {
    if ( circuit.closed ) return;
    FsCircuit.CircuitJob job = new FsCircuit.CircuitJob ( action );
    if ( circuit.onWorker () ) {
      circuit.submitTransit ( job, null );
    } else {
      circuit.submitIngress ( job, null );
    }
  }
}
