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
/// Each Port operation enqueues a `CircuitJob` on the owning circuit. The
/// stored value is read and written **only** on the worker thread, so the
/// field is plain (no volatile / atomics). Transform failures (throws or
/// `null` returns) are isolated under spec §15.4: the previous value is
/// retained and the operation is dropped silently from the caller's view.
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
    circuit.submitIngress ( new FsCircuit.CircuitJob ( () -> target.emit ( value ) ), null );
  }

  @Override
  public void replace ( @NotNull E value ) {
    requireNonNull ( value );
    circuit.submitIngress ( new FsCircuit.CircuitJob ( () -> this.value = value ), null );
  }

  @Override
  public void update ( @NotNull UnaryOperator < E > fn ) {
    requireNonNull ( fn );
    circuit.submitIngress ( new FsCircuit.CircuitJob ( () -> {
      try {
        E next = fn.apply ( value );
        if ( next != null ) value = next;
      } catch ( Throwable ignored ) {
        // §15.4 failure isolation: retain previous value, drop the operation
      }
    } ), null );
  }

  @Override
  public < A > void update ( @NotNull A arg, @NotNull BiFunction < ? super E, ? super A, ? extends E > fn ) {
    requireNonNull ( arg );
    requireNonNull ( fn );
    circuit.submitIngress ( new FsCircuit.CircuitJob ( () -> {
      try {
        E next = fn.apply ( value, arg );
        if ( next != null ) value = next;
      } catch ( Throwable ignored ) {
        // §15.4 failure isolation
      }
    } ), null );
  }
}
