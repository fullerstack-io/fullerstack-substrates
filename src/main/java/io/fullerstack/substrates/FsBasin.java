package io.fullerstack.substrates;

import io.humainary.substrates.api.Substrates.Basin;
import io.humainary.substrates.api.Substrates.Fault;
import io.humainary.substrates.api.Substrates.NotNull;
import io.humainary.substrates.api.Substrates.Pipe;
import io.humainary.substrates.api.Substrates.Provided;
import io.humainary.substrates.api.Substrates.Subject;
import io.humainary.substrates.api.Substrates.Tenure;

import java.util.ArrayDeque;

import static java.util.Objects.requireNonNull;

/// **FsBasin** — 3.0 circuit-owned bounded buffer; the multi-value sibling of Cell.
///
/// Retains the most recent `capacity` values in emission order, evicting the
/// oldest when full. Values arrive through the emit-only feed ([#pipe()]) and
/// leave through queued [#drain(Pipe)] jobs — both execute on the owning
/// circuit's worker thread, so the buffer itself is worker-confined and needs
/// no synchronization.
///
/// A Basin is a [io.humainary.substrates.api.Substrates.Substrate], not a
/// Resource: it has identity but no close — its storage lives and dies with
/// the owning circuit.
@Provided
@Tenure ( Tenure.EPHEMERAL )
@SuppressWarnings ( "unchecked" )
public final class FsBasin < E > implements Basin < E > {

  private final Subject < Basin < E > > subject;
  private final FsCircuit               circuit;
  private final int                     capacity;

  /// Retained values — worker-confined: appended by the feed pipe's receptor
  /// and forwarded/cleared by drain jobs, both on the circuit worker thread.
  private final ArrayDeque < E > buffer;

  /// The emit-only feed; [#pipe()] returns this same instance on every call.
  private final Pipe < E > feed;

  /// Spec §11: when no name is supplied the basin inherits the owning
  /// circuit's name (null name delegates to the parent subject), with a
  /// fresh Id so distinct basins keep distinct identities.
  FsBasin ( FsSubject < ? > parent, FsCircuit circuit, int capacity ) {
    this.subject  = (Subject < Basin < E > >) (Subject < ? >) new FsSubject <> ( null, parent, Basin.class );
    this.circuit  = circuit;
    this.capacity = capacity;
    this.buffer   = new ArrayDeque <> ( Math.min ( capacity, 1024 ) );
    this.feed     = circuit.pipe ( this::append );
  }

  @NotNull
  @Override
  public Subject < Basin < E > > subject () {
    return subject;
  }

  @NotNull
  @Override
  public Pipe < E > pipe () {
    return feed;
  }

  /// Queues a drain on the owning circuit: each retained value is forwarded
  /// to `pipe` in emission order on the worker thread, then evicted. A
  /// same-circuit target receives worker-local transit emissions; a foreign
  /// target receives cross-circuit emissions. Draining is a queued emission,
  /// not a lifecycle operation — enqueued after close it is silently dropped.
  @Override
  public void drain ( @NotNull Pipe < ? super E > pipe ) {
    requireNonNull ( pipe );
    if ( ! ( pipe instanceof FsPipe < ? > ) ) {
      throw new Fault ( subject, "drain", "target pipe is not from this runtime provider" );
    }
    if ( circuit.isClosed () ) return;
    circuit.submitIngress (
      new FsCircuit.CircuitJob ( () -> {
        for ( E value : buffer ) {
          pipe.emit ( value );
        }
        buffer.clear ();
      } ),
      null
    );
  }

  /// Feed receptor — runs on the circuit worker; evicts the oldest retained
  /// value when at capacity before appending.
  private void append ( E value ) {
    if ( buffer.size () == capacity ) buffer.pollFirst ();
    buffer.addLast ( value );
  }

}
