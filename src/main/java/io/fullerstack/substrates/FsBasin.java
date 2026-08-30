package io.fullerstack.substrates;

import io.humainary.substrates.api.Substrates.Basin;
import io.humainary.substrates.api.Substrates.Fault;
import io.humainary.substrates.api.Substrates.NotNull;
import io.humainary.substrates.api.Substrates.Pipe;
import io.humainary.substrates.api.Substrates.Provided;
import io.humainary.substrates.api.Substrates.Subject;
import io.humainary.substrates.api.Substrates.Tenure;


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
  private final int                     capacity;

  /// The drain capability, issued by the owning circuit through `Circuit.pipe(Receptor)`. A
  /// drain is an emission whose value is the target — the receptor is fixed and built once, so
  /// no carrier is allocated per drain. See `docs/CAPABILITIES.md`.
  private final Pipe < Pipe < ? super E > > drains;

  /// Retained values — worker-confined: appended by the feed pipe's receptor
  /// and forwarded/cleared by drain jobs, both on the circuit worker thread.
  ///
  /// A [DelayLine] rather than a deque: a Basin is a bounded ring that evicts its oldest entry
  /// at capacity, which is what a line already is. The deque this replaced allocated a node per
  /// retained value; the line writes in place and allocates nothing after construction.
  private final DelayLine buffer;

  /// The emit-only feed; [#pipe()] returns this same instance on every call.
  private final Pipe < E > feed;

  /// Spec §11: when no name is supplied the basin inherits the owning
  /// circuit's name (null name delegates to the parent subject), with a
  /// fresh Id so distinct basins keep distinct identities.
  FsBasin ( FsSubject < ? > parent, FsCircuit circuit, int capacity ) {
    this.subject  = (Subject < Basin < E > >) (Subject < ? >) new FsSubject <> ( null, parent, Basin.class );
    this.capacity = capacity;
    this.buffer   = DelayLine.of ( capacity );
    this.feed     = circuit.pipe ( this::append );
    this.drains   = circuit.< Pipe < ? super E > >pipe ( this::forward );
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
    // §15.1 provider mismatch. Tested by provider, not by concrete class: a Sink's channel is
    // provider-supplied but is not an FsPipe — it mints a Capture at the emit site, which a
    // queued receptor cannot do. Only `emit` is used below, so the class never mattered.
    if ( FsOperators.foreign ( pipe ) ) {
      throw new Fault ( subject, "drain", "target pipe is not from this runtime provider" );
    }
    // §5.3 routing, §9.1 post-close drop: both are FsCircuit.submit's admission. A drain raised on the
    // worker is transit work, so it stays inside the cascade that asked for it rather than landing
    // in a later ingress turn — draining from inside a receptor is the natural shape for this.
    drains.emit ( pipe );
  }

  /// Drain receptor — runs on the circuit worker; forwards every retained value in emission
  /// order, then evicts.
  private void forward ( Pipe < ? super E > target ) {
    final int retained = buffer.size ();
    for ( int i = 0; i < retained; i++ ) {
      target.emit ( (E) buffer.at ( i ) );
    }
    buffer.dropOldest ( retained );
  }

  /// Feed receptor — runs on the circuit worker; evicts the oldest retained
  /// value when at capacity before appending.
  private void append ( E value ) {
    buffer.append ( value );
  }

}
