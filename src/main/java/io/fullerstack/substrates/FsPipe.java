package io.fullerstack.substrates;

import io.humainary.substrates.api.Substrates.Name;
import io.humainary.substrates.api.Substrates.NotNull;
import io.humainary.substrates.api.Substrates.Pipe;
import io.humainary.substrates.api.Substrates.Provided;
import io.humainary.substrates.api.Substrates.Subject;

import java.util.function.Consumer;

import static io.humainary.substrates.api.Substrates.cortex;
import static java.util.Objects.requireNonNull;

/// Pipe — the emission carrier.
///
/// Holds a receiver and a circuit. emit(v) enqueues [receiver, v]
/// to the circuit's queue. That's it.
///
/// The receiver is typically a Channel (for conduit pipes) but can be
/// any Consumer<Object> (flow receivers, receptor adapters).
///
/// pipe.pipe(flow) creates a new pipe whose receiver runs the flow
/// chain on the circuit thread, then delivers to the original target.
@Provided
public final class FsPipe < E > implements Pipe < E > {

  private final Consumer < Object > receiver;
  private final FsCircuit           circuit;

  private volatile Subject < Pipe < E > > subject;

  /// Conduit pipe constructor — receiver is a channel.
  FsPipe ( FsChannel < E > channel, FsCircuit circuit ) {
    this.receiver = channel;
    this.circuit = circuit;
  }

  /// General constructor — for flow pipes, circuit.pipe(receptor), etc.
  FsPipe ( Consumer < Object > receiver, FsCircuit circuit ) {
    this.receiver = receiver;
    this.circuit = circuit;
  }

  /// 2.7: named-pipe constructor. Pre-seeds the subject with the given
  /// name + circuit-subject parent so diagnostic surfaces (logs, dumps,
  /// observatories) can identify this pipe by its caller-supplied name.
  @SuppressWarnings ( "unchecked" )
  FsPipe ( Consumer < Object > receiver, FsCircuit circuit, Name name, FsSubject < ? > parent ) {
    this.receiver = receiver;
    this.circuit = circuit;
    this.subject = (Subject < Pipe < E > >) (Subject < ? >) new FsSubject <> ( name, parent, Pipe.class );
  }

  /// §4.3: "A pipe's enclosure is the subject of the source that owns it —
  /// typically a conduit, but any Source subtype. The source's enclosure is in
  /// turn the circuit's subject … This produces a fully-qualified path for every
  /// component."
  ///
  /// A conduit pipe carries the channel's subject, which the conduit already
  /// minted under itself. Every other anonymous pipe is minted directly by a
  /// circuit — `circuit.pipe()`, `pipe(Receptor)`, the cross-circuit forwarder
  /// of `pipe(Pipe)`, the fan-out of `pipe(List)` — so the circuit's subject is
  /// the owning source, and the omitted name inherits from it (§16.3: the
  /// unnamed form uses the owning circuit's name). Previously this branch built
  /// `FsSubject(null, null, Pipe.class)`, an orphan with no enclosure whose
  /// `name()` dereferenced a null parent.
  ///
  /// Minted under the monitor rather than racily: §4.2/§4.3 make the identifier
  /// a per-instance identity, so two concurrent callers must not walk away with
  /// two subjects carrying two ids for one pipe.
  @Override
  public Subject < Pipe < E > > subject () {
    Subject < Pipe < E > > s = subject;
    if ( s == null ) {
      synchronized ( this ) {
        s = subject;
        if ( s == null ) {
          s = derive ();
          subject = s;
        }
      }
    }
    return s;
  }

  @SuppressWarnings ( "unchecked" )
  private Subject < Pipe < E > > derive () {
    return ( receiver instanceof FsChannel < ? > ch )
      ? (Subject < Pipe < E > >) (Subject < ? >) ch.subject ()
      : (Subject < Pipe < E > >) (Subject < ? >)
        new FsSubject <> ( null, (FsSubject < ? >) circuit.subject (), Pipe.class );
  }

  Consumer < Object > receiver () {
    return receiver;
  }

  FsCircuit circuit () {
    return circuit;
  }

  // ─── Emit ───

  /// Routes the emission per SPEC §5.3 dual-queue model:
  /// - External threads → ingress queue (shared, MPSC)
  /// - Circuit/worker thread (cascade re-entry from within processing) → transit
  ///   queue (single-thread, takes priority over ingress)
  ///
  /// Cascade priority is the spec's mechanism for ensuring effects of an emission
  /// resolve before the next external input is processed.
  @Override
  public void emit ( @NotNull E emission ) {
    requireNonNull ( emission, "emission must not be null" );
    if ( circuit.closed ) return;
    if ( circuit.onWorker () ) {
      circuit.submitTransit ( receiver, emission );
    } else {
      circuit.submitIngress ( receiver, emission );
    }
  }

}
