package io.fullerstack.substrates;

import io.humainary.substrates.api.Substrates.Pipe;
import io.humainary.substrates.api.Substrates.Provided;
import io.humainary.substrates.api.Substrates.Receptor;
import io.humainary.substrates.api.Substrates.Registrar;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/// Registrar — collects consumers during subscriber activation.
///
/// Everything is Consumer<Object> on the hot path. A registered receptor is stored
/// behind one adapter lambda and invoked during the channel's dispatch walk; a
/// registered pipe is stored behind one [Admit] and *submitted* to its own circuit
/// — see [#register(Pipe)] for why the two kinds differ.
///
/// Enforces the @Temporal contract: register() is valid only during the callback, and only on
/// the circuit context that issued it — see [#checkLive].
@Provided
public final class FsRegistrar < E > implements Registrar < E > {

  private final List < Consumer < Object > > consumers = new ArrayList <> ();

  /// The worker that issued this registrar. §6.4 (3.6.0): a callback-scoped object "MUST NOT be
  /// used from any other execution context, including one the callback itself starts, even while
  /// the callback is still running", and for a Registrar that use MUST be detected (§15.1). A
  /// registrar is constructed in the channel's rebuild, on the owning worker, so the constructing
  /// thread is the issuing context.
  private final Thread owner = Thread.currentThread ();
  private boolean closed;

  /// Rejects a call after the callback returned, or from any thread but the issuing worker.
  ///
  /// The owner test runs first: `closed` is a plain field written by the worker, so a foreign
  /// thread's read of it proves nothing, while the thread identity is fixed at construction.
  private void checkLive () {
    if ( Thread.currentThread () != owner ) throw new IllegalStateException ( "Registrar used outside the circuit context that issued it" );
    if ( closed ) throw new IllegalStateException ( "Registrar is closed — register() only valid during callback" );
  }

  @Override
  @SuppressWarnings ( "unchecked" )
  public void register ( Receptor < ? super E > receptor ) {
    checkLive ();
    // Wrap Receptor in Consumer<Object> — this is the cold path (subscriber callback).
    // A receptor stays inline in the dispatch walk: §6.3 and §16.3 let a provider
    // store it directly, and it has no owning circuit to submit to — it is the channel's.
    consumers.add ( v -> receptor.receive ( (E) v ) );
  }

  /// §5.3: a registered pipe is delivered by *submission* to the pipe's own circuit.
  ///
  /// A registration is a pipe, and the only thing a channel can do with a pipe is emit
  /// into it (§6.1, §14). Emitting from the worker is transit work, "sequenced behind
  /// transit work already accepted for the current cascading chain … Transit is FIFO over
  /// every kind of operation it carries, not over emissions alone" (§5.3). Emitting into a
  /// pipe another circuit owns is ingress work *there* (§5.3: "Work submitted from another
  /// circuit's context is a caller context from the receiving circuit's point of view").
  /// Both are one decision, [FsCircuit#submit], which also carries §9.1's post-close drop.
  ///
  /// This provider used to unwrap the pipe to its bare receiver and run it inline in the
  /// dispatch walk. That put a registered delivery *ahead* of transit the same cascade had
  /// already accepted, and ran a foreign pipe's receiver on the wrong worker. §6.1 licenses
  /// a fast path only while "the public ordering … and circuit-context confinement
  /// contracts" hold, and that one held neither.
  @Override
  public void register ( Pipe < ? super E > pipe ) {
    checkLive ();
    if ( pipe instanceof FsPipe < ? > fp ) {
      consumers.add ( new Admit ( fp.circuit (), fp.receiver () ) );
    } else {
      // Unknown pipe kinds: their own emit is their own submission.
      consumers.add ( v -> pipe.emit ( (E) v ) );
    }
  }

  /// A registered pipe's delivery, as §5.3 defines it: a submission to the pipe's own circuit.
  ///
  /// Holds the circuit and the receiver rather than the [FsPipe] so the delivery reaches
  /// [FsCircuit#submit] directly, skipping `Pipe.emit`'s interface dispatch and its null check
  /// (the value was already accepted non-null upstream). One instance per registered pipe per
  /// subscriber×channel rebuild — cold; nothing is allocated per delivery.
  static final class Admit implements Consumer < Object > {

    private final FsCircuit           circuit;
    private final Consumer < Object > receiver;

    Admit ( FsCircuit circuit, Consumer < Object > receiver ) {
      this.circuit  = circuit;
      this.receiver = receiver;
    }

    @Override
    public void accept ( Object value ) {
      circuit.submit ( receiver, value );
    }
  }

  /// Returns the registered consumers and closes this registrar.
  public List < Consumer < Object > > consumers () {
    closed = true;
    return consumers;
  }

  /// Legacy accessor — returns consumers as receptors for compatibility.
  @SuppressWarnings ( "unchecked" )
  public List < Receptor < ? super E > > receptors () {
    closed = true;
    List < Receptor < ? super E > > result = new ArrayList <> ();
    for ( Consumer < Object > c : consumers ) {
      result.add ( v -> c.accept ( v ) );
    }
    return result;
  }
}
