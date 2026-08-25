package io.fullerstack.substrates;

import io.fullerstack.substrates.FsCircuit.ReceptorAdapter;
import io.humainary.substrates.api.Substrates.Cell;
import io.humainary.substrates.api.Substrates.Name;
import io.humainary.substrates.api.Substrates.NotNull;
import io.humainary.substrates.api.Substrates.Pipe;
import io.humainary.substrates.api.Substrates.Receptor;
import io.humainary.substrates.api.Substrates.Subject;

/// **FsCell** — circuit-owned state cell with safe publication.
///
/// Implements `Substrates.Cell<E>` (new in 2.7). A cell holds the latest
/// value accepted through its update pipe; the volatile slot guarantees
/// safe publication after the owning circuit processes each emission.
///
/// ## Implementation
///
/// The cell's pipe is a direct **receptor pipe** whose receptor simply writes
/// to the volatile slot. No conduit / channel / subscriber machinery is needed
/// — Cell's contract is "the latest emit becomes the value." Any thread can
/// `cell.pipe().emit(v)` and any thread can `cell.get()`; the receptor runs on
/// the circuit thread so the slot is always updated under deterministic
/// ordering.
///
/// The pipe is built here rather than through `circuit.pipe(Receptor)` for one
/// reason: §4.3 makes a pipe's enclosure "the subject of the source that owns
/// it", and the source that owns this one is the **cell**, not the circuit.
/// Routing it through the circuit factory parented it to the circuit and lost
/// the cell level of the path. The receiver is still a
/// [ReceptorAdapter], so the drain loop's `accept` call site keeps the single
/// concrete type its monomorphism depends on (see the marker-class note in
/// [FsCircuit]).
@SuppressWarnings ( "unchecked" )
public final class FsCell < E > implements Cell < E > {

  private final Subject < Cell < E > > subject;
  private final Pipe < E >             pipe;

  private volatile E value;

  public FsCell ( FsSubject < ? > parent, Name name, FsCircuit circuit, E initial ) {

    final FsSubject < ? > cellSubject =
      new FsSubject <> ( name, parent, Cell.class );

    final Receptor < E > update = emission -> {
      if ( emission != null ) value = emission;
    };

    this.subject = (Subject < Cell < E > >) (Subject < ? >) cellSubject;
    this.value   = initial;
    // §4.3: the update capability's enclosure is the cell's own subject.
    this.pipe    = new FsPipe <> ( new ReceptorAdapter < E > ( update ), circuit, null, cellSubject );

  }

  @NotNull
  @Override
  public Subject < Cell < E > > subject () {
    return subject;
  }

  @Override
  public E get () {
    return value;
  }

  @NotNull
  @Override
  public Pipe < E > pipe () {
    return pipe;
  }
}
