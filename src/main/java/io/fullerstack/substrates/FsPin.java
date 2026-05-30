package io.fullerstack.substrates;

import static java.util.Objects.requireNonNull;

import io.humainary.substrates.api.Substrates.Name;
import io.humainary.substrates.api.Substrates.NotNull;
import io.humainary.substrates.api.Substrates.Pin;
import io.humainary.substrates.api.Substrates.Subject;

/// **FsPin** — 2.9 circuit-owned, owner-context-guarded state handle.
///
/// Grants immediate `get()`/`set()` access from inside the owning circuit's
/// worker thread; any other thread raises [IllegalStateException] (spec
/// §5118-5172). Per spec §5175, the guard is a `Thread.currentThread()`
/// intrinsic plus a reference comparison against the cached worker thread —
/// JIT-hoisted out of hot loops, so the per-access cost converges to a plain
/// field read. No volatile: confinement, not synchronization, provides the
/// safety guarantee.
@SuppressWarnings ( "unchecked" )
public final class FsPin < E > implements Pin < E > {

  private final Subject < Pin < E > > subject;
  private final Thread                worker;

  private E value;

  public FsPin ( FsSubject < ? > parent, Name name, FsCircuit circuit, E initial ) {
    this.subject = (Subject < Pin < E > >) (Subject < ? >) new FsSubject <> ( name, parent, Pin.class );
    this.worker  = circuit.worker ();
    this.value   = initial;
  }

  @NotNull
  @Override
  public Subject < Pin < E > > subject () {
    return subject;
  }

  @NotNull
  @Override
  public E get () {
    guard ();
    return value;
  }

  @Override
  public void set ( @NotNull E value ) {
    requireNonNull ( value );
    guard ();
    this.value = value;
  }

  private void guard () {
    if ( Thread.currentThread () != worker ) {
      throw new IllegalStateException (
        "Pin access requires the owning circuit's worker thread" );
    }
  }
}
