package io.fullerstack.substrates;

import static io.humainary.substrates.api.Substrates.cortex;
import static java.util.Objects.requireNonNull;

import io.humainary.substrates.api.Substrates;
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
  private final FsCircuit             circuit;

  private E value;

  public FsPin ( FsSubject < ? > parent, Name name, FsCircuit circuit, E initial ) {
    this.subject = (Subject < Pin < E > >) (Subject < ? >) new FsSubject <> ( name, parent, Pin.class );
    this.circuit = circuit;
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
    guard ( "get" );
    return value;
  }

  @Override
  public void set ( @NotNull E value ) {
    requireNonNull ( value );
    guard ( "set" );
    this.value = value;
  }

  /// §11.6's owner-context guard.
  ///
  /// The spec states the idiom in terms of §11.3's `Current`: "compare `Cortex.current()`
  /// against the pin's owning circuit's `current()`". The comparison below is that check in
  /// its Java projection — §11.3 names `Thread.currentThread()` as the Java mechanism for a
  /// `Current`, and this circuit's `Current` is interned against its worker — so it answers
  /// the same question without a per-access lookup, which matters because §11.6 promises
  /// `get`/`set` with "no queueing, no admission overhead".
  ///
  /// The `Current` is used where it earns its cost: on the **cold** fault path, so the error
  /// names the offending context rather than describing it in prose (§15.3).
  private void guard ( String operation ) {
    if ( cortex ().current () != circuit.current () ) {
      // IllegalStateException, NOT Fault. §11.6 calls this an "illegal-context-use error
      // (§15.1)", but the Java binding for THIS operation is fixed by the API's own contract
      // — `Pin.get`/`Pin.set` are declared `@throws IllegalStateException` — and
      // `PinContractTest` asserts the type. Appendix A.2 makes the mechanism a binding
      // detail; the category is what travels.
      throw new IllegalStateException (
        "pin access requires the owning circuit's context; called from "
        + Substrates.cortex ().current ().subject () );
    }
  }
}
