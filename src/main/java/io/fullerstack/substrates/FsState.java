package io.fullerstack.substrates;

import static io.humainary.substrates.api.Substrates.cortex;

import io.humainary.substrates.api.Substrates.Name;
import io.humainary.substrates.api.Substrates.New;
import io.humainary.substrates.api.Substrates.NotNull;
import io.humainary.substrates.api.Substrates.Provided;
import io.humainary.substrates.api.Substrates.Slot;
import io.humainary.substrates.api.Substrates.State;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;

/// An immutable collection of typed slots, stored most-recently-written first (SPEC §8.1).
///
/// Every write is a real **upsert**: "Writing a slot whose (name, type) matches an existing entry
/// MUST upsert that logical entry: the new slot becomes the most recently written entry and the
/// prior matching slot is removed" (§8.1, restated as a MUST in §16.3). The array therefore holds
/// **at most one slot per (name, type) pair** and its length is bounded by the number of unique
/// pairs ever written to the chain of states leading here — never by the number of writes.
///
/// The invariant is maintained on the write path, not recovered later by a compaction pass: a
/// state that collapses duplicates only on demand still reports them through `stream()`,
/// `iterator()` and `spliterator()`, and §8.1 makes those observations normative.
///
/// Immutability (§16.1#11) is structural: each write allocates a fresh spine and copies slot
/// references into it, so no ancestor state can observe a descendant's write. The spine is copied
/// rather than shared because the removal of the prior matching slot is a change in the middle of
/// the sequence, which a shared cons-chain cannot express.
@Provided
final class FsState implements State {

  private static final Slot < ? >[] EMPTY_ARRAY = new Slot < ? >[0];

  /// Shared singleton for empty state - internal use only (e.g.,
  /// Subject.state()).
  /// Note: Cortex.state() must use create() to comply with @New annotation
  /// contract.
  static final FsState EMPTY = new FsState ( EMPTY_ARRAY );

  /// Slots in most-recently-written-first order, one per (name, type) pair.
  /// Never null, never mutated, always exactly sized.
  private final Slot < ? >[] slots;

  /// Private constructor.
  private FsState ( Slot < ? >[] slots ) {
    this.slots = slots;
  }

  /// Factory method to create new empty State - required by @New annotation
  /// contract.
  static FsState create () {
    return new FsState ( EMPTY_ARRAY );
  }

  /// SPEC §8.2 slot matching: "Slot matching uses both name canonical identity (§1.2) and type
  /// value equality (§4.5)."
  ///
  /// Names are interned (§16.1#4), so canonical identity is reference identity. Slot types are
  /// class literals drawn from the fixed §8.3 set, so type *value* equality is likewise reference
  /// identity. There is deliberately **no** `isAssignableFrom` fallback: §4.5 defines type
  /// matching as equality, not as a subtype test, and a fallback would collapse distinct §8.3
  /// types that share a supertype.
  private static boolean matches ( Slot < ? > candidate, Name name, Class < ? > type ) {
    return candidate.name () == name && candidate.type () == type;
  }

  @Override
  public Iterator < Slot < ? > > iterator () {
    return Spliterators.iterator ( spliterator () );
  }

  @Override
  public Spliterator < Slot < ? > > spliterator () {
    return Arrays.spliterator ( slots );
  }

  /// The §8.1 / §16.3 upsert.
  ///
  /// 1. §16.3: "Writing a slot whose (name, type, value) already matches the existing entry MUST
  ///    return the same state instance." The rule is applied only when the matching entry is
  ///    already the most recently written one — rewriting an *older* entry has to move it to the
  ///    head (§8.1), and iteration order is observable, so that is a genuine change and MUST
  ///    produce a new state. Value equality is the boxed `equals`, which for `Float`/`Double` is
  ///    bitwise; without that a `NaN` slot could never satisfy the same-instance MUST.
  /// 2. Otherwise the new slot becomes the head and any prior slot with the same (name, type) is
  ///    *removed*, not shadowed. At most one such slot can exist, because every write maintains
  ///    that invariant.
  private State addSlot ( Slot < ? > slot ) {
    final Name        name = slot.name ();
    final Class < ? > type = slot.type ();
    final Slot < ? >[] src = slots;
    final int         len  = src.length;

    // (1) semantically equivalent write of the most recent entry (§16.3, §16.1#11)
    if ( len > 0
      && matches ( src[0], name, type )
      && Objects.equals ( src[0].value (), slot.value () ) ) {
      return this;
    }

    // (2) locate the prior entry for this (name, type) pair, if any
    int prior = -1;
    for ( int i = 0; i < len; i++ ) {
      if ( matches ( src[i], name, type ) ) {
        prior = i;
        break;
      }
    }

    if ( prior < 0 ) {
      // a new (name, type) pair: prepend, keeping every existing slot
      var arr = new Slot < ? >[len + 1];
      arr[0] = slot;
      System.arraycopy ( src, 0, arr, 1, len );
      return new FsState ( arr );
    }

    // an upsert: the new slot leads, the prior matching slot is dropped, and the
    // relative order of every other slot is preserved
    var arr = new Slot < ? >[len];
    arr[0] = slot;
    System.arraycopy ( src, 0, arr, 1, prior );
    System.arraycopy ( src, prior + 1, arr, prior + 1, len - prior - 1 );
    return new FsState ( arr );
  }

  @New ( conditional = true )
  @NotNull
  @Override
  @SuppressWarnings ( "unchecked" )
  public State state ( @NotNull Name name, int value ) {
    Objects.requireNonNull ( name, "name must not be null" );
    return addSlot ( new FsSlot <> ( name, value, (Class < Integer >) (Class < ? >) int.class ) );
  }

  @New ( conditional = true )
  @NotNull
  @Override
  @SuppressWarnings ( "unchecked" )
  public State state ( @NotNull Name name, long value ) {
    Objects.requireNonNull ( name, "name must not be null" );
    return addSlot ( new FsSlot <> ( name, value, (Class < Long >) (Class < ? >) long.class ) );
  }

  @New ( conditional = true )
  @NotNull
  @Override
  @SuppressWarnings ( "unchecked" )
  public State state ( @NotNull Name name, float value ) {
    Objects.requireNonNull ( name, "name must not be null" );
    return addSlot ( new FsSlot <> ( name, value, (Class < Float >) (Class < ? >) float.class ) );
  }

  @New ( conditional = true )
  @NotNull
  @Override
  @SuppressWarnings ( "unchecked" )
  public State state ( @NotNull Name name, double value ) {
    Objects.requireNonNull ( name, "name must not be null" );
    return addSlot ( new FsSlot <> ( name, value, (Class < Double >) (Class < ? >) double.class ) );
  }

  @New ( conditional = true )
  @NotNull
  @Override
  @SuppressWarnings ( "unchecked" )
  public State state ( @NotNull Name name, boolean value ) {
    Objects.requireNonNull ( name, "name must not be null" );
    return addSlot ( new FsSlot <> ( name, value, (Class < Boolean >) (Class < ? >) boolean.class ) );
  }

  @New ( conditional = true )
  @NotNull
  @Override
  public State state ( @NotNull Name name, @NotNull String value ) {
    Objects.requireNonNull ( name, "name must not be null" );
    Objects.requireNonNull ( value, "value must not be null" );
    return addSlot ( new FsSlot <> ( name, value, String.class ) );
  }

  @New ( conditional = true )
  @NotNull
  @Override
  public State state ( @NotNull Name name, @NotNull Name value ) {
    Objects.requireNonNull ( name, "name must not be null" );
    Objects.requireNonNull ( value, "value must not be null" );
    return addSlot ( new FsSlot <> ( name, value, Name.class ) );
  }

  @New ( conditional = true )
  @NotNull
  @Override
  public State state ( @NotNull Name name, @NotNull State value ) {
    Objects.requireNonNull ( name, "name must not be null" );
    Objects.requireNonNull ( value, "value must not be null" );
    return addSlot ( new FsSlot <> ( name, value, State.class ) );
  }

  @New ( conditional = true )
  @NotNull
  @Override
  public State state ( @NotNull Slot < ? > slot ) {
    Objects.requireNonNull ( slot, "slot must not be null" );
    return addSlot ( slot );
  }

  @New ( conditional = true )
  @NotNull
  @Override
  public State state ( @NotNull Enum < ? > value ) {
    Objects.requireNonNull ( value, "value must not be null" );
    Name slotName = cortex ().name ( value.getDeclaringClass () );
    Name slotValue = FsName.fromEnum ( value );
    return addSlot ( new FsSlot <> ( slotName, slotValue, Name.class ) );
  }

  @Override
  public Stream < Slot < ? > > stream () {
    return Arrays.stream ( slots );
  }

  /// SPEC §16.3: "The `value` operation returns the value of the slot matching the given slot's
  /// (name, type) pair, or the template slot's own value when no match exists." Upsert makes the
  /// match unique, so the first hit is the only hit.
  @Override
  @SuppressWarnings ( "unchecked" )
  public < T > T value ( Slot < T > slot ) {
    final Name        name = slot.name ();
    final Class < T > type = slot.type ();
    final Slot < ? >[] src = slots;
    for ( int i = 0; i < src.length; i++ ) {
      if ( matches ( src[i], name, type ) ) {
        return (T) src[i].value ();
      }
    }
    return slot.value ();
  }

}
