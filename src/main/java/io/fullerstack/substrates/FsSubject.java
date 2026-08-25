package io.fullerstack.substrates;

import io.humainary.substrates.api.Substrates.Circuit;
import io.humainary.substrates.api.Substrates.Extent;
import io.humainary.substrates.api.Substrates.Fault;
import io.humainary.substrates.api.Substrates.Id;
import io.humainary.substrates.api.Substrates.Identity;
import io.humainary.substrates.api.Substrates.Name;
import io.humainary.substrates.api.Substrates.Provided;
import io.humainary.substrates.api.Substrates.State;
import io.humainary.substrates.api.Substrates.Subject;
import io.humainary.substrates.api.Substrates.Substrate;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;

/// The identity of a substrate.
///
/// An omitted (`null`) name inherits the enclosure's name, which is what §11.2
/// (Cell), §11.5 (Port) and §11.6 (Pin) mean by "when no name is supplied, the
/// implementation uses the owning circuit's name". That inheritance is resolved
/// **eagerly at construction**, not on every read: §16.3 declares
/// `Subject.name(): Name` with no absence, so the accessor has to be total, and
/// resolving it lazily made it partial — an unnamed subject with no enclosure
/// dereferenced a null parent and threw. The Rust projection
/// (`identity/subject.rs`) reaches the same conclusion for the same reason.
@Identity
@Provided
@SuppressWarnings ( {"unchecked"} )
public final class FsSubject < S extends Substrate < S > > implements Subject < S > {

  /// Counter for unique subject IDs - cheaper than UUID.randomUUID() (~5ns vs
  /// ~300ns).
  private static final AtomicLong ID_COUNTER = new AtomicLong ();

  /// §16.3: a subject with neither a name nor an enclosure to inherit one from
  /// still owes a valid non-empty name. "That default name need not be unique;
  /// uniqueness is provided by the subject identifier (§4.2)."
  private static final Name DEFAULT_NAME = FsName.intern ( "substrates" );

  /// Identifier token (§4.2, §4.3).
  ///
  /// A final class, **not** a record: §4.2 makes identifier comparison canonical
  /// identity (§1.2), and the API annotates `Id` `@Identity` — "a type whose
  /// instances can be compared by (object) reference for equality". A record
  /// derives value-based `equals`/`hashCode` from `value`, which leaves
  /// `hashCode()` unequal to the reference hash and so is not identity-based.
  /// Inheriting `Object`'s pair is exactly the required semantics.
  private static final class FsId implements Id {

    private final long value;

    FsId ( long value ) {
      this.value = value;
    }

    long value () {
      return value;
    }

    @Override
    public String toString () {
      return String.valueOf ( value );
    }

  }

  private final Name            name; // resolved at construction — never null
  private final FsSubject < ? > parent;
  private final Class < ? >     type;
  private final FsId            id; // unique id
  private       State           state = FsState.EMPTY; // mutable on circuit thread

  /// Creates a root subject with the given name and type.
  public FsSubject ( Name name, Class < ? > type ) {
    this.name = name != null ? name : DEFAULT_NAME;
    this.parent = null;
    this.type = type;
    this.id = new FsId ( ID_COUNTER.getAndIncrement () );
  }

  /// Creates a child subject with the given name, parent, and type.
  /// A null name inherits the parent's name (§11.2/§11.5/§11.6); with no parent
  /// either, it takes the §16.3 default so that [#name()] stays total.
  public FsSubject ( Name name, FsSubject < ? > parent, Class < ? > type ) {
    this.name = name != null ? name
      : parent != null ? parent.name ()
      : DEFAULT_NAME;
    this.parent = parent;
    this.type = type;
    this.id = new FsId ( ID_COUNTER.getAndIncrement () );
  }

  @Override
  public Id id () {
    return id;
  }

  @Override
  public Name name () {
    return name;
  }

  @Override
  public State state () {
    return state;
  }

  /// Update the State on this Subject. Called on the circuit thread —
  /// no synchronisation needed. State is immutable; this replaces the
  /// reference to the current State with a new one.
  public void state ( State state ) {
    this.state = state;
  }

  @Override
  public Class < S > type () {
    return (Class < S >) type;
  }

  @Override
  public Optional < Subject < ? > > enclosure () {
    return Optional.ofNullable ( parent );
  }

  /// Optimized within() — walks parent field directly instead of
  /// using default Extent.within() which allocates Optional per level.
  @Override
  public boolean within ( final Extent < ?, ? > enclosure ) {
    requireNonNull ( enclosure, "enclosure must not be null" );
    for ( FsSubject < ? > current = parent; current != null; current = current.parent ) {
      if ( current == enclosure ) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int compareTo ( Subject < ? > other ) {
    requireNonNull ( other, "other must not be null" );
    if ( this == other ) return 0;
    if ( ! ( other instanceof FsSubject < ? > fs ) ) {
      // §15.1 provider mismatch, MUST detect. A foreign subject carries no
      // identifier this runtime can order against — §4.2 makes identifier
      // comparison canonical identity, and a canonical identity minted by
      // another provider is meaningless here, so falling back to a name
      // comparison would fabricate a total order that is neither stable nor
      // antisymmetric with the foreign side's own compareTo. §15.3 names the
      // receiver as the fault's subject and renders the offending argument.
      throw new Fault (
        this,
        "compareTo",
        "subject is not from this runtime provider: " + other.getClass ().getName () );
    }
    return Long.compare ( this.id.value (), fs.id.value () );
  }

  /// Finds the circuit ancestor in the subject hierarchy.
  /// Returns null if no Circuit type ancestor is found.
  public FsSubject < ? > findCircuitAncestor () {
    FsSubject < ? > current = this;
    while ( current != null ) {
      if ( current.type == Circuit.class ) {
        return current;
      }
      current = current.parent;
    }
    return null;
  }

  /// Optimized part() — caches the formatted string to avoid repeated
  /// String.formatted() + type().getSimpleName() reflection on every call.
  @Override
  public String part () {
    return "Subject[name=" + name () + ",type=" + type.getSimpleName () + ",id=" + id + "]";
  }

  /// Optimized path() — walks parent chain directly instead of
  /// using default Extent.foldTo() which allocates Optional per level.
  @Override
  public CharSequence path () {
    if ( parent == null ) {
      return part ();
    }
    StringBuilder sb = new StringBuilder ();
    buildPath ( sb );
    return sb;
  }

  private void buildPath ( StringBuilder sb ) {
    if ( parent != null ) {
      parent.buildPath ( sb );
      sb.append ( '/' );
    }
    sb.append ( part () );
  }

  @Override
  public String toString () {
    return path ().toString ();
  }

}
