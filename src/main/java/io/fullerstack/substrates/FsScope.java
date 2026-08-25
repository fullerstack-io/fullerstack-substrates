package io.fullerstack.substrates;

import io.humainary.substrates.api.Substrates.Closure;
import io.humainary.substrates.api.Substrates.Extent;
import io.humainary.substrates.api.Substrates.Fault;
import io.humainary.substrates.api.Substrates.Idempotent;
import io.humainary.substrates.api.Substrates.Name;
import io.humainary.substrates.api.Substrates.NotNull;
import io.humainary.substrates.api.Substrates.Provided;
import io.humainary.substrates.api.Substrates.Resource;
import io.humainary.substrates.api.Substrates.Scope;
import io.humainary.substrates.api.Substrates.Subject;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

/// A container for grouping resources with coordinated lifecycle management.
///
/// Scope provides structured resource management - resources registered with
/// a scope are automatically closed when the scope closes. Resources are closed
/// in reverse registration order (LIFO).
///
/// ## Key Features
///
/// - **Automatic cleanup**: All registered resources closed on scope close
/// - **LIFO ordering**: Last registered is first closed (§9.2, §16.1#10)
/// - **Child scopes**: Create nested scopes for hierarchical management
/// - **Idempotent close**: Safe to call close() multiple times
/// - **Terminal**: §9.2 — "Transition to closed is terminal"; every management
///   operation afterwards raises a [Fault] (App. A.2: `Fault` is the Java
///   projection of a synchronously detected substrate runtime error, and a bare
///   `IllegalStateException` cannot carry the §15.3 receiver subject).
///
/// ## Concurrency
///
/// The terminal transition is a single compare-and-set, and every list is taken
/// under `lock` and mutated there — never read outside it and acted on. A
/// check-then-act on the closed flag lets a `register` landing inside a
/// concurrent `close` push a resource onto a list the close has already drained,
/// which strands that resource for the life of the process.
///
/// **No member's `close()` runs while `lock` is held** (§15.4 #2): a member's
/// close is arbitrary code that may reach back into this scope.
///
/// @see Resource
@Provided
final class FsScope implements Scope {

  /// Cached Name for anonymous scopes.
  static final Name SCOPE_NAME = FsName.intern ( "scope" );

  /// The name for this scope (null for anonymous).
  private final Name name;

  /// Parent scope (for hierarchy).
  private final FsScope parent;

  /// Eagerly created subject — avoids the unsynchronised lazy race (FsSubject
  /// constructor calls ID_COUNTER.getAndIncrement() so concurrent racing
  /// creators would assign distinct ids and one would orphan).
  private final Subject < Scope > subject;

  /// Guards `resources`, `children` and `closureCache`.
  private final Object lock = new Object ();

  /// Registered resources (closed in reverse order). Lazily initialized.
  private List < Resource > resources;

  /// Child scopes. Lazily initialized.
  private List < FsScope > children;

  /// Cache of closures per resource (cleared when consumed). Lazily initialized.
  private Map < Resource, FsClosure < ? > > closureCache;

  /// §9.2's terminal flag. A compare-and-set, not a check-then-act.
  private final AtomicBoolean closed = new AtomicBoolean ();

  /// Creates a new root scope with the given name.
  FsScope ( Name name ) {
    this.name = name;
    this.parent = null;
    this.subject = new FsSubject <> ( effectiveName (), null, Scope.class );
  }

  /// Creates a new child scope with the given name and parent.
  FsScope ( Name name, FsScope parent ) {
    this.name = name;
    this.parent = parent;
    this.subject = new FsSubject <> ( effectiveName (), (FsSubject < ? >) parent.subject (), Scope.class );
  }

  boolean isClosed () {
    return closed.get ();
  }

  /// Called by FsClosure when consumed to remove from cache and resources list.
  void closureConsumed ( Resource resource ) {
    synchronized ( lock ) {
      if ( closureCache != null )
        closureCache.remove ( resource );
      if ( resources != null )
        resources.remove ( resource );
    }
  }

  @Override
  public Subject < Scope > subject () {
    return subject;
  }

  /// Returns the effective name for this scope.
  private Name effectiveName () {
    return name != null ? name : SCOPE_NAME;
  }

  @Override
  public String part () {
    return effectiveName ().part ();
  }

  @Override
  public Optional < Scope > enclosure () {
    return Optional.ofNullable ( parent );
  }

  /// Optimized within() — walks parent field directly instead of
  /// using default Extent.within() which allocates Optional per level.
  @Override
  public boolean within ( final Extent < ?, ? > enclosure ) {
    requireNonNull ( enclosure, "enclosure must not be null" );
    for ( FsScope current = parent; current != null; current = current.parent ) {
      if ( current == enclosure ) {
        return true;
      }
    }
    return false;
  }

  /// §15.1 provider mismatch: "Objects from incompatible provider
  /// implementations are mixed." The API states the consequence for both
  /// management operations in as many words — "@throws Fault if the resource
  /// parameter is not a runtime-provided implementation". Every resource this
  /// runtime provides is declared in this package; nothing else can be closed by
  /// a scope, because nothing else obeys the queued-close contract §9.1 binds it to.
  private void requireProvided ( Resource < ? > resource, String op ) {
    if ( resource.getClass ().getPackage () != FsScope.class.getPackage () ) {
      throw new Fault ( subject, op, "resource is not from this runtime provider" );
    }
  }

  /// §9.2 terminal check. `Fault`, not `IllegalStateException`: App. A.2 binds
  /// the Java projection of a synchronously detected substrate runtime error to
  /// `Fault`, and §15.3 requires the receiver's subject to be identifiable.
  private void requireOpen ( String op ) {
    if ( closed.get () ) {
      throw new Fault ( subject, op, "scope is closed" );
    }
  }

  @NotNull
  @Override
  @SuppressWarnings ( "unchecked" )
  public < R extends Resource < R > > Closure < R > closure ( @NotNull R resource ) {
    requireNonNull ( resource, "resource must not be null" );
    requireProvided ( resource, "closure" );
    requireOpen ( "closure" );

    synchronized ( lock ) {
      // Re-read the flag under the lock: a close that landed between the check
      // above and here has already drained the lists, and a closure registered
      // now would never be closed.
      requireOpen ( "closure" );

      if ( closureCache == null ) {
        closureCache = new IdentityHashMap <> ();
      }
      FsClosure < ? > cached = closureCache.get ( resource );
      if ( cached != null && !cached.isConsumed () ) {
        return (Closure < R >) cached;
      }
      FsClosure < R > closure = new FsClosure <> ( resource, this );
      closureCache.put ( resource, closure );
      if ( resources == null ) {
        resources = new ArrayList <> ();
      }
      // Register the resource so it gets closed when the scope closes (if not consumed)
      resources.add ( resource );
      return closure;
    }
  }

  @NotNull
  @Override
  public < R extends Resource < R > > R register ( @NotNull R resource ) {
    requireNonNull ( resource, "resource must not be null" );
    requireProvided ( resource, "register" );
    requireOpen ( "register" );

    synchronized ( lock ) {
      requireOpen ( "register" );

      if ( resources == null ) {
        resources = new ArrayList <> ();
      }
      // Idempotent: same instance (by identity) is a no-op, and keeps its
      // original close-order position (§9.2).
      for ( int i = 0, len = resources.size (); i < len; i++ ) {
        if ( resources.get ( i ) == resource ) return resource;
      }
      resources.add ( resource );
      return resource;
    }
  }

  @NotNull
  @Override
  public Scope scope () {
    return newChild ( SCOPE_NAME );
  }

  @NotNull
  @Override
  public Scope scope ( @NotNull Name childName ) {
    requireNonNull ( childName, "name must not be null" );
    return newChild ( childName );
  }

  private Scope newChild ( Name childName ) {
    requireOpen ( "scope" );
    synchronized ( lock ) {
      requireOpen ( "scope" );
      FsScope child = new FsScope ( childName, this );
      if ( children == null ) {
        children = new ArrayList <> ();
      }
      children.add ( child );
      return child;
    }
  }

  /// §9.2: "When a scope closes, all resources registered with it close
  /// automatically in reverse registration order (last registered, first
  /// closed)", then its child scopes. Terminal and idempotent (§16.1#8).
  ///
  /// Iterative, not recursive: a scope hierarchy is caller-shaped, and a
  /// recursive walk turns a deep one into a StackOverflowError during cleanup —
  /// the worst possible moment for it.
  @Idempotent
  @Override
  public void close () {
    // §9.2's terminal transition, once. First, so a re-entrant close from inside
    // a member's own close finds the scope already closed and returns.
    if ( closed.getAndSet ( true ) ) return;

    Deque < FsScope > pending = new ArrayDeque <> ();
    closeMembers ( this, pending );
    while ( !pending.isEmpty () ) {
      FsScope child = pending.pollLast ();
      // The child's own terminal transition, taken here rather than through a
      // recursive close: idempotence stays keyed on this one swap, so a child
      // already closed through its own handle is skipped exactly as before.
      if ( child.closed.getAndSet ( true ) ) continue;
      closeMembers ( child, pending );
    }
  }

  /// Closes one scope's registrations in reverse order and stages its children.
  ///
  /// The lists are taken under the lock and every close runs outside it
  /// (§15.4 #2). Each close is individually guarded — §9.2: "If a resource
  /// signals an error during close, the error MUST be suppressed and remaining
  /// resources MUST still close." One guard around the whole loop would satisfy
  /// the first half of that sentence and violate the second.
  private static void closeMembers ( FsScope scope, Deque < FsScope > pending ) {
    List < Resource > registered;
    List < FsScope >  kids;
    synchronized ( scope.lock ) {
      registered = scope.resources;
      kids       = scope.children;
      scope.resources    = null;
      scope.children     = null;
      scope.closureCache = null;
    }

    if ( registered != null ) {
      // §16.1#10: last registered, first closed.
      for ( int i = registered.size () - 1; i >= 0; i-- ) {
        try {
          registered.get ( i ).close ();
        } catch ( Throwable ignored ) {
          // §9.2 — suppressed; remaining resources still close.
        }
      }
    }

    // Staged in creation order, popped from the tail, so children close in
    // reverse creation order — the same rule the registrations follow.
    if ( kids != null ) pending.addAll ( kids );
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
