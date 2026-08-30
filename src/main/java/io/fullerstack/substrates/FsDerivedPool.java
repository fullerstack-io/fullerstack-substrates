package io.fullerstack.substrates;

import io.humainary.substrates.api.Substrates.Name;
import io.humainary.substrates.api.Substrates.NotNull;
import io.humainary.substrates.api.Substrates.Pool;
import io.humainary.substrates.api.Substrates.Provided;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/// A derived pool that applies a transformation function to each result from
/// an underlying pool, per Substrates 2.4 §10.1:
///
/// - The function is invoked **at most once per name**; the cached outcome
///   (success, null-rejection, or failure) is replayed for subsequent lookups.
/// - If the function returns null, `get(name)` raises [NullPointerException]
///   for that name; the rejection is cached.
/// - If the function throws, the **same exception** is re-thrown on every
///   subsequent `get(name)` for that name without re-invoking the function.
///
/// `Cortex.pool(fn)` is this type too, over an identity source: a root pool is a derivation that
/// transforms a name into a value, so there is no separate root implementation.
///
/// This is the **only** pool implementation. `Conduit` and `Sink` do not write their own storage:
/// they obtain one from `Cortex.pool(Function)`, the prescribed factory, and so land here too.
///
/// ## Three states
///
/// Most pools only ever see one name — a derived pool is typically created at the moment of a
/// lookup and discarded after it — so no map is allocated until a second distinct name arrives:
///
/// - **EMPTY** — nothing resolved; neither the entry nor the map exists.
/// - **SINGLE** — one entry, held inline as `(last, cached)`. No map.
/// - **MULTI** — a second distinct name promotes to an [IdentityHashMap], and the inline pair
///   stays live as the one-entry cache in front of it.
///
/// ## Concurrency
///
/// A reader takes no lock. `get` used to be `synchronized` outright, which put a monitor in front
/// of every channel resolution once `Conduit` and `Sink` started sharing this.
///
/// - The inline entry publishes **cached-then-last**, with `last` volatile. That release/acquire
///   pair is what makes the lock-free fast path sound: a reader seeing a `last` sees the `cached`
///   written before it, and can never pair one entry's name with another's value.
/// - [#overflow] is volatile and replaced, never mutated, so a reader holding the old map stays
///   consistent.
/// - Resolution is serialised on this pool's monitor and re-checks after acquiring it, so the
///   function runs **at most once per name** however many callers race — which is what §10.1's
///   exactly-once rests on.
/// - The function may re-enter [#get] for other names: a hierarchical conduit channel
///   materialises its ancestors that way. The monitor is reentrant and every publish re-reads the
///   field, so entries added by the recursion survive the outer publish.
@Provided
final class FsDerivedPool < T, S > implements Pool < T > {

  /// Sentinel cached for entries whose function returned null. Each `get(name)`
  /// for that name then raises NPE without re-invoking the function.
  private static final Object NULL_RESULT = new Object ();

  /// Wrapper for a cached function failure. Holds the exact exception that
  /// the function threw; `get(name)` rethrows the same instance per spec.
  private record CachedFailure( RuntimeException ex ) { }

  private final Pool < S >                          source;
  private final Function < ? super S, ? extends T > fn;

  /// The inline entry — the whole of state SINGLE, and the one-entry cache in state MULTI.
  /// Written cached-first; `last` is volatile so the pair publishes as one. See above.
  private Object        cached;
  private volatile Name last;

  /// Resolved outcomes by name — a value, [#NULL_RESULT], or a [CachedFailure]. Allocated on the
  /// second distinct name and replaced on every insert.
  private volatile Map < Name, Object > overflow;

  FsDerivedPool ( Pool < S > source, Function < ? super S, ? extends T > fn ) {
    this.source = source;
    this.fn     = fn;
  }

  @NotNull
  @Override
  public T get ( @NotNull Name name ) {
    // §15.2 absence violation: a required argument is never optional. Checked before the store is
    // consulted, whose inline entry is null on a fresh pool and would read a null name as a hit.
    requireNonNull ( name, "name must not be null" );

    if ( name == last ) return unwrap ( cached, name );

    final Map < Name, Object > map = overflow;
    if ( map != null ) {
      final Object hit = map.get ( name );
      if ( hit != null ) return unwrap ( remember ( name, hit ), name );
    }

    return unwrap ( resolve ( name ), name );
  }

  /// Resolves one name, at most once. Serialised so racing callers cannot both invoke the
  /// function, which is §10.1's exactly-once requirement.
  private synchronized Object resolve ( Name name ) {

    if ( name == last ) return cached;

    final Map < Name, Object > seen = overflow;
    if ( seen != null ) {
      final Object hit = seen.get ( name );
      if ( hit != null ) return remember ( name, hit );
    }

    final Object outcome = compute ( name );

    // Re-read rather than reusing `seen`: the function may have resolved names of its own.
    final Map < Name, Object > current = overflow;

    if ( current == null && last == null ) {
      // EMPTY → SINGLE. No map: the inline entry is the whole pool.
      return remember ( name, outcome );
    }

    final Map < Name, Object > next =
      current == null ? new IdentityHashMap <> ( 4 ) : new IdentityHashMap <> ( current );
    if ( current == null ) next.put ( last, cached );
    next.put ( name, outcome );
    overflow = next;

    return remember ( name, outcome );
  }

  /// Publishes the inline entry — value first, then the volatile name that releases it.
  private Object remember ( Name name, Object outcome ) {
    cached = outcome;
    last   = name;
    return outcome;
  }

  /// Wraps the user function so a null result and a thrown exception can both be cached — the
  /// spec requires each to be memoised rather than re-invoked. Runs under the store's monitor,
  /// once per name.
  private Object compute ( Name name ) {
    try {
      final T result = fn.apply ( source.get ( name ) );
      return result != null ? result : NULL_RESULT;
    } catch ( RuntimeException ex ) {
      return new CachedFailure ( ex );
    }
  }

  @SuppressWarnings ( "unchecked" )
  private T unwrap ( Object cached, Name name ) {
    if ( cached == NULL_RESULT ) {
      throw new NullPointerException ( "Pool function returned null for name: " + name );
    }
    if ( cached instanceof CachedFailure cf ) {
      throw cf.ex;
    }
    return (T) cached;
  }

  @NotNull
  @Override
  public < U > Pool < U > pool ( @NotNull Function < ? super T, ? extends U > nextFn ) {
    return new FsDerivedPool <> ( this, nextFn );
  }

}
