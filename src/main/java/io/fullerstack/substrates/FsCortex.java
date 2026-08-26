package io.fullerstack.substrates;

import static java.util.Objects.requireNonNull;

import io.humainary.substrates.api.Substrates.Circuit;
import io.humainary.substrates.api.Substrates.Cortex;
import io.humainary.substrates.api.Substrates.Current;
import io.humainary.substrates.api.Substrates.Fiber;
import io.humainary.substrates.api.Substrates.Flow;
import io.humainary.substrates.api.Substrates.Name;
import io.humainary.substrates.api.Substrates.New;
import io.humainary.substrates.api.Substrates.NotNull;
import io.humainary.substrates.api.Substrates.Pool;
import io.humainary.substrates.api.Substrates.Provided;
import io.humainary.substrates.api.Substrates.Scope;
import io.humainary.substrates.api.Substrates.Slot;
import io.humainary.substrates.api.Substrates.State;
import io.humainary.substrates.api.Substrates.Subject;

import java.lang.reflect.Member;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/// The entry point for creating substrates.
@Provided
final class FsCortex implements Cortex {

  /// Cached Name for anonymous scopes - avoid repeated HashMap lookup.
  static final Name SCOPE_NAME   = FsName.intern ( "scope" );
  /// Cached Name for anonymous circuits.
  static final Name CIRCUIT_NAME = FsName.intern ( "circuit" );

  private final Subject < Cortex > subject;

  /// §11.3's per-context Current — "each execution context has exactly one current, interned
  /// for that context's lifetime".
  ///
  /// A `ThreadLocal`, and the word that matters is **lifetime**. This was a
  /// `ConcurrentHashMap<Long, FsCurrent>` keyed by `Thread.threadId()`, justified by a comment
  /// claiming it beat a ThreadLocal by a few nanoseconds. It also never evicted: a thread id is
  /// not a liveness signal, so every thread that ever asked for a Current left an `FsCurrent`
  /// AND an interned `thread.<name>` node behind it, permanently. The code below deliberately
  /// names virtual threads `vt-<id>`, which is precisely the workload where thread ids are
  /// unbounded — one leaked entry per request, for the life of the process.
  ///
  /// A ThreadLocal's entry dies with its thread, which is the interning lifetime §11.3 asks
  /// for rather than an approximation of it.
  private final ThreadLocal < FsCurrent > current = new ThreadLocal <> ();

  FsCortex () {
    this.subject = new FsSubject <> ( FsName.intern ( "cortex" ), Cortex.class );
  }

  /// §5.7: publishes a circuit's own [Current] as the Current of its worker
  /// thread, for the whole life of that thread.
  ///
  /// "`Circuit.current()` MUST return a stable Current for the lifetime of the
  /// circuit. Comparing `Circuit.current()` with `Cortex.current()` lets callers
  /// detect whether they are currently executing on that circuit context without
  /// invoking an operation that is illegal from the circuit context, such as
  /// `await` or `pulse`." That comparison is only usable if the two are the
  /// *same* Current on the worker — minting a separate per-thread Current there
  /// makes the documented guard always report "not on the circuit", which is the
  /// exact failure the guard exists to prevent.
  /// Called by a circuit's worker on entry to its own loop. A ThreadLocal can only be set by
  /// the thread it belongs to, which is why this is no longer done by the constructing thread
  /// — and nothing can observe an unbound worker, because the worker's only body is that loop.
  void bindCurrent ( FsCurrent current ) {
    this.current.set ( current );
  }

  /// Releases the worker-thread binding once the worker has exited.
  void unbindCurrent () {
    current.remove ();
  }

  /// Gets or creates the Current for the current thread.
  ///
  /// The minting path lives in [#mintCurrent] so that this method stays small
  /// enough to inline at any call site. Inlined together they measured 97
  /// bytes, well past the 35-byte `MaxInlineSize` that applies wherever the
  /// call is not hot enough to earn the `FreqInlineSize` budget — and this is
  /// read on every emission, via `cortex().current()`.
  private FsCurrent getOrCreateCurrent () {
    final FsCurrent cached = current.get ();
    return cached != null ? cached : mintCurrent ();
  }

  /// Mints and caches this thread's Current. Runs once per thread.
  private FsCurrent mintCurrent () {
    final Thread t = Thread.currentThread ();
    String threadName = t.getName ();
    // Handle empty thread names (common with virtual threads)
    if ( threadName == null || threadName.isEmpty () ) {
      threadName = "vt-" + t.threadId ();
    }
    final FsSubject < Current > currentSubject = new FsSubject <> ( FsName.intern ( "thread." + threadName ),
      (FsSubject < ? >) subject, Current.class );
    final FsCurrent minted = new FsCurrent ( currentSubject );
    current.set ( minted );
    return minted;
  }

  @Override
  public Subject < Cortex > subject () {
    return subject;
  }

  @New
  @NotNull
  @Override
  public Circuit circuit () {
    return circuit ( CIRCUIT_NAME );
  }

  @New
  @NotNull
  @Override
  public Circuit circuit ( @NotNull Name name ) {
    requireNonNull ( name, "name must not be null" );
    FsSubject < Circuit > circuitSubject = new FsSubject <> ( name, (FsSubject < ? >) subject, Circuit.class );
    return new FsCircuit ( this, circuitSubject );
  }

  @Override
  public Current current () {
    return getOrCreateCurrent ();
  }

  // =========================================================================
  // Name factory methods - delegate to FsName static factories
  // =========================================================================

  @Override
  public Name name ( String path ) {
    return FsName.intern ( path );
  }

  @Override
  public Name name ( Enum < ? > path ) {
    return FsName.fromEnum ( path );
  }

  @Override
  public Name name ( Iterable < String > parts ) {
    return FsName.fromIterable ( parts );
  }

  @Override
  public < T > Name name ( Iterable < ? extends T > it, Function < ? super T, String > mapper ) {
    return FsName.fromIterable ( it, mapper );
  }

  @Override
  public Name name ( Iterator < String > it ) {
    return FsName.fromIterator ( it );
  }

  @Override
  public < T > Name name ( Iterator < ? extends T > it, Function < ? super T, String > mapper ) {
    return FsName.fromIterator ( it, mapper );
  }

  @Override
  public Name name ( Class < ? > type ) {
    return FsName.fromClass ( type );
  }

  @Override
  public Name name ( Member member ) {
    return FsName.fromMember ( member );
  }

  // =========================================================================
  // Pool factory method — root pool (3.0)
  // =========================================================================

  /// The identity source that roots a cortex pool: each name resolves to
  /// itself, so the derived pool's function receives the Name directly.
  /// FsDerivedPool supplies the once-per-name caching contract (success,
  /// null-rejection, and failure all memoised).
  private static final Pool < Name > NAME_IDENTITY = new Pool <> () {

    @NotNull
    @Override
    public Name get ( @NotNull Name name ) {
      return name;
    }

    @NotNull
    @Override
    public < U > Pool < U > pool ( @NotNull Function < ? super Name, ? extends U > fn ) {
      requireNonNull ( fn );
      return new FsDerivedPool <> ( this, fn );
    }

  };

  /// 3.0: root pool — materializes values on demand from names, invoking the
  /// factory function exactly once per name (cached result or failure replayed
  /// for subsequent lookups). Holds values for lookup but does not own them —
  /// no close, no lifecycle cascade (use Bank for owned Resources).
  @New
  @NotNull
  @Override
  public < T > Pool < T > pool ( @NotNull Function < ? super Name, ? extends T > fn ) {
    requireNonNull ( fn, "fn must not be null" );
    return new FsDerivedPool <> ( NAME_IDENTITY, fn );
  }

  // =========================================================================
  // Flow factory methods — standalone immutable flows (2.0)
  // =========================================================================

  @New
  @NotNull
  @Override
  public < E > Flow < E, E > flow () {
    return new FsFlow <> ();
  }

  @New
  @NotNull
  @Override
  public < E > Flow < E, E > flow ( @NotNull Class < E > type ) {
    requireNonNull ( type, "type must not be null" );
    return new FsFlow <> ();
  }

  /// 2.3: returns an identity flow with the fiber attached at the output side.
  /// Equivalent to `flow().fiber(fiber)`.
  @New
  @NotNull
  @Override
  public < E > Flow < E, E > flow ( @NotNull Fiber < E > fiber ) {
    requireNonNull ( fiber, "fiber must not be null" );
    Flow < E, E > f = new FsFlow <> ();
    return f.fiber ( fiber );
  }

  /// 2.3: returns an empty identity fiber.
  @New
  @NotNull
  @Override
  public < E > Fiber < E > fiber () {
    return new FsFiber <> ();
  }

  /// 2.3: returns an empty identity fiber. The class parameter is for type inference.
  @New
  @NotNull
  @Override
  public < E > Fiber < E > fiber ( @NotNull Class < E > type ) {
    requireNonNull ( type, "type must not be null" );
    return new FsFiber <> ();
  }

  @New
  @NotNull
  @Override
  public Scope scope ( @NotNull Name name ) {
    requireNonNull ( name, "name must not be null" );
    return new FsScope ( name );
  }

  @New
  @NotNull
  @Override
  public Scope scope () {
    return new FsScope ( SCOPE_NAME );
  }

  @New
  @NotNull
  @Override
  @SuppressWarnings ( "unchecked" )
  public Slot < Boolean > slot ( @NotNull Name name, boolean value ) {
    Objects.requireNonNull ( name, "name must not be null" );
    return new FsSlot <> ( name, value, (Class < Boolean >) (Class < ? >) boolean.class );
  }

  @New
  @NotNull
  @Override
  @SuppressWarnings ( "unchecked" )
  public Slot < Integer > slot ( @NotNull Name name, int value ) {
    Objects.requireNonNull ( name, "name must not be null" );
    return new FsSlot <> ( name, value, (Class < Integer >) (Class < ? >) int.class );
  }

  @New
  @NotNull
  @Override
  @SuppressWarnings ( "unchecked" )
  public Slot < Long > slot ( @NotNull Name name, long value ) {
    Objects.requireNonNull ( name, "name must not be null" );
    return new FsSlot <> ( name, value, (Class < Long >) (Class < ? >) long.class );
  }

  @New
  @NotNull
  @Override
  @SuppressWarnings ( "unchecked" )
  public Slot < Double > slot ( @NotNull Name name, double value ) {
    Objects.requireNonNull ( name, "name must not be null" );
    return new FsSlot <> ( name, value, (Class < Double >) (Class < ? >) double.class );
  }

  @New
  @NotNull
  @Override
  @SuppressWarnings ( "unchecked" )
  public Slot < Float > slot ( @NotNull Name name, float value ) {
    Objects.requireNonNull ( name, "name must not be null" );
    return new FsSlot <> ( name, value, (Class < Float >) (Class < ? >) float.class );
  }

  @New
  @NotNull
  @Override
  public Slot < String > slot ( @NotNull Name name, @NotNull String value ) {
    Objects.requireNonNull ( name, "name must not be null" );
    Objects.requireNonNull ( value, "value must not be null" );
    return new FsSlot <> ( name, value, String.class );
  }

  @New
  @NotNull
  @Override
  public Slot < Name > slot ( @NotNull Enum < ? > value ) {
    Objects.requireNonNull ( value, "value must not be null" );
    Name slotName = name ( value.getDeclaringClass () );
    // Value is the full enum name: DeclaringClass.name
    Name slotValue = name ( value );
    return new FsSlot <> ( slotName, slotValue, Name.class );
  }

  @New
  @NotNull
  @Override
  public Slot < Name > slot ( @NotNull Name name, @NotNull Name value ) {
    Objects.requireNonNull ( name, "name must not be null" );
    Objects.requireNonNull ( value, "value must not be null" );
    return new FsSlot <> ( name, value, Name.class );
  }

  @New
  @NotNull
  @Override
  public Slot < State > slot ( @NotNull Name name, @NotNull State value ) {
    Objects.requireNonNull ( name, "name must not be null" );
    Objects.requireNonNull ( value, "value must not be null" );
    return new FsSlot <> ( name, value, State.class );
  }

  @New
  @NotNull
  @Override
  public State state () {
    return FsState.create ();
  }

}
