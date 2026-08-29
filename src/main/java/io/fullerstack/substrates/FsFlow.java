package io.fullerstack.substrates;

import io.fullerstack.substrates.FsOperators.Wrap;
import io.humainary.substrates.api.Substrates.Cell;
import io.humainary.substrates.api.Substrates.Change;
import io.humainary.substrates.api.Substrates.Fiber;
import io.humainary.substrates.api.Substrates.Flow;
import io.humainary.substrates.api.Substrates.New;
import io.humainary.substrates.api.Substrates.NotNull;
import io.humainary.substrates.api.Substrates.Pipe;
import io.humainary.substrates.api.Substrates.Provided;
import io.humainary.substrates.api.Substrates.Run;
import io.humainary.substrates.api.Substrates.Subject;
import io.humainary.substrates.api.Substrates.Window;

import java.time.Duration;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/// Immutable Flow<I,O> implementation for Substrates 2.3.
///
/// Flow carries type-changing composition: `map`, `flow`, `fiber`, `pipe`.
/// Per-emission operators (diff, guard, limit, peek, etc.) live on Fiber<E>;
/// their implementations are in {@link FsOperators}.
///
/// Operators are stored as a uniform {@code Wrap[]} array — `map` becomes
/// a {@link MapWrap}, `fiber(...)` and `flow(...)` inline the source's Wraps
/// directly into the destination's array. Materialise walks the array with
/// no `instanceof` dispatch.
///
/// @param <I> input type (what {@code Pipe.pipe(Flow)} receives)
/// @param <O> output type (what the target pipe emits)
@Provided
public final class FsFlow < I, O > implements Flow < I, O > {

  // ═══════════════════════════════════════════════════════════════════════════
  // Type-changing operator — Flow's own contribution
  // ═══════════════════════════════════════════════════════════════════════════

  /// Map operator — the only Flow-specific operator. Drops null results so
  /// `map` can act as a filter when the function returns null. Type-erased
  /// because operators travel through the {@code Wrap[]} alongside
  /// type-preserving ones.
  @SuppressWarnings ( { "unchecked", "rawtypes" } )
  static final class MapWrap implements Wrap < Object > {
    final Function < Object, Object > fn;

    MapWrap ( Function fn ) { this.fn = (Function < Object, Object >) fn; }

    @Override
    public Consumer < Object > wrap ( Consumer < Object > downstream ) {
      final Function < Object, Object > f = fn;
      return v -> {
        Object r = f.apply ( v );
        if ( r != null ) downstream.accept ( r );
      };
    }
  }

  /// **Scan (state-only projection)** — folds inputs into a per-materialization
  /// state slot via `step(state, input) -> state`, then emits a projection
  /// `emit(state) -> P` downstream. Spec §6.2.3:
  ///
  /// - Supplier invoked ONCE per materialization at attachment time (in `wrap`).
  /// - `step` throws → state NOT replaced; side effects in step are retained
  ///   (no rollback); next emission proceeds from prior state.
  /// - `emit` throws → state already advanced; next emission proceeds from
  ///   advanced state.
  /// - `emit` returns null → emission dropped downstream.
  /// - All throws are swallowed at this stage (external callback isolation
  ///   per §15.4); they do not propagate up the chain.
  @SuppressWarnings ( { "unchecked", "rawtypes" } )
  static final class ScanStateWrap implements Wrap < Object > {
    final Supplier < ? > initial;
    final BiFunction < Object, Object, Object > step;
    final Function < Object, Object > emit;

    ScanStateWrap ( Supplier initial, BiFunction step, Function emit ) {
      this.initial = initial;
      this.step    = (BiFunction < Object, Object, Object >) step;
      this.emit    = (Function < Object, Object >) emit;
    }

    @Override
    public Consumer < Object > wrap ( Consumer < Object > downstream ) {
      return new ScanState ( initial.get (), step, emit, downstream );
    }
  }

  /// The materialised scan stage.
  static final class ScanState implements Consumer < Object > {
    private final BiFunction < Object, Object, Object > step;
    private final Function < Object, Object >           emit;
    private final Consumer < Object >                   d;
    private       Object                                state;

    ScanState ( Object initial,
                BiFunction < Object, Object, Object > step,
                Function < Object, Object > emit,
                Consumer < Object > d ) {
      this.state = initial;
      this.step  = step;
      this.emit  = emit;
      this.d     = d;
    }

    @Override
    public void accept ( Object v ) {
      final Object next;
      try {
        next = step.apply ( state, v );
      } catch ( RuntimeException raised ) {
        return;                     // state unchanged, drop emission
      }
      state = next;
      final Object projection;
      try {
        projection = emit.apply ( next );
      } catch ( RuntimeException raised ) {
        return;                     // state already advanced; drop this emission
      }
      if ( projection != null ) d.accept ( projection );
    }
  }

  /// **Scan (input-aware projection)** — same as ScanStateWrap but emit
  /// signature is `(state, input) -> P` so the projection can blend
  /// running state with the current input (z-scores, residuals, anomaly
  /// scoring).
  @SuppressWarnings ( { "unchecked", "rawtypes" } )
  static final class ScanInputAwareWrap implements Wrap < Object > {
    final Supplier < ? > initial;
    final BiFunction < Object, Object, Object > step;
    final BiFunction < Object, Object, Object > emit;

    ScanInputAwareWrap ( Supplier initial, BiFunction step, BiFunction emit ) {
      this.initial = initial;
      this.step    = (BiFunction < Object, Object, Object >) step;
      this.emit    = (BiFunction < Object, Object, Object >) emit;
    }

    @Override
    public Consumer < Object > wrap ( Consumer < Object > downstream ) {
      return new ScanInputAware ( initial.get (), step, emit, downstream );
    }
  }

  /// The materialised input-aware scan stage.
  static final class ScanInputAware implements Consumer < Object > {
    private final BiFunction < Object, Object, Object > step;
    private final BiFunction < Object, Object, Object > emit;
    private final Consumer < Object >                   d;
    private       Object                                state;

    ScanInputAware ( Object initial,
                     BiFunction < Object, Object, Object > step,
                     BiFunction < Object, Object, Object > emit,
                     Consumer < Object > d ) {
      this.state = initial;
      this.step  = step;
      this.emit  = emit;
      this.d     = d;
    }

    @Override
    public void accept ( Object v ) {
      final Object next;
      try {
        next = step.apply ( state, v );
      } catch ( RuntimeException raised ) {
        return;
      }
      state = next;
      final Object projection;
      try {
        projection = emit.apply ( next, v );
      } catch ( RuntimeException raised ) {
        return;
      }
      if ( projection != null ) d.accept ( projection );
    }
  }

  /// **Window (count-based)** — appends each surviving input to a per-
  /// materialization rolling buffer of fixed `count`, and emits an
  /// `FsWindow` view over the current buffer on every accepted input.
  ///
  /// Buffer is non-circular: when full, shift left to drop oldest. Each
  /// emit reuses the same buffer (the Window view shares it). Per spec,
  /// the view is callback-scoped — downstream MUST consume within the
  /// receiving callback before the next emission mutates the buffer.
  static final class WindowCountWrap implements Wrap < Object > {
    final int count;

    WindowCountWrap ( int count ) {
      if ( count <= 0 ) throw new IllegalArgumentException ( "count must be > 0" );
      this.count = count;
    }

    @Override
    public Consumer < Object > wrap ( Consumer < Object > downstream ) {
      return new CountWindow ( count, downstream );
    }
  }

  /// The materialised count-window stage.
  ///
  /// Retains the last `capacity` values on a [DelayLine] and emits a view over it — no copy,
  /// and O(1) per admission where the shift this replaced was O(capacity).
  static final class CountWindow implements Consumer < Object > {
    private final DelayLine           line;
    private final Consumer < Object > d;
    /// §6.4.1 temporal lease, per materialisation — see [WindowLease]. Built from the line
    /// rather than beside it: the lease carries the ring every view reads, so it cannot be a
    /// field initialiser, which would run before the line exists.
    private final WindowLease         lease;

    CountWindow ( int capacity, Consumer < Object > d ) {
      this.line  = DelayLine.of ( capacity );
      this.lease = new WindowLease ( line.buffer () );
      this.d     = d;
    }

    @Override
    public void accept ( Object v ) {
      line.append ( v );
      d.accept (
        new FsWindow <> ( line.start (), line.size (), false, lease, lease.latch () )
      );
    }
  }

  /// **Window (duration + capacity)** — same as count-based but also
  /// evicts entries whose capture timestamp is older than `duration`
  /// relative to the current processing time. Two parallel arrays:
  /// values + capture timestamps in nanoseconds.
  static final class WindowDurationWrap implements Wrap < Object > {
    final long durationNanos;
    final int  capacity;

    WindowDurationWrap ( Duration duration, int capacity ) {
      if ( duration == null || duration.isZero () || duration.isNegative () ) {
        throw new IllegalArgumentException ( "duration must be > 0" );
      }
      if ( capacity <= 0 ) throw new IllegalArgumentException ( "capacity must be > 0" );
      // API contract on Flow#window(Duration,int): `@throws
      // IllegalArgumentException if duration is zero, negative, or cannot be
      // represented in nanoseconds`. Duration.toNanos raises ArithmeticException
      // on overflow, which is neither of §15.1's declared error shapes.
      try {
        this.durationNanos = duration.toNanos ();
      } catch ( ArithmeticException overflow ) {
        throw new IllegalArgumentException ( "duration cannot be represented in nanoseconds", overflow );
      }
      this.capacity      = capacity;
    }

    @Override
    public Consumer < Object > wrap ( Consumer < Object > downstream ) {
      return new DurationWindow ( durationNanos, capacity, downstream );
    }
  }

  /// The materialised duration-window stage.
  ///
  /// Retains the last `capacity` values on a [DelayLine] and drops any older than
  /// `durationNanos` relative to the §5.8 processing time of the admission being handled.
  ///
  /// The line owns the capture stamps, so ageing entries out is a scan and a `dropOldest`, and
  /// this stage never sees a physical index.
  static final class DurationWindow implements Consumer < Object > {
    private final long                durationNanos;
    private final DelayLine           line;
    private final Consumer < Object > d;
    /// §6.4.1 temporal lease, per materialisation — see [WindowLease]. Built from the line
    /// rather than beside it, for the reason given on [CountWindow#lease].
    private final WindowLease         lease;

    DurationWindow ( long durationNanos, int capacity, Consumer < Object > d ) {
      this.durationNanos = durationNanos;
      this.line          = DelayLine.timed ( capacity );
      this.lease         = new WindowLease ( line.buffer () );
      this.d             = d;
    }

    @Override
    public void accept ( Object v ) {

      final long now = FsOperators.stimulus ();   // §5.8: one reading per ingress chain

      // Values are held in capture order, so the first young enough ends the scan.
      final int held    = line.size ();
      int       expired = 0;
      while ( expired < held && now - line.timeAt ( expired ) > durationNanos ) expired++;
      if ( expired > 0 ) line.dropOldest ( expired );

      line.append ( v, now );

      d.accept (
        new FsWindow <> ( line.start (), line.size (), false, lease, lease.latch () )
      );
    }
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Immutable state
  // ═══════════════════════════════════════════════════════════════════════════

  /// The wiring plan. Immutable and structurally shared — see [Recipe].
  ///
  /// This replaces four fields: an operator array, its length, and two arrays of positioned
  /// per-attachment factories. A factory is now just a [Recipe.Deferred] node sitting where it
  /// was composed, so there is no position to record and nothing to splice.
  private final Recipe recipe;

  /// Identity flow — no operators.
  public FsFlow () {
    this.recipe = Recipe.EMPTY;
  }

  private FsFlow ( Recipe recipe ) {
    this.recipe = recipe;
  }

  /// Returns a new FsFlow with the given operator composed after this one's. O(1).
  @SuppressWarnings ( "unchecked" )
  private < X, Y > FsFlow < X, Y > append ( Wrap < ? > op ) {
    return (FsFlow < X, Y >) (FsFlow < ?, ? >) new FsFlow <> ( recipe.then ( op ) );
  }

  /// Internal accessor used when one flow is composed into another.
  Recipe recipe () { return recipe; }

  // ═══════════════════════════════════════════════════════════════════════════
  // Materialisation
  // ═══════════════════════════════════════════════════════════════════════════

  /// Builds this flow's consumer chain for one attachment, terminating at `downstream`.
  ///
  /// The first-composed operator ends up outermost and sees each value first, so runtime data
  /// flow matches the user's reading order per SPEC §6.2.5. Per-attachment factories (§6.2 /
  /// 2.4 / 2.6) resolve here, once, against `subject` — they are nodes in the plan, so they
  /// need no separate resolution pass.
  @SuppressWarnings ( "unchecked" )
  private Consumer < I > wire ( Subject < ? > subject, Consumer < Object > downstream ) {
    return (Consumer < I >) (Consumer < ? >) recipe.wire ( subject, downstream );
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Flow<I,O> API (2.3) — only map/flow/fiber/pipe
  // ═══════════════════════════════════════════════════════════════════════════

  @NotNull
  @Override
  public < P > Flow < I, P > map ( @NotNull Function < ? super O, ? extends P > fn ) {
    Objects.requireNonNull ( fn );
    return append ( new MapWrap ( fn ) );
  }

  @NotNull
  @Override
  @SuppressWarnings ( "unchecked" )
  public Flow < I, O > fiber ( @NotNull Fiber < O > fiber ) {
    Objects.requireNonNull ( fiber );
    if ( !( fiber instanceof FsFiber < ? > fsFiber ) ) {
      // §15.1 provider mismatch, MUST detect; Appendix A.2 binds it to Fault.
      throw FsOperators.fault ( "fiber", "fiber is not from this runtime provider" );
    }
    // Fiber operators are type-preserving (E→E with E ≡ O at this position), so the fiber's
    // plan composes directly after this one's.
    return new FsFlow <> ( recipe.then ( fsFiber.recipe () ) );
  }

  /// 2.4: Per-attachment fiber factory. The factory is invoked once per
  /// `pipe(target)` call with `target.subject()`, and its returned fiber's
  /// operators are inlined at the position the factory was added.
  @NotNull
  @Override
  public Flow < I, O > fiber ( @NotNull Function < ? super Subject < ? >, ? extends Fiber < O > > factory ) {
    Objects.requireNonNull ( factory );
    // The provider check belongs at resolution, not composition: a factory result is a
    // composition argument like any other, and it does not exist until the attachment.
    return new FsFlow <> ( recipe.thenResolving ( subject -> {
      final Fiber < ? > produced = factory.apply ( subject );
      Objects.requireNonNull ( produced, "fiber factory must not return null" );
      if ( !( produced instanceof FsFiber < ? > fsFiber ) ) {
        throw FsOperators.fault ( "fiber", "fiber factory produced a value from another provider" );
      }
      return fsFiber.recipe ();
    } ) );
  }

  @NotNull
  @Override
  @SuppressWarnings ( "unchecked" )
  public < P > Flow < I, P > flow ( @NotNull Flow < ? super O, ? extends P > next ) {
    Objects.requireNonNull ( next );
    if ( !( next instanceof FsFlow < ?, ? > nextFlow ) ) {
      // §15.1 provider mismatch, MUST detect; Appendix A.2 binds it to Fault.
      throw FsOperators.fault ( "flow", "next flow is not from this runtime provider" );
    }
    if ( nextFlow.recipe.isEmpty () ) {
      return (Flow < I, P >) (Flow < ?, ? >) this;
    }
    // next runs after `this`, so its plan composes after — and its own per-attachment
    // factories travel with it, needing no position translation.
    return (Flow < I, P >) (Flow < ?, ? >) new FsFlow <> ( recipe.then ( nextFlow.recipe ) );
  }

  /// 2.6: Per-attachment Flow factory. Same as fiber-factory but produces
  /// a Flow (which is type-changing). Recursively resolves the inner Flow's
  /// own factories at materialisation.
  @NotNull
  @Override
  @SuppressWarnings ( "unchecked" )
  public < P > Flow < I, P > flow (
      @NotNull Function < ? super Subject < ? >, ? extends Flow < ? super O, ? extends P > > factory ) {
    Objects.requireNonNull ( factory, "factory" );
    return (Flow < I, P >) (Flow) new FsFlow <> ( recipe.thenResolving ( subject -> {
      final Flow < ?, ? > produced = factory.apply ( subject );
      Objects.requireNonNull ( produced, "flow factory must not return null" );
      if ( !( produced instanceof FsFlow < ?, ? > fsFlow ) ) {
        throw FsOperators.fault ( "flow", "flow factory produced a value from another provider" );
      }
      return fsFlow.recipe ();
    } ) );
  }

  /// When target is a same-circuit FsPipe, the flow's terminal submits to
  /// transit directly — bypassing target.emit's checks. If target's receiver
  /// is an FsChannel, submit channel.dispatch instead of channel itself,
  /// skipping the channel's version check on the cascade hot path (spec
  /// §5.4.1 + §7.6.2 — subscriber state cannot change mid-cascade).
  @New
  @NotNull
  @Override
  @SuppressWarnings ( "unchecked" )
  public Pipe < I > pipe ( @NotNull Pipe < ? super O > target ) {
    Objects.requireNonNull ( target );
    // §15.1 provider mismatch, MUST detect — ahead of the identity-flow elision
    // below, so an identity flow cannot smuggle a foreign target through, and
    // in place of the former inline-emit branch, which ran the whole operator
    // chain on the caller's thread (§16.1#1).
    if ( FsOperators.foreign ( target ) ) {
      throw FsOperators.fault ( "pipe", "target pipe is not from this runtime provider" );
    }
    // Empty flow elision — nothing composed means I == O. A pending factory is never empty:
    // §6.2 promises it runs once per attachment, so it has to be wired to find out.
    if ( recipe.isEmpty () ) return (Pipe < I >) (Pipe < ? >) target;

    final Subject < ? > subject = target.subject ();
    final Consumer < I > chain;
    if ( target instanceof FsPipe < ? > fp ) {
      final FsCircuit c = fp.circuit ();
      final Consumer < Object > targetReceiver = fp.receiver ();
      if ( targetReceiver instanceof FsChannel < ? > channel ) {
        // Submit channel.cascadeDispatch directly — receptors + STEM, no
        // version check. Falls back to channel before first rebuild.
        chain = wire ( subject, v -> {
          Consumer < Object > d = channel.cascadeDispatch;
          c.submit ( d != null ? d : channel, v );
        } );
      } else {
        chain = wire ( subject, v -> c.submit ( targetReceiver, v ) );
      }
      // §4.3: a materialized pipe's enclosure is the pipe it feeds, one level
      // deeper, so chained attachments form a fully-qualified nested path.
      return new FsPipe <> ( (Consumer < Object >) (Consumer < ? >) chain, c,
        (FsSubject < ? >) subject );
    }
    // This provider's own non-FsPipe carriers (an FsSink channel pipe, say)
    // expose no receiver to submit to, so the chain is driven through emit().
    final Pipe < Object > carrier = (Pipe < Object >) (Pipe < ? >) target;
    chain = wire ( subject, carrier::emit );
    final Subject < Pipe < I > > nested = (Subject < Pipe < I > >) (Subject < ? >)
      new FsSubject <> ( null, (FsSubject < ? >) target.subject (), Pipe.class );
    return new Pipe <> () {
      @Override
      public void emit ( @NotNull I emission ) {
        chain.accept ( emission );
      }
      @Override
      public Subject < Pipe < I > > subject () {
        return nested;
      }
    };
  }

  /// 2.7: attach this flow's pipeline before a Cell's update pipe.
  @NotNull
  @Override
  public Pipe < I > pipe ( @NotNull Cell < ? super O > cell ) {
    Objects.requireNonNull ( cell, "cell must not be null" );
    // §15.1 provider mismatch is checked on the Cell itself — see the twin
    // comment on FsFiber#pipe(Cell).
    if ( FsOperators.foreign ( cell ) ) {
      throw FsOperators.fault ( "pipe", "cell is not from this runtime provider" );
    }
    return pipe ( cell.pipe () );
  }


  /// 3.0: emits a [Run] per admission carrying the value and its consecutive-run
  /// length. Length resets to 1 on a change (`Objects.equals`-difference), increments
  /// on a repeat. First admission emits with length=1. Spec §4632-4655.
  @NotNull
  @Override
  public Flow < I, Run < O > > run () {
    return appendOp ( new RunWrap () );
  }

  /// 3.0: emits a [Change] only at a run boundary — when an admission is
  /// value-unequal to its predecessor. Carries the closed run's terminal value
  /// and length, plus the value that opened the next run. First admission opens
  /// the first run and emits nothing; the open or final run is never reported.
  /// Spec §4223-4248.
  @NotNull
  @Override
  public Flow < I, Change < O > > change () {
    return appendOp ( new ChangeWrap () );
  }

  /// Immutable Run carrier — per-admission envelope produced by [#run()].
  /// `@Tenure(EPHEMERAL) @ReadOnly @Provided` per spec §6700.
  private record RunImpl < E > ( E emission, long length ) implements Run < E > {}

  /// Immutable Change carrier — run-boundary envelope produced by [#change()].
  /// `@Tenure(EPHEMERAL) @ReadOnly @Provided` per spec §550.
  private record ChangeImpl < E > ( E from, E to, long length ) implements Change < E > {}

  /// 3.0 run — stateful per-materialization. Each admission emits a fresh
  /// [Run] with the value and its consecutive-run length.
  ///
  /// State: `prev` (last value seen) + `length` (running count). On admission:
  /// if `Objects.equals(value, prev)` then `length++`; else reset
  /// `prev = value; length = 1`. Always emit `(value, length)`.
  @SuppressWarnings ( { "unchecked", "rawtypes" } )
  static final class RunWrap implements Wrap < Object > {
    @Override
    public Consumer < Object > wrap ( Consumer < Object > downstream ) {
      return new RunLength ( downstream );
    }
  }

  /// The materialised run stage: emits every admission with the length of the run it belongs to.
  static final class RunLength implements Consumer < Object > {
    private final Consumer < Object > d;
    private       Object              prev;
    private       long                length;
    private       boolean             seeded;

    RunLength ( Consumer < Object > d ) {
      this.d = d;
    }

    @Override
    public void accept ( Object v ) {
      if ( !seeded || !Objects.equals ( v, prev ) ) {
        prev   = v;
        length = 1L;
        seeded = true;
      } else {
        length++;
      }
      d.accept ( new RunImpl ( v, length ) );
    }
  }

  /// 3.0 change — stateful per-materialization. Emits a [Change] only at a run
  /// boundary; the first admission opens the first run silently; the trailing
  /// open run is never reported.
  ///
  /// State: `prev` + `length` + `hasPrev`. First admission: seed `prev`,
  /// `length = 1`, emit nothing. Same as `prev`: `length++`, emit nothing.
  /// Different from `prev`: emit `Change(prev, value, length)`, then reset
  /// `prev = value; length = 1`.
  @SuppressWarnings ( { "unchecked", "rawtypes" } )
  static final class ChangeWrap implements Wrap < Object > {
    @Override
    public Consumer < Object > wrap ( Consumer < Object > downstream ) {
      return new RunBoundary ( downstream );
    }
  }

  /// The materialised change stage: emits only where one run ends and the next begins.
  static final class RunBoundary implements Consumer < Object > {
    private final Consumer < Object > d;
    private       Object              prev;
    private       long                length;
    private       boolean             hasPrev;

    RunBoundary ( Consumer < Object > d ) {
      this.d = d;
    }

    @Override
    public void accept ( Object v ) {
      if ( !hasPrev ) {
        prev    = v;
        length  = 1L;
        hasPrev = true;
        return;                     // the first admission opens the first run silently
      }
      if ( Objects.equals ( v, prev ) ) {
        length++;
        return;                     // still inside the same run
      }
      d.accept ( new ChangeImpl ( prev, v, length ) );
      prev   = v;
      length = 1L;
    }
  }

  /// 2.10: type-changing sibling of `Fiber.relate(E, BinaryOperator<E>)`.
  /// Emits projection of each `(prev, curr)` relation, advancing `prev` on
  /// every emission (including those `op` filtered via null). Spec §4549-4554.
  @NotNull
  @Override
  public < P > Flow < I, P > relate (
      O initial,
      @NotNull BiFunction < ? super O, ? super O, ? extends P > op ) {
    Objects.requireNonNull ( op, "op" );
    return appendOp ( new RelateWrap ( initial, op ) );
  }

  /// 2.10 relate — tracks `prev` (seeded from `initial`), advances on every
  /// emission, projects via `op(prev, curr) -> P`. Returning null filters
  /// the projection but does NOT prevent `prev` advancing.
  @SuppressWarnings ( { "unchecked", "rawtypes" } )
  static final class RelateWrap implements Wrap < Object > {
    final Object                                initial;
    final BiFunction < Object, Object, Object > op;

    RelateWrap ( Object initial, BiFunction op ) {
      this.initial = initial;
      this.op      = (BiFunction < Object, Object, Object >) op;
    }

    @Override
    public Consumer < Object > wrap ( Consumer < Object > downstream ) {
      return new Relation ( initial, op, downstream );
    }
  }

  /// The materialised relate stage: projects each `(prev, curr)` pair, advancing `prev` on every
  /// admission — including one whose projection is filtered or raises.
  static final class Relation implements Consumer < Object > {
    private final BiFunction < Object, Object, Object > op;
    private final Consumer < Object >                   d;
    private       Object                                prev;

    Relation ( Object initial, BiFunction < Object, Object, Object > op, Consumer < Object > d ) {
      this.prev = initial;
      this.op   = op;
      this.d    = d;
    }

    @Override
    public void accept ( Object v ) {
      final Object p = prev;
      prev = v;                     // advance regardless of projection or throw
      final Object projection;
      try {
        projection = op.apply ( p, v );
      } catch ( RuntimeException raised ) {
        return;
      }
      if ( projection != null ) d.accept ( projection );
    }
  }

  /// 2.6: heterogeneous fold (scan) with state-only projection.
  @NotNull
  @Override
  public < S, P > Flow < I, P > scan (
      @NotNull Supplier < ? extends S > initial,
      @NotNull BiFunction < ? super S, ? super O, ? extends S > step,
      @NotNull Function < ? super S, ? extends P > emit ) {
    Objects.requireNonNull ( initial, "initial" );
    Objects.requireNonNull ( step,    "step" );
    Objects.requireNonNull ( emit,    "emit" );
    return appendOp ( new ScanStateWrap ( initial, step, emit ) );
  }

  /// 2.6: scan with input-aware projection.
  @NotNull
  @Override
  public < S, P > Flow < I, P > scan (
      @NotNull Supplier < ? extends S > initial,
      @NotNull BiFunction < ? super S, ? super O, ? extends S > step,
      @NotNull BiFunction < ? super S, ? super O, ? extends P > emit ) {
    Objects.requireNonNull ( initial, "initial" );
    Objects.requireNonNull ( step,    "step" );
    Objects.requireNonNull ( emit,    "emit" );
    return appendOp ( new ScanInputAwareWrap ( initial, step, emit ) );
  }

  /// 2.6: count-based windowing. Returns a flow that emits an
  /// FsWindow view of the most-recent `size` upstream values on every
  /// accepted input. The view is callback-scoped per spec §6.2.3.
  @NotNull
  @Override
  public Flow < I, Window < O > > window ( int size ) {
    if ( size <= 0 ) throw new IllegalArgumentException ( "size must be > 0" );
    return appendOp ( new WindowCountWrap ( size ) );
  }

  /// 2.7: time-based windowing with capacity bound.
  @NotNull
  @Override
  public Flow < I, Window < O > > window ( @NotNull Duration duration, int maxSize ) {
    Objects.requireNonNull ( duration, "duration" );
    if ( maxSize <= 0 ) throw new IllegalArgumentException ( "maxSize must be > 0" );
    return appendOp ( new WindowDurationWrap ( duration, maxSize ) );
  }

  /// Internal helper: returns a new FsFlow with the given op composed after this one's.
  ///
  /// The output type changes from O to whatever the op produces. Operators travel untyped
  /// inside the plan, so the new output type is asserted here rather than tracked.
  @SuppressWarnings ( { "unchecked", "rawtypes" } )
  private < P > Flow < I, P > appendOp ( Wrap < ? > op ) {
    return (Flow < I, P >) (Flow) new FsFlow <> ( recipe.then ( op ) );
  }
}
