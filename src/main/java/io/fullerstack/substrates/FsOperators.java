package io.fullerstack.substrates;

import io.humainary.substrates.api.Substrates;
import io.humainary.substrates.api.Substrates.Fault;
import io.humainary.substrates.api.Substrates.Pipe;
import io.humainary.substrates.api.Substrates.Receptor;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/// Operator implementations shared by FsFiber and FsFlow.
///
/// Every class here is a per-emission stateful or stateless processor that
/// implements `Consumer<E>`. Operators are constructed via {@link Wrap} factories
/// at materialise time so each pipe materialisation gets independent state.
///
/// Type-preserving by design — none of these change the emission type.
/// Type-changing belongs to FsFlow's own operators (e.g. MapWrap).
final class FsOperators {

  private FsOperators () {}

  /// Package name every type produced by this provider lives in. Used by
  /// [#foreign(Object)] to answer §15.1's provider-mismatch question without a
  /// per-implementation `instanceof` ladder that would silently reject this
  /// provider's own non-`FsPipe` carriers (`FsSink`'s channel pipes, say).
  private static final String PROVIDER_PACKAGE = "io.fullerstack.substrates";

  /// True when `candidate` was not produced by this provider.
  ///
  /// Spec §15.1 lists provider mismatch as MUST detect, naming exactly this
  /// case: "a Cell, Pipe, Fiber, or Flow from a different provider is passed
  /// to a composition operation (`Fiber.pipe`, `Flow.pipe`, `Flow.fiber`,
  /// `Fiber.fiber`)". Appendix A.2 binds that to [Fault] in Java.
  static boolean foreign ( Object candidate ) {
    return !PROVIDER_PACKAGE.equals ( candidate.getClass ().getPackageName () );
  }

  /// Builds a fault for a receiver that carries no subject of its own.
  ///
  /// `Fiber`, `Flow` and `Window` are composition/temporal values, not
  /// `Substrate`s, so none of them has a subject; §15.3's "identify the
  /// receiver subject" is answered with the provider's cortex subject — the
  /// nearest enclosing substrate every one of them belongs to.
  static Fault fault ( String operation, String message ) {
    return new Fault ( Substrates.cortex ().subject (), operation, message );
  }

  /// §5.8 stimulus time, held per circuit and confined to that circuit's worker.
  ///
  /// "A single ingress item (§5.3) and the entire transit cascade it triggers share one
  /// processing-time reading. Time advances on each ingress item … but does **not** advance
  /// across the internal transit hops of one cascade."
  ///
  /// Plain fields, not volatile: the holder is written by `IngressQueue.drainBatchLoop` and
  /// read by operators, both of which run only on the owning circuit's worker thread.
  static final class Stimulus {
    long    value;
    boolean valid;
  }

  /// The worker thread's stimulus holder, bound **once** when the worker starts.
  ///
  /// A circuit's worker is a single virtual thread that lives as long as the circuit
  /// (`FsCircuit`), so this is set once per circuit and never per emission. Reading it here
  /// rather than capturing the circuit at materialisation is also more correct: a chain
  /// reports the stimulus of the circuit *currently running it*, which §16.1 #16 allows to
  /// differ from the one it was materialised against.
  static final ThreadLocal < Stimulus > STIMULUS = new ThreadLocal <> ();

  /// §5.8's processing time for the ingress chain in progress.
  ///
  /// **Lazily established**, exactly as §5.8 permits: "An implementation is not required to
  /// read the clock for ingress items that no time-aware operator observes." The clock is read
  /// on the first time-aware observation of a chain and reused by every later one, so a transit
  /// hop that sleeps is invisible — internal cause-effect within a cascade is co-temporal.
  ///
  /// Off the worker (a foreign carrier driven through `emit`, say) there is no chain to be
  /// co-temporal with, so the clock is read directly.
  static long stimulus () {
    final Stimulus s = STIMULUS.get ();
    if ( s == null ) return System.nanoTime ();
    if ( !s.valid ) {
      s.value = System.nanoTime ();
      s.valid = true;
    }
    return s.value;
  }

  /// Operator factory: takes the downstream consumer and returns a wrapping
  /// consumer. Single shared shape lets FsFiber and FsFlow store operators
  /// in a uniform `Wrap[]` and materialise via a single loop with no
  /// `instanceof` checks.
  @FunctionalInterface
  interface Wrap < E > {
    Consumer < E > wrap ( Consumer < E > downstream );
  }

  // ─── Filtering / transforming ───────────────────────────────────────────────

  static final class Guard < E > implements Consumer < E > {
    final Predicate < ? super E > p;
    final Consumer < E >          d;

    Guard ( Predicate < ? super E > p, Consumer < E > d ) { this.p = p; this.d = d; }

    @Override
    public void accept ( E v ) { if ( p.test ( v ) ) d.accept ( v ); }
  }

  static final class GuardStateful < E > implements Consumer < E > {
    final BiPredicate < ? super E, ? super E > p;
    final Consumer < E >                       d;
    Object prev;

    GuardStateful ( E initial, BiPredicate < ? super E, ? super E > p, Consumer < E > d ) {
      this.prev = initial; this.p = p; this.d = d;
    }

    @Override
    @SuppressWarnings ( "unchecked" )
    public void accept ( E v ) {
      if ( p.test ( (E) prev, v ) ) { prev = v; d.accept ( v ); }
    }
  }

  /// Edge detector — emits on a transition between *consecutive* values.
  /// Unlike [GuardStateful] (which advances `prev` only on pass, per the `guard`
  /// contract), `prev` advances on **every** input regardless of pass/drop, so
  /// the bi-predicate always sees the immediately preceding value. Generalizes
  /// [Diff] to an arbitrary transition predicate.
  static final class Edge < E > implements Consumer < E > {
    final BiPredicate < ? super E, ? super E > p;
    final Consumer < E >                       d;
    Object prev;

    Edge ( E initial, BiPredicate < ? super E, ? super E > p, Consumer < E > d ) {
      this.prev = initial; this.p = p; this.d = d;
    }

    @Override
    @SuppressWarnings ( "unchecked" )
    public void accept ( E v ) {
      final boolean fire = p.test ( (E) prev, v );
      prev = v;                       // advance on every input, not only on pass
      if ( fire ) d.accept ( v );
    }
  }

  /// GuardStateful that allows null initial values (high/low operators).
  static final class GuardStatefulNullable < E > implements Consumer < E > {
    final BiPredicate < ? super E, ? super E > p;
    final Consumer < E > d;
    Object prev;

    GuardStatefulNullable ( BiPredicate < ? super E, ? super E > p, Consumer < E > d ) {
      this.p = p; this.d = d;
    }

    @Override
    @SuppressWarnings ( "unchecked" )
    public void accept ( E v ) {
      if ( p.test ( (E) prev, v ) ) { prev = v; d.accept ( v ); }
    }
  }

  static final class Diff < E > implements Consumer < E > {
    final Consumer < E > d;
    Object prev;
    boolean has;

    Diff ( Consumer < E > d )            { this.d = d; }

    Diff ( E initial, Consumer < E > d ) { this.prev = initial; this.has = true; this.d = d; }

    @Override
    public void accept ( E v ) {
      // Single field read of prev, identity short-circuit before equals,
      // and the has=true write only fires on first emission instead of every
      // emission. Emissions are non-null per spec §1.2 — no v != null check.
      if ( has ) {
        Object p = prev;
        if ( v != p && !v.equals ( p ) ) {
          prev = v;
          d.accept ( v );
        }
      } else {
        prev = v;
        has = true;
        d.accept ( v );
      }
    }
  }

  static final class Limit < E > implements Consumer < E > {
    final Consumer < E > d;
    final long           max;
    long count;

    Limit ( long max, Consumer < E > d ) { this.max = max; this.d = d; }

    @Override
    public void accept ( E v ) { if ( count++ < max ) d.accept ( v ); }
  }

  static final class Skip < E > implements Consumer < E > {
    final Consumer < E > d;
    final long           n;
    long count;

    Skip ( long n, Consumer < E > d ) { this.n = n; this.d = d; }

    @Override
    public void accept ( E v ) { if ( count++ >= n ) d.accept ( v ); }
  }

  static final class Replace < E > implements Consumer < E > {
    final UnaryOperator < E > t;
    final Consumer < E >      d;

    Replace ( UnaryOperator < E > t, Consumer < E > d ) { this.t = t; this.d = d; }

    @Override
    public void accept ( E v ) { E r = t.apply ( v ); if ( r != null ) d.accept ( r ); }
  }

  static final class Reduce < E > implements Consumer < E > {
    final BinaryOperator < E > op;
    final Consumer < E >       d;
    Object acc;

    Reduce ( E initial, BinaryOperator < E > op, Consumer < E > d ) {
      this.acc = initial; this.op = op; this.d = d;
    }

    @Override
    @SuppressWarnings ( "unchecked" )
    public void accept ( E v ) {
      // Spec §6.2.3 (Reduce): "If the result is present, it is emitted; if the
      // result is absent, the current emission is dropped and subsequent
      // operator invocations receive absence as the accumulator until the
      // operator returns a present value." The accumulator advances either way.
      E r = op.apply ( (E) acc, v );
      acc = r;
      if ( r != null ) d.accept ( r );
    }
  }

  static final class Peek < E > implements Consumer < E > {
    final Receptor < ? super E > r;
    final Consumer < E >         d;

    Peek ( Receptor < ? super E > r, Consumer < E > d ) {
      this.r = r; this.d = d;
    }

    @Override
    public void accept ( E v ) { r.receive ( v ); d.accept ( v ); }
  }

  static final class DropWhile < E > implements Consumer < E > {
    final Predicate < ? super E > p;
    final Consumer < E >          d;
    boolean dropping = true;

    DropWhile ( Predicate < ? super E > p, Consumer < E > d ) { this.p = p; this.d = d; }

    @Override
    public void accept ( E v ) { if ( dropping && p.test ( v ) ) return; dropping = false; d.accept ( v ); }
  }

  static final class TakeWhile < E > implements Consumer < E > {
    final Predicate < ? super E > p;
    final Consumer < E >          d;
    boolean taking = true;

    TakeWhile ( Predicate < ? super E > p, Consumer < E > d ) { this.p = p; this.d = d; }

    @Override
    public void accept ( E v ) { if ( taking && p.test ( v ) ) d.accept ( v ); else taking = false; }
  }

  static final class Integrate < E > implements Consumer < E > {
    final BinaryOperator < E >    op;
    final Predicate < ? super E > fire;
    final Consumer < E >          d;
    final Object                  initial;
    Object acc;

    Integrate ( E initial, BinaryOperator < E > op, Predicate < ? super E > fire, Consumer < E > d ) {
      this.initial = initial; this.acc = initial; this.op = op; this.fire = fire; this.d = d;
    }

    @Override
    @SuppressWarnings ( "unchecked" )
    public void accept ( E v ) {
      E r = op.apply ( (E) acc, v );
      acc = r;
      if ( fire.test ( r ) ) {
        acc = initial;
        // Spec §6.2.3 (Integrate): "If the fire predicate returns true for an
        // absent state, the stage resets to the initial value but the absent
        // output still drops the current emission."
        if ( r != null ) d.accept ( r );
      }
    }
  }

  static final class Relate < E > implements Consumer < E > {
    final BinaryOperator < E > op;
    final Consumer < E >       d;
    Object prev;

    Relate ( E initial, BinaryOperator < E > op, Consumer < E > d ) {
      this.prev = initial; this.op = op; this.d = d;
    }

    @Override
    @SuppressWarnings ( "unchecked" )
    public void accept ( E v ) {
      E r = op.apply ( (E) prev, v ); prev = v;
      if ( r != null ) d.accept ( r );
    }
  }

  // ─── Comparator-based / range ───────────────────────────────────────────────

  static final class Clamp < E > implements Consumer < E > {
    final Comparator < ? super E > c;
    final E              lower, upper;
    final Consumer < E > d;

    Clamp ( Comparator < ? super E > c, E lower, E upper, Consumer < E > d ) {
      this.c = c; this.lower = lower; this.upper = upper; this.d = d;
    }

    @Override
    public void accept ( E v ) {
      if ( c.compare ( v, lower ) < 0 ) d.accept ( lower );
      else if ( c.compare ( v, upper ) > 0 ) d.accept ( upper );
      else d.accept ( v );
    }
  }

  // ─── 2.3 operators ──────────────────────────────────────────────────────────

  static final class Chance < E > implements Consumer < E > {
    final double         p;
    final Consumer < E > d;

    Chance ( double p, Consumer < E > d ) { this.p = p; this.d = d; }

    @Override
    public void accept ( E v ) {
      if ( ThreadLocalRandom.current ().nextDouble () < p ) d.accept ( v );
    }
  }

  static final class Change < E > implements Consumer < E > {
    final Function < ? super E, ? > key;
    final Consumer < E >            d;
    Object  prevKey;
    boolean has;

    Change ( Function < ? super E, ? > key, Consumer < E > d ) { this.key = key; this.d = d; }

    @Override
    public void accept ( E v ) {
      Object k = key.apply ( v );
      if ( !has || !Objects.equals ( prevKey, k ) ) { prevKey = k; has = true; d.accept ( v ); }
    }
  }

  /// Ring-buffer lookback; emits initial for the first `depth` emissions.
  static final class Delay < E > implements Consumer < E > {
    final int            depth;
    final Object[]       buffer;
    final Consumer < E > d;
    int  idx;
    long count;

    Delay ( int depth, E initial, Consumer < E > d ) {
      this.depth  = depth;
      this.buffer = new Object[depth];
      Arrays.fill ( buffer, initial );
      this.d = d;
    }

    @Override
    @SuppressWarnings ( "unchecked" )
    public void accept ( E v ) {
      E out = (E) buffer[idx];
      buffer[idx] = v;
      idx = ( idx + 1 ) % depth;
      count++;
      d.accept ( out );
    }
  }

  /// Periodic sampling — emit every Nth value.
  static final class Every < E > implements Consumer < E > {
    final int            n;
    final Consumer < E > d;
    int count;

    Every ( int n, Consumer < E > d ) { this.n = n; this.d = d; }

    @Override
    public void accept ( E v ) { if ( ++count % n == 0 ) d.accept ( v ); }
  }

  /// Time-based rate limit (2.7) — per spec §6.2.3 `Fiber.every(Duration)`:
  ///
  /// 2.10 heartbeat — diff with a max-silence escape valve.
  ///
  /// Tracks the last emitted value and an anchor timestamp for the current
  /// run of duplicates. The first emission always passes. A *changing* value
  /// passes and clears the anchor (no clock read). A duplicate either anchors
  /// the run (first dup) and is dropped, or — if the anchor is `maxSilence`
  /// nanos old — re-emits the held value and re-anchors.
  ///
  /// Per spec §3415-3425 the clock is read **only on duplicates**, so a
  /// continuously-changing stream costs no more than `diff()`.
  static final class Heartbeat < E > implements Consumer < E > {

    private final long                  maxSilenceNanos;
    private final Consumer < ? super E > downstream;

    private E       prev;
    private boolean hasEmitted;
    private boolean anchored;
    private long    anchorNanos;

    Heartbeat ( long maxSilenceNanos, Consumer < ? super E > downstream ) {
      this.maxSilenceNanos = maxSilenceNanos;
      this.downstream      = downstream;
    }

    @Override
    public void accept ( E value ) {
      if ( ! hasEmitted ) {
        hasEmitted = true;
        prev       = value;
        anchored   = false;
        downstream.accept ( value );
        return;
      }
      if ( ! Objects.equals ( value, prev ) ) {
        prev     = value;
        anchored = false;
        downstream.accept ( value );
        return;
      }
      // Duplicate — consult the clock.
      long now = stimulus ();                       // §5.8: one reading per ingress chain
      if ( ! anchored ) {
        anchored    = true;
        anchorNanos = now;
        return;
      }
      if ( now - anchorNanos >= maxSilenceNanos ) {
        anchorNanos = now;
        downstream.accept ( prev );
      }
    }
  }

  ///   - The **first observed value anchors the interval and is dropped**.
  ///   - A later value observed after at least `durationNanos` has
  ///     elapsed is emitted.
  ///   - If more than one interval elapsed before that value arrives,
  ///     the internal clock advances by the elapsed interval slots rather
  ///     than by the late-arrival time — avoiding drift after overruns.
  static final class EveryTime < E > implements Consumer < E > {
    final long           durationNanos;
    final Consumer < E > d;
    boolean anchored = false;
    long    anchorNanos;   // start of current slot

    EveryTime ( long durationNanos, Consumer < E > d ) {
      this.durationNanos = durationNanos;
      this.d             = d;
    }

    @Override
    public void accept ( E v ) {
      final long now = stimulus ();                 // §5.8: one reading per ingress chain
      if ( !anchored ) {
        // First value anchors the interval and is dropped.
        anchored = true;
        anchorNanos = now;
        return;
      }
      final long elapsed = now - anchorNanos;
      if ( elapsed < durationNanos ) {
        return;   // still within the current slot
      }
      // Slot-aligned advance: skip the full slots that have elapsed.
      final long slots = elapsed / durationNanos;
      anchorNanos += slots * durationNanos;
      d.accept ( v );
    }
  }

  /// Two-state hysteresis: enters passing state when `enter` matches; exits
  /// (drops emissions) when `exit` matches.
  static final class Hysteresis < E > implements Consumer < E > {
    final Predicate < ? super E > enter;
    final Predicate < ? super E > exit;
    final Consumer < E >          d;
    boolean passing;

    Hysteresis ( Predicate < ? super E > enter, Predicate < ? super E > exit, Consumer < E > d ) {
      this.enter = enter; this.exit = exit; this.d = d;
    }

    @Override
    public void accept ( E v ) {
      if ( passing ) {
        if ( exit.test ( v ) ) passing = false;
        else d.accept ( v );
      } else {
        if ( enter.test ( v ) ) { passing = true; d.accept ( v ); }
      }
    }
  }

  /// Refractory-period gate — spec §6.2.3 (Inhibit): "When an emission passes
  /// through, the next `refractory` emissions reaching this operator are
  /// suppressed. Once the refractory count is exhausted, the next emission
  /// passes and the cycle repeats. The first emission always passes
  /// immediately."
  ///
  /// Distinguished from [Skip], which the same paragraph separates it from:
  /// Skip drops a one-off prefix and then passes everything for ever, whereas
  /// Inhibit is a cooldown that re-arms after every pass. "The phase
  /// difference matters when composed after event-detecting operators."
  static final class Inhibit < E > implements Consumer < E > {
    final int            refractory;
    final Consumer < E > d;
    int remaining;

    Inhibit ( int refractory, Consumer < E > d ) { this.refractory = refractory; this.d = d; }

    @Override
    public void accept ( E v ) {
      if ( remaining > 0 ) { remaining--; return; }
      // Re-armed before the successor runs: §15.4 treats a raising downstream
      // as having dropped the emission, not as having left it un-sent, so the
      // cooldown must already be set when user code is entered.
      remaining = refractory;
      d.accept ( v );
    }
  }

  /// Rising-edge detector — spec §6.2.3 (Pulse): "An internal `active` state
  /// starts false. On each emission, if the predicate returns true **and the
  /// previous state was false**, the current value is emitted and the state
  /// becomes true; otherwise the emission is dropped and the state tracks the
  /// current predicate result. The gate re-arms whenever the predicate returns
  /// false — the next true reading emits again."
  ///
  /// A stateless [Guard] over the same predicate is a *level* detector: it
  /// re-emits every "still true" reading. §6.2.3 contrasts Pulse with [Edge]
  /// precisely on "retaining only a single boolean of state", which a guard
  /// does not have.
  static final class Pulse < E > implements Consumer < E > {
    final Predicate < ? super E > p;
    final Consumer < E >          d;
    boolean active;

    Pulse ( Predicate < ? super E > p, Consumer < E > d ) { this.p = p; this.d = d; }

    @Override
    public void accept ( E v ) {
      final boolean reading = p.test ( v );
      if ( reading && !active ) {
        active = true;
        d.accept ( v );
      } else {
        active = reading;
      }
    }
  }

  /// Sliding-window aggregation — spec §6.2.3 (Rolling): "Once the buffer is
  /// full, each subsequent emission folds the entire buffer — from `identity`,
  /// over all `size` values, **in insertion order** — through `combiner` and
  /// emits the result **when present**. The combiner MAY return absence; the
  /// fold continues with absence as the accumulator, and an absent final
  /// aggregate drops the current emission. The first `size - 1` inputs are
  /// warm-up and produce no output."
  ///
  /// The buffer is held in insertion order by shifting left once full rather
  /// than by a modular write index. That is not tidiness: a ring's `[0, size)`
  /// read order *is* insertion order until the ring wraps and is a rotation of
  /// it afterwards, so a non-commutative combiner (concatenation, subtraction,
  /// first-wins) silently changes answer on the `size + 1`-th input while every
  /// commutative test still passes. The fold is O(size) per emission either
  /// way, which §6.2.3 mandates so that non-invertible combiners work.
  static final class Rolling < E > implements Consumer < E > {
    final int                  size;
    final BinaryOperator < E > combiner;
    final E                    identity;
    final Object[]             buffer;
    final Consumer < E >       d;
    int filled;

    Rolling ( int size, BinaryOperator < E > combiner, E identity, Consumer < E > d ) {
      this.size     = size;
      this.combiner = combiner;
      this.identity = identity;
      this.buffer   = new Object[size];
      this.d        = d;
    }

    @Override
    @SuppressWarnings ( "unchecked" )
    public void accept ( E v ) {
      if ( filled == size ) {
        System.arraycopy ( buffer, 1, buffer, 0, size - 1 );
        buffer[size - 1] = v;
      } else {
        buffer[filled++] = v;
        if ( filled < size ) return;   // warm-up: the first size-1 inputs emit nothing
      }
      E acc = identity;
      for ( int i = 0; i < size; i++ ) acc = combiner.apply ( acc, (E) buffer[i] );
      if ( acc != null ) d.accept ( acc );
    }
  }

  /// Steady run length: emit when the same value (Objects.equals) repeats `count` times.
  static final class SteadyN < E > implements Consumer < E > {
    final int            count;
    final Consumer < E > d;
    Object prev;
    int    run;

    SteadyN ( int count, Consumer < E > d ) { this.count = count; this.d = d; }

    @Override
    public void accept ( E v ) {
      if ( Objects.equals ( v, prev ) ) {
        run++;
      } else {
        prev = v;
        run = 1;
      }
      if ( run == count ) d.accept ( v );
    }
  }

  /// Steady run length: emit when the (prev, curr) bi-predicate has held `count` times in a row.
  static final class SteadyPredicate < E > implements Consumer < E > {
    final int                                  count;
    final BiPredicate < ? super E, ? super E > p;
    final Consumer < E >                       d;
    Object  prev;
    boolean has;
    int     run;

    SteadyPredicate ( int count, BiPredicate < ? super E, ? super E > p, Consumer < E > d ) {
      this.count = count; this.p = p; this.d = d;
    }

    @Override
    @SuppressWarnings ( "unchecked" )
    public void accept ( E v ) {
      if ( has && p.test ( (E) prev, v ) ) run++;
      else run = 1;
      prev = v;
      has  = true;
      if ( run == count ) d.accept ( v );
    }
  }

  /// Tumbling (non-overlapping) window: collect `size` emissions, fold with combiner over identity, emit.
  static final class Tumble < E > implements Consumer < E > {
    final int                  size;
    final BinaryOperator < E > combiner;
    final E                    identity;
    final Consumer < E >       d;
    E   acc;
    int filled;

    Tumble ( int size, BinaryOperator < E > combiner, E identity, Consumer < E > d ) {
      this.size     = size;
      this.combiner = combiner;
      this.identity = identity;
      this.acc      = identity;
      this.d        = d;
    }

    @Override
    public void accept ( E v ) {
      // §6.2.3: "Tumble fires after a fixed number of inputs regardless of
      // content", so the count advances and the batch boundary is taken before
      // the combiner runs — a raising combiner drops its batch without shifting
      // the batch grid for the life of the materialization.
      if ( ++filled == size ) {
        filled = 0;
        final E carried = acc;
        // "if the batch fires with an absent accumulator the current emission
        // is dropped **before the state resets** to `identity`" — so the reset
        // is completed before user code is entered.
        acc = identity;
        final E r = combiner.apply ( carried, v );
        if ( r != null ) d.accept ( r );
      } else {
        acc = combiner.apply ( acc, v );
      }
    }
  }

  // ─── 2.5 operators ─────────────────────────────────────────────────────────

  /// Distinct (unbounded): suppresses any value previously seen. Memory grows
  /// with stream cardinality. Spec §6.2.3.
  static final class Distinct < E > implements Consumer < E > {
    final Consumer < E > d;
    final HashSet < E >  seen = new HashSet <> ();

    Distinct ( Consumer < E > d ) { this.d = d; }

    @Override
    public void accept ( E v ) { if ( seen.add ( v ) ) d.accept ( v ); }
  }

  /// Distinct (capacity): FIFO-windowed duplicate suppression. LinkedHashSet
  /// gives O(1) contains/add plus insertion order for FIFO eviction. Suppressed
  /// duplicates do NOT refresh window position. Spec §6.2.3.
  static final class DistinctCapacity < E > implements Consumer < E > {
    final int                 capacity;
    final Consumer < E >      d;
    final LinkedHashSet < E > window;

    DistinctCapacity ( int capacity, Consumer < E > d ) {
      this.capacity = capacity;
      this.d        = d;
      this.window   = new LinkedHashSet <> ();
    }

    @Override
    public void accept ( E v ) {
      if ( window.contains ( v ) ) return;
      window.add ( v );
      if ( window.size () > capacity ) {
        Iterator < E > it = window.iterator ();
        it.next ();
        it.remove ();
      }
      d.accept ( v );
    }
  }

  /// Route: predicate-matched values are diverted to `side` and dropped from
  /// the main pipeline; non-matching values pass through. Stateless. Spec §6.2.2.
  static final class Route < E > implements Consumer < E > {
    final Predicate < ? super E > p;
    final Pipe < E >              side;
    final Consumer < E >          d;

    Route ( Predicate < ? super E > p, Pipe < E > side, Consumer < E > d ) {
      this.p = p; this.side = side; this.d = d;
    }

    @Override
    public void accept ( E v ) {
      if ( p.test ( v ) ) side.emit ( v );
      else                d.accept ( v );
    }
  }

  /// Tee: fan-out to `side` then continue downstream. Stateless. Spec §6.2.2.
  static final class Tee < E > implements Consumer < E > {
    final Pipe < E >     side;
    final Consumer < E > d;

    Tee ( Pipe < E > side, Consumer < E > d ) { this.side = side; this.d = d; }

    @Override
    public void accept ( E v ) {
      side.emit ( v );
      d.accept ( v );
    }
  }

  /// Streak: emit the Nth consecutive matching emission, then reset.
  /// Non-matching emissions reset the counter and are dropped. Spec §6.2.3.
  /// (required == 1 is handled at FsFiber level — degenerates to Guard.)
  static final class Streak < E > implements Consumer < E > {
    final int                     required;
    final Predicate < ? super E > matches;
    final Consumer < E >          d;
    int count;

    Streak ( int required, Predicate < ? super E > matches, Consumer < E > d ) {
      this.required = required; this.matches = matches; this.d = d;
    }

    @Override
    public void accept ( E v ) {
      if ( matches.test ( v ) ) {
        if ( ++count >= required ) {
          count = 0;
          d.accept ( v );
        }
      } else {
        count = 0;
      }
    }
  }

  /// When: predicate-matched values traverse a pre-materialised sub-fiber that
  /// terminates at `d`; non-matching values pass through directly. Stateless
  /// at this stage — the sub-fiber's per-materialization state is materialised
  /// here (once per outer materialization). Spec §6.2.2.
  static final class When < E > implements Consumer < E > {
    final Predicate < ? super E > p;
    final Consumer < E >          matched;
    final Consumer < E >          d;

    When ( Predicate < ? super E > p, FsFiber < E > sub, Consumer < E > d ) {
      this.p       = p;
      this.d       = d;
      this.matched = sub.materialise ( d );
    }

    @Override
    public void accept ( E v ) {
      if ( p.test ( v ) ) matched.accept ( v );
      else                d.accept ( v );
    }
  }
}
