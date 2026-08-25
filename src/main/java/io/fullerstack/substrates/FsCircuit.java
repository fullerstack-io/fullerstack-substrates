package io.fullerstack.substrates;

import static io.humainary.substrates.api.Substrates.cortex;
import static java.util.Objects.requireNonNull;

import io.humainary.substrates.api.Substrates.Bank;
import io.humainary.substrates.api.Substrates.Basin;
import io.humainary.substrates.api.Substrates.Capture;
import io.humainary.substrates.api.Substrates.Cell;
import io.humainary.substrates.api.Substrates.Circuit;
import io.humainary.substrates.api.Substrates.Conduit;
import io.humainary.substrates.api.Substrates.Fault;
import io.humainary.substrates.api.Substrates.Current;
import io.humainary.substrates.api.Substrates.Idempotent;
import io.humainary.substrates.api.Substrates.Name;
import io.humainary.substrates.api.Substrates.New;
import io.humainary.substrates.api.Substrates.NotNull;
import io.humainary.substrates.api.Substrates.Pin;
import io.humainary.substrates.api.Substrates.Pipe;
import io.humainary.substrates.api.Substrates.Port;
import io.humainary.substrates.api.Substrates.Provided;
import io.humainary.substrates.api.Substrates.Pulse;
import io.humainary.substrates.api.Substrates.Queued;
import io.humainary.substrates.api.Substrates.Receptor;
import io.humainary.substrates.api.Substrates.Registrar;
import io.humainary.substrates.api.Substrates.Routing;
import io.humainary.substrates.api.Substrates.Sink;
import io.humainary.substrates.api.Substrates.Subject;
import io.humainary.substrates.api.Substrates.Subscriber;
import io.humainary.substrates.api.Substrates.Ticker;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Single-digit nanosecond latency Substrates Circuit implementation.
 *
 * <p>Achieves low emission latency through:
 * <ul>
 *   <li>Intrusive MPSC linked list for ingress (external threads)
 *   <li>Pre-allocated ring buffer for transit (cascade emissions)
 *   <li>VarHandle release/acquire semantics for thread coordination
 *   <li>Thread identity routing (worker vs external)
 *   <li>Self-waking timed park — producers never wake the worker
 * </ul>
 *
 * <p><b>Dual-queue architecture:</b>
 * <ul>
 *   <li><b>Ingress:</b> MPSC linked list for external threads (wait-free)
 *   <li><b>Transit:</b> Ring buffer for cascade emissions (single-threaded, zero allocation)
 * </ul>
 */
@Provided
public final class FsCircuit implements Circuit {

  // Spin count before parking (~100μs with Thread.onSpinWait)
  // Based on benchmarking: hot spin provides no benefit, cool spin of 1000 is optimal
  /// Spin iterations an awaiter performs before falling back to park().
  ///
  /// The marker fires within ~2µs in the common case (the worker self-wakes every
  /// PARK_NANOS, drains, fires the marker), so spinning catches the signal without paying
  /// the ~5-15µs virtual-thread park/unpark round trip.
  ///
  /// Tuned by sweep, and the shape is a cliff rather than a slope: 500 falls below it
  /// (shallow cyclic_emit_await regresses 10ns/op), 1000 catches the marker, 5000+ wastes
  /// ~3ns/op on deep-cascade awaits whose 140µs the spin can never catch anyway. Real
  /// awaits are rare — tests, shutdown, bridges — so the spin cost is negligible.
  private static final int AWAIT_SPIN_COUNT = 1_000;

  /// Bounded park on the slow path, so a close racing an await is observed without the
  /// awaiter having registered itself anywhere.
  private static final long AWAIT_PARK_NANOS = 1_000_000L;

  private static final int SPIN_COUNT = 1000;

  // Timed park interval when no work after spin phase.
  // Worker self-wakes — producers never need to unpark.
  // 1μs: on virtual threads this is just an FJP reschedule, no carrier blocking.
  private static final long PARK_NANOS = 1_000L;


  // ─────────────────────────────────────────────────────────────────────────────
  // Circuit state (read-only after construction)
  // ─────────────────────────────────────────────────────────────────────────────

  private final FsCortex            cortex;
  private final Subject < Circuit > subject;
  private final Thread              worker;

  /// 2.4: lazily-initialised Current for this circuit's execution context.
  /// Must remain stable for the circuit's lifetime (spec §11.3).
  private volatile Current circuitCurrent;

  /// 2.8: lazily-initialised scheduler shared by all Tickers on this circuit
  /// (spec §11.5). Single-thread daemon — tick callbacks just submit emissions
  /// to the target pipe (ingress queue), so we don't need parallelism here.
  private volatile ScheduledExecutorService scheduler;

  // ─────────────────────────────────────────────────────────────────────────────
  // Queue infrastructure
  // ─────────────────────────────────────────────────────────────────────────────

  private final IngressQueue     ingress = new IngressQueue ();
  private final TransitQueueRing transit = new TransitQueueRing ();

  // ─────────────────────────────────────────────────────────────────────────────
  // Synchronization state
  // ─────────────────────────────────────────────────────────────────────────────

  // Set by close marker (runs on circuit thread) to signal worker exit.
  // Plain field - only accessed from circuit thread.
  private boolean shouldExit;

  // Set when close() is called to reject new emissions
  volatile boolean closed;

  // Pre-allocated marker receivers — ReceptorAdapter wrapping marker lambdas.
  // Drain loop splits the call site: isMarker() identity check routes markers
  // to fireMarker() (cold, separate type profile), keeping the hot-path
  // r.accept(v) and receptor.receive() fully monomorphic — zero class_check traps.
  private final Consumer < Object > awaitMarkerReceiver;
  private final Consumer < Object > closeMarkerReceiver;

  /// §5.8's processing-time reading for the ingress chain in progress.
  ///
  /// Invalidated once per ingress item by `IngressQueue.drainBatchLoop` and established lazily
  /// by the first time-aware operator that observes it, so a whole transit cascade — however
  /// long it takes in wall time — is one causal moment. Package-private: the ingress drain and
  /// the worker bind are the only writers, and both run on this circuit's worker.
  final FsOperators.Stimulus stimulus = new FsOperators.Stimulus ();

  // ─────────────────────────────────────────────────────────────────────────────
  // ReceptorAdapter — wraps a user Receptor in a Consumer<Object>.
  //
  // Hot path: ingress/transit drain → r.accept(v) → receptor.receive(v).
  // Used ONLY for user receptors (circuit.pipe(receptor) and friends).
  // Markers and circuit jobs use distinct concrete classes below so the
  // bci=5 receptor.receive call site here stays monomorphic per circuit.
  // ─────────────────────────────────────────────────────────────────────────────

  @SuppressWarnings ( "unchecked" )
  static final class ReceptorAdapter < E > implements Consumer < Object >, Receptor < E > {
    final Receptor < ? super E > receptor;

    ReceptorAdapter ( Receptor < ? super E > receptor ) {
      this.receptor = receptor;
    }

    @Override
    public void accept ( Object o ) {
      receptor.receive ( (E) o );
    }

    @Override
    public void receive ( E emission ) {
      receptor.receive ( emission );
    }
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Marker classes — DO NOT COLLAPSE.
  //
  // AwaitMarker, CloseMarker, CircuitJob, and ReceptorAdapter (above) are
  // deliberately separate concrete classes, each with a distinct .accept()
  // body. Each call site in the dispatch chain only ever sees ONE of these
  // class types, which keeps every site monomorphic in the JIT type profile:
  //
  //   ReceptorAdapter.accept → receptor.receive   (user emissions, hot path)
  //   AwaitMarker.accept     → circuit.onAwaitMarker
  //   CloseMarker.accept     → circuit.onCloseMarker
  //   CircuitJob.accept      → action.run         (subscribe / unsubscribe etc.)
  //
  // Collapsing any of these into a shared base reintroduces a bimorphic (or
  // worse) call-site profile on r.accept(v) in the drain loop. HotSpot then
  // emits a class_check trap that deoptimizes the inlined receptor chain,
  // regressing PipeOps.async_emit_batch_await from ~22 ns to ~30+ ns.
  //
  // Canary benchmark: io.humainary.substrates.jmh.PipeOps.async_emit_batch_await
  // Structural test:  FsCircuitMarkerInvariantTest
  // ─────────────────────────────────────────────────────────────────────────────

  /// One awaiter's barrier: a released flag and the thread to wake if it parked.
  ///
  /// Deliberately not a `CountDownLatch`. The marker fires in single-digit microseconds, so
  /// the common case must be a spin on a volatile read, not a blocking synchronizer with an
  /// AQS node behind it — parking costs more than the barrier it is waiting for.
  ///
  /// Held in a `ThreadLocal` and reused, so a thread that awaits repeatedly allocates once
  /// for its lifetime rather than once per call. Reuse is safe because a barrier is only
  /// ever recycled after its own await has returned.
  static final class AwaitBarrier {
    final Thread thread;
    volatile boolean released;
    AwaitBarrier ( Thread thread ) { this.thread = thread; }
  }

  private static final ThreadLocal < AwaitBarrier > AWAIT_BARRIER =
    ThreadLocal.withInitial ( () -> new AwaitBarrier ( Thread.currentThread () ) );

  /// Await marker — delivered through the ingress queue and routed to
  /// {@link FsCircuit#onAwaitMarker(Object)} via isMarker() identity check.
  static final class AwaitMarker implements Consumer < Object > {
    final FsCircuit circuit;
    AwaitMarker ( FsCircuit circuit ) { this.circuit = circuit; }
    @Override public void accept ( Object o ) { circuit.onAwaitMarker ( o ); }
  }

  /// Close marker — delivered through the ingress queue and routed to
  /// {@link FsCircuit#onCloseMarker(Object)} via isMarker() identity check.
  static final class CloseMarker implements Consumer < Object > {
    final FsCircuit circuit;
    CloseMarker ( FsCircuit circuit ) { this.circuit = circuit; }
    @Override public void accept ( Object o ) { circuit.onCloseMarker ( o ); }
  }

  /// Generic one-shot circuit job — wraps a Runnable for ingress dispatch.
  /// Used by FsConduit subscribe/unsubscribe and similar circuit-thread-only work.
  /// Distinct compiled body keeps the action.run() call site separate from
  /// ReceptorAdapter.accept's receptor.receive() call site.
  static final class CircuitJob implements Consumer < Object > {
    final Runnable action;
    CircuitJob ( Runnable action ) { this.action = action; }
    @Override public void accept ( Object o ) { action.run (); }
  }

  /// Pulse probe — single-use diagnostic carrier (spec §5.7, 2.4).
  /// Stamps `dequeued` on the worker thread and unparks the caller.
  /// Distinct concrete class so it doesn't pollute ReceptorAdapter's call-site profile.
  static final class PulseProbe implements Consumer < Object > {
    final Thread caller;
    volatile long dequeued;

    PulseProbe ( Thread caller ) {
      this.caller = caller;
    }

    @Override
    public void accept ( Object ignored ) {
      dequeued = System.nanoTime ();   // volatile write publishes the timestamp
      LockSupport.unpark ( caller );
    }
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Construction
  // ─────────────────────────────────────────────────────────────────────────────

  public FsCircuit ( FsCortex cortex, Subject < Circuit > subject ) {
    this.cortex  = cortex;
    this.subject = subject;

    // Pre-allocate marker receivers as concrete classes — distinct types
    // from ReceptorAdapter so the hot-path r.accept(v) → receptor.receive()
    // call site stays monomorphic when only user receptors flow through it.
    this.awaitMarkerReceiver = new AwaitMarker ( this );
    this.closeMarkerReceiver = new CloseMarker ( this );

    // Create the worker unstarted, bind this circuit's Current to it, then start.
    //
    // §5.7: the worker thread's Current IS this circuit's Current, so that
    // `cortex.current() == circuit.current()` is the on-circuit guard the spec
    // documents. Binding happens on the creating thread, before the worker runs,
    // so no dispatched work can observe an unbound worker. Thread ids are
    // assigned at construction, so an unstarted thread already has one.
    final Thread w = Thread.ofVirtual ()
      .name ( "circuit-" + subject.name () )
      .unstarted ( this::workerLoop );
    this.worker = w;
    w.start ();
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Emission API (called by FsPipe)
  // ─────────────────────────────────────────────────────────────────────────────

  /**
   * Submit emission from external thread to ingress queue.
   * Fire-and-forget: just enqueue, no worker wake-up.
   * Worker self-wakes via timed park — producers never pay unpark cost.
   */
  @jdk.internal.vm.annotation.ForceInline
  final void submitIngress ( Consumer < Object > receiver, Object value ) {
    ingress.enqueue ( receiver, value );
  }

  /**
   * Submit cascade emission from worker thread to transit queue.
   * Single-threaded — uses shared QChunk buffer for zero-allocation transit.
   */
  @jdk.internal.vm.annotation.ForceInline
  final void submitTransit ( Consumer < Object > receiver, Object value ) {
    transit.enqueue ( receiver, value );
  }

  /**
   * Submit an emission to this circuit from an unknown thread — the same
   * routing decision {@link FsPipe#emit} makes, factored out so that
   * Flow/Fiber terminals reach the right queue too.
   *
   * <p>SPEC §5.3: a cascade re-entry raised on this circuit's own worker is
   * transit work; anything raised from another thread — including another
   * circuit's worker — is an ingress admission of <em>this</em> circuit. The
   * transit ring is single-threaded, so routing a foreign-thread emission into
   * it is a data race, not merely a lost emission.
   */
  @jdk.internal.vm.annotation.ForceInline
  final void submit ( Consumer < Object > receiver, Object value ) {
    if ( onWorker () ) {
      transit.enqueue ( receiver, value );
    } else {
      ingress.enqueue ( receiver, value );
    }
  }

  /**
   * Returns true if transit queue has pending cascade work.
   * Plain field read — single-threaded, no volatile needed.
   */
  @jdk.internal.vm.annotation.ForceInline
  boolean transitHasWork () {
    return transit.hasWork ();
  }

  /**
   * Drain all queued cascade emissions. Delegates to TransitQueueRing.
   */
  @jdk.internal.vm.annotation.ForceInline
  boolean drainTransit () {
    return transit.drain ();
  }

  /**
   * Identity check: is this receiver a marker (await/close)?
   * Splits the call site so the hot path {@code r.accept(v)} only
   * ever sees regular ReceptorAdapters — monomorphic, no class_check traps.
   */
  @jdk.internal.vm.annotation.ForceInline
  boolean isMarker ( Consumer < Object > r ) {
    return r == awaitMarkerReceiver || r == closeMarkerReceiver;
  }

  /**
   * Fire a marker receiver. NOT inlined — cold path with its own
   * type profile so marker receptor types don't pollute the hot path.
   */
  void fireMarker ( Consumer < Object > marker, Object value ) {
    marker.accept ( value );
  }

  /// §11.3's context comparison: is the caller running on this circuit's context?
  ///
  /// The specification states the idiom as "compare `Cortex.current()` against the circuit's
  /// `current()`" (§11.3, §11.6), and names `Thread.currentThread()` as the Java projection
  /// of a `Current`. This circuit's `Current` is interned against its worker, so the identity
  /// below answers the same question with a reference compare instead of a per-call lookup.
  ///
  /// It exists so the worker `Thread` never leaves this class. It used to be handed out by a
  /// `worker()` accessor whose own javadoc admitted it was "for thread identity checks" —
  /// five call sites each rewrote the comparison, and every one of them held a reference
  /// they could have parked or interrupted.
  ///
  /// **Why this is the thread and not `cortex.current() == current()`.** That literal form is
  /// what §11.3 writes, and it was tried here. The two are the same predicate: §5.7 requires
  /// this circuit's `Current` to BE its worker's, so they agree by construction. Measured, the
  /// literal form adds a `ThreadLocal` read to every emission — `CortexOps.current` is
  /// 2.775 +/- 0.513 ns/op against a `PipeOps.async_emit_batch` of roughly 24 ns/op, so about
  /// a tenth of the emission path for no semantic difference. (The direct A/B on `PipeOps` was
  /// 26.2 +/- 7.2 against 23.5 +/- 5.4 — overlapping, so the arithmetic above is the honest
  /// evidence and that pair is not.) §11.3 settles it: the mechanism "is not specified; only
  /// the identity and temporal guarantees are REQUIRED", and it names `Thread.currentThread()`
  /// as the Java one.
  ///
  /// The thread form is also the more robust of the two: it answers correctly even before the
  /// worker has bound its `Current`, a window the literal form would report as "not on the
  /// circuit". Nothing reaches it today — the bind is `workerLoop`'s first statement — but the
  /// asymmetry is worth knowing.
  ///
  /// Every site goes through here, `submit` included. `submit` is the hottest method in the
  /// projection and carries `@ForceInline`; this is a final method on a final class reading
  /// one field, so the JIT inlines it to the same compare. Correctness of the §5.3 routing
  /// decision having exactly one definition is worth more than keeping a hand-copied branch,
  /// and `PipeOps` is the measurement that says whether that was free.
  final boolean onWorker () {
    return Thread.currentThread () == worker;
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Worker Loop - FIFO depth-first processing with spin-before-park
  // ─────────────────────────────────────────────────────────────────────────────

  private void workerLoop () {
    // §11.3/§5.7: this circuit's Current IS its worker's Current, so that
    // `cortex.current() == circuit.current()` is the guard the spec documents. Minting a
    // separate per-thread Current here would make that comparison always report "not on the
    // circuit" — the exact failure the guard exists to prevent.
    cortex.bindCurrent ( (FsCurrent) current () );
    // §5.8: bind this circuit's stimulus holder to its worker, once. The worker is a single
    // virtual thread living as long as the circuit, so time-aware operators reach the holder
    // with a ThreadLocal read and never a per-emission lookup of the circuit.
    FsOperators.STIMULUS.set ( stimulus );
    drainLoop ();
    // §5.7: the binding lives exactly as long as the worker does. Released here
    // rather than in close(), because work admitted before the close marker is
    // still dispatched afterwards and must keep observing the circuit's Current.
    cortex.unbindCurrent ();
    FsOperators.STIMULUS.remove ();
  }

  private void drainLoop () {
    // Hoist final field to local — guarantees register allocation.
    final IngressQueue q = ingress;

    for ( ; ; ) {

      // Drain up to 64 ingress slots with depth-first cascade interleaving.
      boolean didWork = q.drainBatch ( this );

      if ( didWork ) continue;

      // shouldExit is set by close marker (runs as ingress job on this thread).
      // Check here after drain confirms empty — no work left, safe to exit.
      if ( shouldExit ) return;

      // No work available - spin before parking
      Object found = null;

      for ( int i = 0; i < SPIN_COUNT && found == null; i++ ) {
        Thread.onSpinWait ();
        found = q.peek ();
      }

      if ( found == null ) {
        // Self-waking park — no producer wake-up needed.
        // Worker resumes after PARK_NANOS or explicit unpark (await/close).
        LockSupport.parkNanos ( PARK_NANOS );
      }
    }
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Pipe Factory Helper
  // ─────────────────────────────────────────────────────────────────────────────

  /// Creates a pipe wrapping a receptor adapter.
  private < E > FsPipe < E > newPipe ( Receptor < ? super E > receptor ) {
    return new FsPipe <> ( new ReceptorAdapter <> ( receptor ), this );
  }


  // ─────────────────────────────────────────────────────────────────────────────
  // Lifecycle
  // ─────────────────────────────────────────────────────────────────────────────

  @Override
  public Subject < Circuit > subject () {
    return subject;
  }

  /// 2.4: Returns the Current identifying this circuit's execution context.
  /// The returned reference MUST remain stable for the circuit's lifetime
  /// (spec §11.3).
  @NotNull
  @Override
  public Current current () {
    Current c = circuitCurrent;
    if ( c == null ) {
      synchronized ( this ) {
        c = circuitCurrent;
        if ( c == null ) {
          FsSubject < Current > cs = new FsSubject <> (
            subject.name (),
            (FsSubject < ? >) subject,
            Current.class );
          c = new FsCurrent ( cs );
          circuitCurrent = c;
        }
      }
    }
    return c;
  }

  @Override
  public void await () {
    checkExternalCaller ( "await" );
    if ( closed ) {
      awaitClosed ();
      return;
    }
    awaitImpl ();
  }

  /// Conformance §16.1 #13: await / pulse / closeAwait MUST signal illegal
  /// context use when called from within the circuit's worker thread.
  /// Used by callers that need to fail-fast before any side effect runs.
  void checkExternalCaller ( String op ) {
    if ( onWorker () ) {
      throw new IllegalStateException (
        "Cannot call Circuit::" + op + " from within a circuit's thread" );
    }
  }

  /// Conformance §9.1 + §16.1 #9: open-required synchronous operations
  /// (conduit, bank, basin, cell, subscriber, subscribe, Bank.get)
  /// MUST signal a closed-resource Fault when the receiver has accepted close.
  void requireOpen ( String op ) {
    if ( closed ) throw new Fault ( subject, op, "circuit is closed" );
  }

  /// Whether this circuit has accepted close. Used by owned resources
  /// (Conduit/Ticker/Basin) to propagate the open-required guard through
  /// to their own open-required ops.
  boolean isClosed () {
    return closed;
  }

  /// 2.8: lazy shared scheduler for Tickers. Single-thread daemon — the
  /// scheduler thread only enqueues the emission onto the circuit's
  /// ingress queue, so it stays lightweight.
  ScheduledExecutorService scheduler () {
    ScheduledExecutorService s = scheduler;
    if ( s != null ) return s;
    synchronized ( this ) {
      s = scheduler;
      if ( s != null ) return s;
      s = Executors.newSingleThreadScheduledExecutor ( r -> {
        Thread t = new Thread ( r, "circuit-ticker-" + subject.name () );
        t.setDaemon ( true );
        return t;
      } );
      scheduler = s;
      return s;
    }
  }

  /// 2.4: Submits a no-op probe through the circuit and returns its timing
  /// snapshot. Captures four timestamps along the round-trip path: caller
  /// stamps `start` and `enqueued`, worker stamps `dequeued`, caller stamps
  /// `stop` after the probe fires. Returns empty when the circuit has
  /// terminated and the probe could not traverse the queue (spec §5.7).
  @NotNull
  @Override
  public Optional < Pulse > pulse () {
    checkExternalCaller ( "pulse" );
    if ( !worker.isAlive () ) return Optional.empty ();

    PulseProbe probe = new PulseProbe ( Thread.currentThread () );
    long start = System.nanoTime ();
    submitIngress ( probe, null );
    long enqueued = System.nanoTime ();

    LockSupport.unpark ( worker );      // cold path — wake worker promptly

    while ( probe.dequeued == 0L ) {
      if ( !worker.isAlive () ) return Optional.empty ();
      LockSupport.parkNanos ( this, 1_000_000L );  // 1 ms termination probe
    }

    long stop = System.nanoTime ();
    return Optional.of ( new PulseImpl ( start, enqueued, probe.dequeued, stop ) );
  }

  /// 2.10 spec §5887-5917: Pulse changed from record to interface. Provider
  /// returns an immutable carrier of the four nanosecond probe readings.
  private record PulseImpl ( long start, long enqueued, long dequeued, long stop ) implements Pulse {}

  private void awaitClosed () {
    try {
      worker.join ();
    } catch ( InterruptedException e ) {
      Thread.currentThread ().interrupt ();
    }
  }

  /**
   * Await: admit a marker at this caller's own barrier position, then spin for it.
   *
   * Every awaiter gets its own marker and its own barrier — see the note in the body for
   * why callers must not share one.
   */
  private void awaitImpl () {

    // §5.5 is positional: the barrier covers "every circuit operation accepted BEFORE the
    // await call". Each awaiter therefore admits its OWN marker at its OWN position.
    //
    // The previous implementation CAS'd a single awaiterThread slot and let later callers
    // piggyback on the FIRST awaiter's marker by spinning until that slot cleared. That
    // marker was admitted before the later caller's work, so the piggybacker could be
    // released with its own emission still queued — the barrier clause violated, and
    // §5.5's "each suspends independently" is what made piggybacking illegal to begin with.
    // Reproduced by AwaitBarrierTest#concurrentAwaitersEachGetTheirOwnBarrier.
    //
    // The marker RECEIVER is still the pre-allocated AwaitMarker singleton — only the
    // payload changed, from null to this awaiter's barrier. That matters: the dispatch-chain
    // comment above requires every call site to stay monomorphic, so the marker class
    // hierarchy must not grow a variant.
    final AwaitBarrier barrier = AWAIT_BARRIER.get ();
    barrier.released = false;

    submitIngress ( awaitMarkerReceiver, barrier );
    LockSupport.unpark ( worker );

    // Spin first, and in the common case that is the whole of it. The marker fires in
    // single-digit microseconds, which is faster than a park/unpark round trip — and on a
    // virtual thread, parking additionally unmounts the carrier.
    //
    // Nothing is registered anywhere: an earlier revision kept the barrier in a
    // ConcurrentLinkedQueue so close() could release it, which put a node allocation, a CAS
    // and an O(n) remove() traversal on EVERY await, to serve a case that only arises when
    // a close races the call. The check below covers that case at zero cost to the common
    // one, which is the right trade for a barrier that usually completes in microseconds.
    for ( int i = 0; i < AWAIT_SPIN_COUNT; i++ ) {
      if ( barrier.released ) return;
      Thread.onSpinWait ();
    }

    // Only a genuinely slow circuit reaches here. Bounded parks rather than an indefinite
    // one: a marker admitted at or after the close marker is never dispatched (§9.1), so
    // its flag is never set, and §5.5 requires await to return immediately once the circuit
    // is closed. Re-checking on each wake also covers spurious returns from park.
    while ( !barrier.released ) {
      if ( closed && !worker.isAlive () ) return;
      LockSupport.parkNanos ( AWAIT_PARK_NANOS );
    }
  }

  /**
   * Marker callback - release the awaiter whose barrier position this marker occupies.
   *
   * Everything admitted before this marker has, by FIFO, already been processed, and each
   * such item's transit cascade drained within its own turn (§5.3). That is the §5.5
   * barrier exactly.
   */
  private void onAwaitMarker ( Object barrier ) {
    final AwaitBarrier b = (AwaitBarrier) barrier;
    b.released = true;
    // Unconditional: the awaiter may be spinning (in which case this is a wasted syscall-free
    // permit) or parked (in which case it is required). Testing which would be a race.
    LockSupport.unpark ( b.thread );
  }

  /**
   * Inject a marker job into the ingress queue.
   */
  private void marker ( Consumer < Object > callback ) {
    if ( closed ) return;
    submitIngress ( callback, null );
  }

  /**
   * Close marker callback - signals worker to exit.
   */
  private void onCloseMarker ( Object ignored ) {
    shouldExit = true;
  }

  @Queued
  @Idempotent
  @Override
  public void close () {
    if ( closed ) return;


    // Inject close marker FIRST (while still accepting emissions)
    // This ensures all emissions before close() are processed
    submitIngress ( closeMarkerReceiver, null );

    // NOW reject new emissions
    closed = true;

    // Always wake worker on close (may be parked)
    LockSupport.unpark ( worker );

    // Awaiters are not tracked: each polls `closed` on its own bounded park (see
    // awaitImpl), so there is nothing to release here and nothing to pay for on the
    // common path.

    // 2.8: stop scheduling further ticks; in-flight ticks already submitted
    // to the ingress queue may still be delivered (spec §11.5).
    ScheduledExecutorService s = scheduler;
    if ( s != null ) s.shutdownNow ();
  }

  @Idempotent
  @Override
  public void closeAwait () {
    // §16.1 #13 — fail-fast before any side effect when called from the circuit thread.
    checkExternalCaller ( "closeAwait" );
    close ();
    // await() routes to awaitClosed() once `closed` is set, which joins the worker
    // thread — that's exactly the "close job fully executed" guarantee.
    await ();
  }


  // ===================================================================================
  // Factory Methods - Conduit (2.0: conduit(Name, Class<E>) — no Composer/Configurer)
  // ===================================================================================

  @New
  @NotNull
  @Override
  public < E > Conduit < E > conduit () {
    requireOpen ( "conduit" );
    // 2.3: no-arg conduit uses the circuit's own subject name.
    return new FsConduit <> ( (FsSubject < ? >) subject, subject.name (), this );
  }

  @New
  @NotNull
  @Override
  public < E > Conduit < E > conduit ( @NotNull Name name, @NotNull Class < E > type ) {
    requireNonNull ( name );
    requireNonNull ( type );
    requireOpen ( "conduit" );
    return new FsConduit <> ( (FsSubject < ? >) subject, name, this );
  }

  @New
  @NotNull
  @Override
  public < E > Conduit < E > conduit ( @NotNull Name name, @NotNull Class < E > type,
                                       @NotNull Routing routing ) {
    requireNonNull ( name );
    requireNonNull ( type );
    requireNonNull ( routing );
    requireOpen ( "conduit" );
    return new FsConduit <> ( (FsSubject < ? >) subject, name, this, routing );
  }

  // ===================================================================================
  // Factory Methods - Sink (2.10, SPEC §1290-1343)
  // ===================================================================================

  /// 2.10: Returns a sink — the output-closed dual of Conduit — that wraps each
  /// emission as a `Capture` and forwards it to `endpoint`. Uses this circuit's
  /// subject name; for an explicit name see [#sink(Name, Pipe)].
  @New
  @NotNull
  @Override
  public < E > Sink < E > sink ( @NotNull Pipe < Capture < E > > endpoint ) {
    requireNonNull ( endpoint );
    requireOpen ( "sink" );
    // SPEC §1307 — endpoint must be a runtime-provided implementation.
    if ( ! ( endpoint instanceof FsPipe < ? > ) ) {
      throw new Fault ( subject, "sink", "endpoint pipe is not from this runtime provider" );
    }
    return new FsSink <> ( (FsSubject < ? >) subject, subject.name (), this, endpoint );
  }

  /// 2.10: Named sink variant.
  @New
  @NotNull
  @Override
  public < E > Sink < E > sink ( @NotNull Name name, @NotNull Pipe < Capture < E > > endpoint ) {
    requireNonNull ( name );
    requireNonNull ( endpoint );
    requireOpen ( "sink" );
    // SPEC §1332 — name and endpoint must both be from this runtime provider.
    if ( ! ( name instanceof FsName ) ) {
      throw new Fault ( subject, "sink", "name is not from this runtime provider" );
    }
    if ( ! ( endpoint instanceof FsPipe < ? > ) ) {
      throw new Fault ( subject, "sink", "endpoint pipe is not from this runtime provider" );
    }
    return new FsSink <> ( (FsSubject < ? >) subject, name, this, endpoint );
  }

  // ===================================================================================
  // Factory Methods - Ticker (2.8, SPEC §11.5)
  // ===================================================================================

  @New
  @NotNull
  @Override
  public Ticker ticker ( @NotNull Duration interval, @NotNull Pipe < ? super Long > target ) {
    requireNonNull ( interval );
    requireNonNull ( target );
    // §11.4 / §16.3: the two-argument form "uses a default name". Aligned with
    // the owning circuit's name, as `cell` / `port` / `pin` are required to be —
    // §16.3 leaves the ticker's default implementation-defined, so this is an
    // alignment choice, not a correction (see TCK-GAPS §D2).
    return ticker ( subject.name (), interval, target );
  }

  @New
  @NotNull
  @Override
  public Ticker ticker ( @NotNull Name name, @NotNull Duration interval,
                         @NotNull Pipe < ? super Long > target ) {
    requireNonNull ( name );
    requireNonNull ( interval );
    requireNonNull ( target );
    requireOpen ( "ticker" );
    if ( interval.isZero () || interval.isNegative () ) {
      throw new IllegalArgumentException ( "interval must be strictly positive" );
    }
    // SPEC §15.1 provider mismatch — target pipe must come from our provider.
    if ( ! ( target instanceof FsPipe < ? > ) ) {
      throw new Fault ( subject, "ticker", "target pipe is not from this runtime provider" );
    }
    return new FsTicker ( (FsSubject < ? >) subject, name, this, interval, target );
  }

  // ===================================================================================
  // Factory Methods - Bank (2.5)
  // ===================================================================================

  @New
  @NotNull
  @Override
  public < E > Bank < Conduit < E > > bank ( @NotNull Class < E > type, @NotNull Routing routing ) {
    requireNonNull ( type );
    requireNonNull ( routing );
    requireOpen ( "bank" );
    return new FsBank <> ( (FsSubject < ? >) subject, cortex ().name ( "bank" ), this, routing );
  }

  // ===================================================================================
  // Factory Methods - Pipe
  // ===================================================================================

  @New
  @NotNull
  @Override
  public < E > Pipe < E > pipe () {
    // 2.3: no-arg pipe — queues emissions and discards them on the circuit thread.
    // Equivalent to circuit.pipe(Receptor.NOOP) but without the wrapper.
    requireOpen ( "pipe" );
    return newPipe ( v -> { /* no-op */ } );
  }

  /// 3.0: target widened to `Pipe<? super E>` — a `Pipe<Number>` hop can carry
  /// `Long` emissions. Same-circuit targets are returned as-is (the cast is
  /// safe: a pipe that accepts `? super E` accepts every `E`).
  @New ( conditional = true )
  @NotNull
  @Override
  @SuppressWarnings ( "unchecked" )
  public < E > Pipe < E > pipe ( @NotNull Pipe < ? super E > target ) {
    requireNonNull ( target );
    // §9.1 / §16.1#9: the open-required check precedes the same-circuit fast
    // path — a closed circuit must not hand the target back as-is.
    requireOpen ( "pipe" );
    // Same-circuit pipe already routes through this circuit — return as-is
    if ( target instanceof FsPipe < ? > fsPipe && fsPipe.circuit () == this ) {
      return (Pipe < E >) target;
    }
    // Cross-circuit: wrap in a receptor pipe that calls target.emit()
    return newPipe ( target::emit );
  }

  /// 3.0: list fan-out. Targets are snapshotted at creation and dispatched in
  /// list order; duplicates receive the emission once per occurrence. Empty
  /// collapses to [#pipe()], single-element to [#pipe(Pipe)].
  @New
  @NotNull
  @Override
  public < E > Pipe < E > pipe ( @NotNull List < ? extends Pipe < ? super E > > targets ) {
    List < Pipe < ? super E > > snapshot = snapshotTargets ( targets );
    requireOpen ( "pipe" );
    if ( snapshot.isEmpty () ) return pipe ();
    if ( snapshot.size () == 1 ) return pipe ( snapshot.get ( 0 ) );
    return newPipe ( fanOut ( snapshot ) );
  }

  /// 3.0: named list fan-out. Always mints a named pipe — an empty list yields
  /// a named no-op pipe, a single-target list a named forwarder — because the
  /// supplied name must be honored on the returned pipe's subject.
  @New
  @NotNull
  @Override
  public < E > Pipe < E > pipe ( @NotNull Name name, @NotNull List < ? extends Pipe < ? super E > > targets ) {
    requireNonNull ( name );
    List < Pipe < ? super E > > snapshot = snapshotTargets ( targets );
    if ( ! ( name instanceof FsName ) ) {
      throw new Fault ( subject, "pipe", "name is not from this runtime provider" );
    }
    requireOpen ( "pipe" );
    Receptor < E > receptor =
      snapshot.isEmpty ()      ? v -> { /* named no-op */ }
      : snapshot.size () == 1  ? snapshot.get ( 0 )::emit
      :                          fanOut ( snapshot );
    return new FsPipe <> ( new ReceptorAdapter <> ( receptor ), this, name, (FsSubject < ? >) subject );
  }

  /// Rejects null lists / null elements synchronously (NPE via `List.copyOf`)
  /// and non-provider targets (Fault), returning the creation-time snapshot.
  private < E > List < Pipe < ? super E > > snapshotTargets ( List < ? extends Pipe < ? super E > > targets ) {
    requireNonNull ( targets );
    List < Pipe < ? super E > > snapshot = List.copyOf ( targets );
    for ( Pipe < ? super E > target : snapshot ) {
      if ( ! ( target instanceof FsPipe < ? > ) ) {
        throw new Fault ( subject, "pipe", "target pipe is not from this runtime provider" );
      }
    }
    return snapshot;
  }

  /// Fan-out receptor — runs on this circuit's worker. Same-circuit targets
  /// observe list order on this circuit (transit-queue FIFO); cross-circuit
  /// targets are enqueued in list order onto their own circuits' ingress.
  private static < E > Receptor < E > fanOut ( List < Pipe < ? super E > > targets ) {
    return value -> {
      for ( Pipe < ? super E > target : targets ) {
        target.emit ( value );
      }
    };
  }

  @New
  @NotNull
  @Override
  public < E > Pipe < E > pipe ( @NotNull Receptor < ? super E > receptor ) {
    requireNonNull ( receptor );
    requireOpen ( "pipe" );
    return newPipe ( receptor );
  }

  /// 2.7: named receptor pipe. Pipe subject reflects the caller-supplied name.
  @New
  @NotNull
  @Override
  public < E > Pipe < E > pipe ( @NotNull Name name, @NotNull Receptor < ? super E > receptor ) {
    requireNonNull ( name );
    requireNonNull ( receptor );
    requireOpen ( "pipe" );
    return new FsPipe <> ( new ReceptorAdapter <> ( receptor ), this, name, (FsSubject < ? >) subject );
  }

  /// 2.7: cell factories. Both create a circuit-owned cell — empty or seeded.
  ///
  /// SPEC §11.2 / §16.3: "when no name is supplied, the implementation uses the
  /// owning circuit's name", and that default name "need not be unique;
  /// uniqueness is provided by the subject identifier (§4.2)". `null` is the
  /// same delegation idiom the sibling `pin(initial)` / `port(initial)`
  /// factories already use — FsSubject resolves an omitted name from the
  /// parent, and Id is minted per subject, so sibling cells stay distinct.
  /// Throws Fault if the circuit is closed (spec §11.3).
  @New
  @NotNull
  @Override
  public < E > Cell < E > cell ( @NotNull E initial ) {
    requireNonNull ( initial );
    requireOpen ( "cell" );
    return new FsCell <> ( (FsSubject < ? >) subject, null, this, initial );
  }

  /// 3.0: named cell — same semantics as [#cell(Object)] with the supplied
  /// name bound to the cell's subject. The name must come from this runtime.
  @New
  @NotNull
  @Override
  public < E > Cell < E > cell ( @NotNull Name name, @NotNull E initial ) {
    requireNonNull ( name );
    requireNonNull ( initial );
    if ( ! ( name instanceof FsName ) ) {
      throw new Fault ( subject, "cell", "name is not from this runtime provider" );
    }
    requireOpen ( "cell" );
    return new FsCell <> ( (FsSubject < ? >) subject, name, this, initial );
  }

  // ===================================================================================
  // Factory Methods - Pin (2.9 SPEC §11.7)
  // ===================================================================================

  /// SPEC §11.7: "when no name is supplied, the implementation uses the
  /// owning circuit's name." FsSubject treats `name == null` as a delegation
  /// to the parent's name(), and Id is freshly generated per subject — so
  /// distinct pin handles still have distinct subject identities even when
  /// they share a name with the owning circuit.
  @New
  @NotNull
  @Override
  public < E > Pin < E > pin ( @NotNull E initial ) {
    requireNonNull ( initial );
    requireOpen ( "pin" );
    return new FsPin <> ( (FsSubject < ? >) subject, null, this, initial );
  }

  @New
  @NotNull
  @Override
  public < E > Pin < E > pin ( @NotNull Name name, @NotNull E initial ) {
    requireNonNull ( name );
    requireNonNull ( initial );
    requireOpen ( "pin" );
    return new FsPin <> ( (FsSubject < ? >) subject, name, this, initial );
  }

  // ===================================================================================
  // Factory Methods - Port (2.9 SPEC §11.6)
  // ===================================================================================

  /// SPEC §11.6: same name-inheritance contract as Pin.
  @New
  @NotNull
  @Override
  public < E > Port < E > port ( @NotNull E initial ) {
    requireNonNull ( initial );
    requireOpen ( "port" );
    return new FsPort <> ( (FsSubject < ? >) subject, null, this, initial );
  }

  @New
  @NotNull
  @Override
  public < E > Port < E > port ( @NotNull Name name, @NotNull E initial ) {
    requireNonNull ( name );
    requireNonNull ( initial );
    requireOpen ( "port" );
    return new FsPort <> ( (FsSubject < ? >) subject, name, this, initial );
  }

  // ===================================================================================
  // Factory Methods - Basin (3.0, SPEC §11: circuit-owned bounded buffer)
  // ===================================================================================

  /// 3.0: circuit-owned bounded buffer — the multi-value sibling of Cell.
  /// The element type is recovered by target typing; see the class-token
  /// default `basin(Class, int)` inherited from the API for inference help.
  @New
  @NotNull
  @Override
  public < E > Basin < E > basin ( int capacity ) {
    if ( capacity < 1 ) {
      throw new IllegalArgumentException ( "basin capacity must be >= 1: " + capacity );
    }
    requireOpen ( "basin" );
    return new FsBasin <> ( (FsSubject < ? >) subject, this, capacity );
  }

  // ===================================================================================
  // Factory Methods - Subscriber (2.0: Subject<Pipe<E>> instead of Subject<Channel<E>>)
  // ===================================================================================

  @New
  @NotNull
  @Override
  public < E > Subscriber < E > subscriber (
    @NotNull Name name, @NotNull @Queued BiConsumer < ? super Subject < Pipe < E > >, ? super Registrar < E > > callback ) {
    requireNonNull ( name );
    requireNonNull ( callback );
    requireOpen ( "subscriber" );
    return new FsSubscriber <> (
      new FsSubject <> ( name, (FsSubject < ? >) subject, Subscriber.class ), callback );
  }

}
