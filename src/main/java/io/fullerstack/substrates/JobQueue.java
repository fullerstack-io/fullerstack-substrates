package io.fullerstack.substrates;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.function.Consumer;

/// Intrusive-linked MPSC ingress queue — the alternative to [IngressQueue]'s
/// chunked array.
///
/// Both are intrusive linked structures; they differ only in **granularity**.
/// [QChunk] is a node holding 128 entries in an interleaved `Object[]`; a [Job]
/// here is a node holding exactly one. That single difference decides three
/// costs:
///
/// - **Type check.** Batching heterogeneous work into one node forces an
///   untyped `Object[]`, so reading a receiver back is a cast to an
///   *interface* — a secondary supertype check, which perfasm measured as the
///   hottest instruction in the drain profile (~11%: `popcntq`, a hashed
///   probe of the secondary-super table, and a stub call on miss). A `Job`
///   field is typed and is a *class*, so there is no check and dispatch is
///   `invokevirtual` rather than `invokeinterface`.
/// - **Marker branch.** The chunked drain asks `isMarker(r)` per item. Here a
///   marker is simply another [Job] subclass and `run` dispatches to it.
/// - **Allocation.** One node per 128 emissions becomes one per emission:
///   ~10 B/op against ~32 B/op. Under backpressure that is also 128x more
///   objects that can survive a young collection and be promoted.
///
/// Neither design recycles. Pooling was tried at chunk granularity and is the
/// ABA that lost emissions and parked `await` — see [QChunk] — and pooling jobs
/// has the same hazard with 128x more chances to hit it.
///
/// Producers are wait-free: one `getAndSet` of the head, then a release store
/// linking the previous node. FIFO across producers. The consumer walks from a
/// stub, so `tail` always points at an already-consumed node and the node after
/// it is the next to run; a null `next` means either empty or a producer caught
/// between its `getAndSet` and its link, which reads the same to the consumer
/// as "nothing committed yet" — the role the receiver slot plays in the
/// chunked queue.
/// Leading isolation for the consumer's cursor — see IngressPad0 for why the
/// padding is a hierarchy of `int` fields rather than `@Contended`.
///
/// Without this, `head` and `tail` sat 4 bytes apart in one cache line: the
/// producer `getAndSet`s `head` on every emission and the consumer writes
/// `tail` on every drain, so the two ends of the queue fought over one line.
/// That is the same defect that cost IngressQueue 34%.
abstract class JobQueuePad0 {
  int p00, p01, p02, p03, p04, p05, p06, p07;
  int p08, p09, p10, p11, p12, p13, p14, p15;
  int p16, p17, p18, p19, p20, p21, p22, p23;
  int p24, p25, p26, p27, p28, p29, p30, p31;
}

/// Consumer end — walked and rewritten by the circuit worker only.
abstract class JobQueueConsumer extends JobQueuePad0 {
  Job tail;
}

abstract class JobQueuePad1 extends JobQueueConsumer {
  int q00, q01, q02, q03, q04, q05, q06, q07;
  int q08, q09, q10, q11, q12, q13, q14, q15;
  int q16, q17, q18, q19, q20, q21, q22, q23;
  int q24, q25, q26, q27, q28, q29, q30, q31;
}

/// Producer end — exchanged by every emitting thread.
abstract class JobQueueProducer extends JobQueuePad1 {
  @SuppressWarnings ( "unused" )   // via HEAD
  volatile Job head;
}

abstract class JobQueuePad2 extends JobQueueProducer {
  int r00, r01, r02, r03, r04, r05, r06, r07;
  int r08, r09, r10, r11, r12, r13, r14, r15;
  int r16, r17, r18, r19, r20, r21, r22, r23;
  int r24, r25, r26, r27, r28, r29, r30, r31;
}

final class JobQueue extends JobQueuePad2 {

  /// Consumed-node placeholder, so the consumer never needs a null tail.
  private static final class StubJob extends Job {
    @Override void run ( FsCircuit circuit ) { }
  }

  /// User work: the §15.4 isolation boundary, then the transit drain.
  private static final class ReceptorJob extends Job {

    private final Consumer < Object > receiver;
    private final Object              value;

    ReceptorJob ( Consumer < Object > receiver, Object value ) {
      this.receiver = receiver;
      this.value    = value;
    }

    @Override
    void run ( FsCircuit circuit ) {
      try {
        receiver.accept ( value );
      } catch ( Throwable ignored ) {
        // §15.4 #4: observability is implementation-defined.
      }
      if ( circuit.transitHasWork () ) circuit.drainTransit ();
    }
  }

  /// Await/close markers: no isolation wrapper, no transit drain — the chunked
  /// queue reaches this by branching on `isMarker`; here it is the vtable.
  private static final class MarkerJob extends Job {

    private final Consumer < Object > marker;
    private final Object              value;

    MarkerJob ( Consumer < Object > marker, Object value ) {
      this.marker = marker;
      this.value  = value;
    }

    @Override
    void run ( FsCircuit circuit ) {
      marker.accept ( value );
    }
  }

  private static final VarHandle HEAD;
  private static final VarHandle NEXT;

  static {
    try {
      MethodHandles.Lookup l = MethodHandles.lookup ();
      HEAD = l.findVarHandle ( JobQueueProducer.class, "head", Job.class );
      NEXT = l.findVarHandle ( Job.class, "next", Job.class );
    } catch ( ReflectiveOperationException e ) {
      throw new ExceptionInInitializerError ( e );
    }
  }

  JobQueue () {
    final Job stub = new StubJob ();
    head = stub;
    tail = stub;
  }

  /// Wait-free: one atomic exchange, then the link that commits it.
  void enqueue ( Consumer < Object > receiver, Object value, boolean marker ) {
    final Job job = marker
      ? new MarkerJob ( receiver, value )
      : new ReceptorJob ( receiver, value );
    final Job prev = (Job) HEAD.getAndSet ( this, job );
    NEXT.setRelease ( prev, job );
  }

  /// Non-null when a committed job is waiting. Used by the worker's spin.
  Object peek () {
    return NEXT.getAcquire ( tail );
  }

  /// Runs every committed job, interleaving transit exactly as the chunked
  /// drain does. Returns true if any job ran.
  boolean drainBatch ( FsCircuit circuit ) {

    Job t = tail;
    Job j = (Job) NEXT.getAcquire ( t );
    if ( j == null ) return false;

    do {
      t.next = null;                       // unlink the consumed node for GC
      t = j;

      // §5.8: one clock reading per ingress chain — invalidated before dispatch
      // and deliberately not inside the transit drain.
      circuit.stimulus.valid = false;

      j.run ( circuit );

      j = (Job) NEXT.getAcquire ( t );
    } while ( j != null );

    tail = t;
    return true;
  }
}
