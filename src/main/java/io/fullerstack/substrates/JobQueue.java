package io.fullerstack.substrates;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.function.Consumer;

/// Intrusive-linked MPSC ingress queue: one [Job] per emission, carrying its
/// own `next`.
///
/// A batched alternative — one node holding 128 entries in an interleaved
/// `Object[]` — was built and rejected. It allocated a third as much but had to
/// read receivers back out of an untyped array, making every dispatch an
/// interface cast; perfasm found that secondary supertype check to be the
/// hottest instruction in the drain. A typed `Job` field is a *class*, so there
/// is no check and dispatch is `invokevirtual`. See `docs/DECISIONS.md`.
///
/// **Nothing here is recycled.** A carrier that crosses the thread boundary
/// cannot be: a producer can claim a node the consumer has already freed, which
/// is an ABA that lost emissions and left `await` parked forever. Worker-confined
/// carriers can be recycled and are — see `TransitQueueRing`.
///
/// Producers are wait-free: one `getAndSet` of the head, then a release store
/// linking the previous node. FIFO across producers. The consumer walks from a
/// stub, so `tail` always points at an already-consumed node and the node after
/// it is the next to run; a null `next` means either empty or a producer caught
/// between its `getAndSet` and its link, which reads the same to the consumer
/// as "nothing committed yet" — the role the receiver slot plays in the
/// chunked queue.
/// Leading isolation for the consumer's cursor.
///
/// Without this, `head` and `tail` sit 4 bytes apart in one cache line: the
/// producer `getAndSet`s `head` on every emission and the consumer writes `tail`
/// on every drain, so the two ends of the queue fight over one line. Measured at
/// 34% of emission cost when it was allowed to happen.
///
/// **Why a class hierarchy of `int` fields, and not `@Contended`.** Both halves
/// of that were measured on this codebase.
///
/// HotSpot honours `jdk.internal.vm.annotation.Contended` only for classes loaded
/// by the boot loader, unless the JVM is started with `-XX:-RestrictContended`. A
/// library cannot require that flag of its consumer, and without it the annotation
/// is inert: emission measured 20.5 ns unpadded against 13.5 ns padded.
///
/// The fields are `int`, not `long`, because `long` needs 8-byte alignment. After
/// the 12-byte object header that leaves a 4-byte hole, and HotSpot's field layout
/// hoists a 4-byte subclass field into any hole it finds — silently relocating the
/// very fields this padding exists to separate. `int` fields are 4-byte aligned, so
/// under compressed oops the hierarchy packs with no holes and the layout matches
/// the declaration. Verify with `objectFieldOffset` after changing any field here.
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
    // The consumer walks from an already-consumed node, so it needs one to start from. This
    // placeholder is never run: the drain advances to `next` before dispatching.
    final Job stub = new Job ();
    head = stub;
    tail = stub;
  }

  /// Wait-free: one atomic exchange, then the link that commits it.
  ///
  /// The fields are written before the exchange, which is a full fence, and the consumer reaches
  /// the node only through an acquire load of `next` — so the release below carries them across.
  void enqueue ( Consumer < Object > receiver, Object value, boolean marker ) {
    final Job job = new Job ();
    job.receiver = receiver;
    job.value    = value;
    job.marker   = marker;
    final Job prev = (Job) HEAD.getAndSet ( this, job );
    NEXT.setRelease ( prev, job );
  }

  /// Non-null when a committed job is waiting. Used by the worker's spin.
  Object peek () {
    return NEXT.getAcquire ( tail );
  }

  /// True when nothing is committed **and** no producer holds a slot it has not linked yet.
  ///
  /// [#peek] alone cannot carry the park decision. A producer publishes in two steps — exchange
  /// the head, then link the previous node — and only the second is visible to `peek`. Between
  /// them the queue looks empty, and the producer's own check of the worker's `parked` flag can
  /// be reordered ahead of its link on any store-buffered machine, so both sides can conclude
  /// "nothing to do here" about the same emission: the worker parks and the producer does not
  /// wake it.
  ///
  /// The head closes that. Its write is the exchange — a locked read-modify-write, globally
  /// ordered — so if a producer's flag read preceded the worker's flag write, that producer's
  /// exchange precedes it too, and the worker sees `head != tail` and does not park. Read only
  /// when the worker is about to park, never on the emission path.
  boolean quiescent () {
    return NEXT.getAcquire ( tail ) == null && head == tail;
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

      run ( j, circuit );

      j = (Job) NEXT.getAcquire ( t );
    } while ( j != null );

    tail = t;
    return true;
  }

  /// Runs one admitted job.
  ///
  /// A marker skips both the §15.4 isolation wrapper and the transit drain. This was a virtual
  /// `Job.run` dispatched through a subclass per kind; markers are 0.009% of admissions, and the
  /// call was measured failing to inline into the drain, so a predicted branch is the cheaper
  /// shape for something that almost never happens.
  private static void run ( Job job, FsCircuit circuit ) {

    if ( job.marker ) {
      job.receiver.accept ( job.value );
      return;
    }

    try {
      job.receiver.accept ( job.value );
    } catch ( Throwable ignored ) {
      // §15.4 #4: observability is implementation-defined.
    }

    if ( circuit.transitHasWork () ) circuit.drainTransit ();
  }
}
