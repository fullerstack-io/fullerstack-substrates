package io.fullerstack.substrates;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.function.Consumer;

/// Leading isolation — see IngressPad0 for why padding is a hierarchy of ints.
abstract class StealPad0 {
  int p00, p01, p02, p03, p04, p05, p06, p07;
  int p08, p09, p10, p11, p12, p13, p14, p15;
  int p16, p17, p18, p19, p20, p21, p22, p23;
  int p24, p25, p26, p27, p28, p29, p30, p31;
}

/// Producer end. Every emitting thread CASes here; the consumer takes the whole
/// chain in one exchange.
abstract class StealProducer extends StealPad0 {
  @SuppressWarnings ( "unused" )   // via HEAD
  volatile Job head;
}

abstract class StealPad1 extends StealProducer {
  int q00, q01, q02, q03, q04, q05, q06, q07;
  int q08, q09, q10, q11, q12, q13, q14, q15;
  int q16, q17, q18, q19, q20, q21, q22, q23;
  int q24, q25, q26, q27, q28, q29, q30, q31;
}

/// Intrusive MPSC where the consumer **steals the whole queue**, rather than
/// chasing the producer node by node.
///
/// [JobQueue] is Vyukov: publish-then-link, which is wait-free for producers and
/// FIFO, but leaves a window where a published node is not yet linked. The
/// consumer therefore cannot take the chain wholesale — it must re-read `next`
/// with acquire semantics per item, which parks it directly behind the producer
/// on the same word.
///
/// This is Treiber: **link-then-publish**. `job.next` is written before the CAS
/// that makes the node visible, and the CAS's release ordering publishes both
/// together, so a reachable node is always fully linked. The consumer can
/// `getAndSet` the head to null and own the entire chain outright — after which
/// it walks with **plain reads**, sharing nothing with producers. `next` need
/// not even be volatile on this path.
///
/// The cost is order. Pushing to the head yields LIFO, so the stolen chain is
/// reversed into arrival order before dispatch — one pointer walk over nodes the
/// drain is about to touch anyway. Producers also CAS rather than exchange, so
/// they retry under contention instead of being wait-free.
final class JobQueueSteal extends StealPad1 {

  private static final VarHandle HEAD;

  static {
    try {
      HEAD = MethodHandles.lookup ().findVarHandle ( StealProducer.class, "head", Job.class );
    } catch ( ReflectiveOperationException e ) {
      throw new ExceptionInInitializerError ( e );
    }
  }

  private static final class ReceptorJob extends Job {
    private final Consumer < Object > receiver;
    private final Object              value;
    ReceptorJob ( Consumer < Object > receiver, Object value ) {
      this.receiver = receiver; this.value = value;
    }
    @Override void run ( FsCircuit circuit ) {
      try {
        receiver.accept ( value );
      } catch ( Throwable ignored ) {
        // §15.4 #4
      }
      if ( circuit.transitHasWork () ) circuit.drainTransit ();
    }
  }

  private static final class MarkerJob extends Job {
    private final Consumer < Object > marker;
    private final Object              value;
    MarkerJob ( Consumer < Object > marker, Object value ) {
      this.marker = marker; this.value = value;
    }
    @Override void run ( FsCircuit circuit ) { marker.accept ( value ); }
  }

  /// Link, then publish. The CAS carries release ordering, so a node reachable
  /// from `head` always has its `next` already written.
  void enqueue ( Consumer < Object > receiver, Object value, boolean marker ) {
    final Job job = marker ? new MarkerJob ( receiver, value ) : new ReceptorJob ( receiver, value );
    for ( ; ; ) {
      final Job old = (Job) HEAD.getAcquire ( this );
      job.next = old;
      if ( HEAD.compareAndSet ( this, old, job ) ) return;
    }
  }

  Object peek () {
    return HEAD.getAcquire ( this );
  }

  boolean drainBatch ( FsCircuit circuit ) {

    final Job stolen = (Job) HEAD.getAndSet ( this, null );
    if ( stolen == null ) return false;

    // LIFO -> arrival order. The chain is privately owned from here, so every
    // read below is plain.
    Job first = null;
    for ( Job j = stolen; j != null; ) {
      final Job n = j.next;
      j.next = first;
      first  = j;
      j      = n;
    }

    for ( Job j = first; j != null; ) {
      final Job n = j.next;
      j.next = null;
      circuit.stimulus.valid = false;
      j.run ( circuit );
      j = n;
    }
    return true;
  }
}
