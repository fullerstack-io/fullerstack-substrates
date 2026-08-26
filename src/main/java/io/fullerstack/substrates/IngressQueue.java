package io.fullerstack.substrates;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.function.Consumer;

/// Leading isolation for the consumer's cursor.
///
/// Padding is expressed as a class hierarchy of `int` fields rather than
/// `@Contended`, for two reasons measured on this codebase.
///
/// HotSpot honours `jdk.internal.vm.annotation.Contended` only for classes
/// loaded by the boot loader, unless the JVM is started with
/// `-XX:-RestrictContended`. A library cannot require that flag of its
/// consumer, and without it the annotation is inert: emission measured
/// 20.5 ns unpadded against 13.5 ns padded.
///
/// The fields are `int`, not `long`, because `long` needs 8-byte alignment.
/// After the 12-byte object header that leaves a 4-byte hole, and HotSpot's
/// field layout hoists a 4-byte subclass field into any hole it finds — which
/// silently relocated the very fields this padding exists to separate. `int`
/// fields are 4-byte aligned, so under compressed oops (refs are 4 bytes) the
/// hierarchy packs with no holes and the layout matches the declaration.
/// Verify with `objectFieldOffset` after changing any field here.
abstract class IngressPad0 {
  int p00, p01, p02, p03, p04, p05, p06, p07;
  int p08, p09, p10, p11, p12, p13, p14, p15;
  int p16, p17, p18, p19, p20, p21, p22, p23;
  int p24, p25, p26, p27, p28, p29, p30, p31;
}

/// Consumer cursor — single-threaded, written on every drained item.
abstract class IngressConsumer extends IngressPad0 {
  QChunk headChunk;
  int    headIndex;
}

/// Separates the consumer cursor from the producer's tail.
abstract class IngressPad1 extends IngressConsumer {
  int q00, q01, q02, q03, q04, q05, q06, q07;
  int q08, q09, q10, q11, q12, q13, q14, q15;
  int q16, q17, q18, q19, q20, q21, q22, q23;
  int q24, q25, q26, q27, q28, q29, q30, q31;
}

/// Producer tail — read on every enqueue, written only on chunk rollover.
abstract class IngressProducer extends IngressPad1 {

  /// accessed via TAIL VarHandle
  @SuppressWarnings ( "unused" )
  volatile QChunk tail;
}

/// Separates the producer's tail from the free list.
abstract class IngressPad2 extends IngressProducer {
  int r00, r01, r02, r03, r04, r05, r06, r07;
  int r08, r09, r10, r11, r12, r13, r14, r15;
  int r16, r17, r18, r19, r20, r21, r22, r23;
  int r24, r25, r26, r27, r28, r29, r30, r31;
}

/**
 * Wait-free MPSC queue using unrolled linked list of {@link QChunk}s.
 *
 * <p>Each chunk holds 64 receiver+value pairs in a flat array. Producers
 * claim slots via atomic {@code getAndAdd} (wait-free). When a chunk fills,
 * a new one is linked. Exhausted chunks are recycled via a Treiber stack
 * free list — zero allocation in steady state.
 *
 * <p>Consumer drains slots sequentially (cache-friendly) and interleaves
 * transit drain after each emission.
 *
 * <p>Consumer cursor, producer tail and free list each sit on their own cache
 * line; see {@link IngressPad0} for why the padding is structural rather than
 * annotated.
 */
public final class IngressQueue extends IngressPad2 {

  private static final VarHandle TAIL;

  static {
    try {
      MethodHandles.Lookup l = MethodHandles.lookup ();
      TAIL = l.findVarHandle ( IngressQueue.class, "tail", QChunk.class );
    } catch ( ReflectiveOperationException e ) {
      throw new ExceptionInInitializerError ( e );
    }
  }



  public IngressQueue () {
    QChunk initial = new QChunk ();
    headChunk = initial;
    headIndex = 0;
    tail = initial;
  }

  /**
   * Enqueue emission. Wait-free: getAndAdd always succeeds.
   * Receiver (never null) acts as commit signal via setRelease.
   */
  public void enqueue ( Consumer < Object > receiver, Object value ) {
    QChunk chunk = (QChunk) TAIL.getOpaque ( this );               // opaque read
    int slot = (int) QChunk.CLAIMED.getAndAdd ( chunk, 1 );      // wait-free claim
    if ( slot < QChunk.CAPACITY ) {
      int base = slot << 1;
      chunk.slots[base + 1] = value;                              // plain store (value first)
      QChunk.SLOTS.setRelease ( chunk.slots, base, receiver );    // release store (commit)
    } else {
      enqueueSlow ( receiver, value, chunk );                     // cold path (1/64 emits)
    }
  }

  /**
   * Cold path: current chunk full, allocate or recycle a new one.
   * At most 1 recursive retry.
   */
  private void enqueueSlow ( Consumer < Object > receiver, Object value, QChunk full ) {

    for ( ; ; ) {

      // Link a successor to the full chunk, unless a peer producer already did.
      QChunk next = (QChunk) QChunk.NEXT.getAcquire ( full );
      if ( next == null ) {
        final QChunk fresh = new QChunk ();
        if ( QChunk.NEXT.compareAndSet ( full, null, fresh ) ) {
          next = fresh;
        } else {
          // Lost the link race; `fresh` was never published, so drop it.
          next = (QChunk) QChunk.NEXT.getAcquire ( full );
        }
      }
      TAIL.compareAndSet ( this, full, next );

      // Retry the claim against whatever is tail now — a peer may have advanced
      // it further than `next` while this producer was linking.
      final QChunk chunk = (QChunk) TAIL.getOpaque ( this );
      final int    slot  = (int) QChunk.CLAIMED.getAndAdd ( chunk, 1 );

      if ( slot < QChunk.CAPACITY ) {
        final int base = slot << 1;
        chunk.slots[base + 1] = value;
        QChunk.SLOTS.setRelease ( chunk.slots, base, receiver );
        return;
      }

      // That chunk filled too. Advance from it rather than nesting a frame.
      full = chunk;
    }
  }

  /**
   * Drain all committed slots with depth-first cascade interleaving.
   * Returns true if any slots were drained.
   */
  boolean drainBatch ( FsCircuit circuit ) {
    int idx = headIndex;
    QChunk chunk = headChunk;
    if ( idx >= QChunk.CAPACITY ) {
      QChunk next = (QChunk) QChunk.NEXT.getAcquire ( chunk );
      if ( next == null ) return false;
      chunk = next;                       // exhausted chunk becomes garbage
      headChunk = next;
      idx = 0;
      headIndex = 0;
    }
    if ( QChunk.SLOTS.getAcquire ( chunk.slots, idx << 1 ) == null ) return false;
    drainBatchLoop ( chunk, idx, circuit );
    return true;
  }

  /**
   * Drain loop: process all committed slots across all chunks.
   * No batch limit — drains the entire chain until a null slot or
   * no next chunk. Interleaves transit after each emission.
   * Head updated ONCE at end (not per iteration).
   */
  @SuppressWarnings ( "unchecked" )
  private void drainBatchLoop ( QChunk chunk, int idx, FsCircuit circuit ) {
    Object[] slots = chunk.slots;
    for ( ; ; ) {
      int base = idx << 1;
      Consumer < Object > r = (Consumer < Object >) QChunk.SLOTS.getAcquire ( slots, base );
      if ( r == null ) break;
      Object v = slots[base + 1];
      slots[base] = null;                                    // clear for GC (plain write)
      slots[base + 1] = null;                                // clear for GC (plain write)

      // §5.8: time advances on each ingress item and NOT across the transit hops of one
      // cascade, so this is the one invalidation point — before dispatch, and deliberately
      // not inside `drainTransit()` below. A plain store; the reading itself is established
      // lazily, and only if some time-aware operator actually observes it.
      circuit.stimulus.valid = false;

      if ( circuit.isMarker ( r ) ) {
        circuit.fireMarker ( r, v );                       // cold: separate type profile
      } else {
        // §15.4 isolation: user-supplied callbacks may throw. Catch here so a
        // failing receptor cannot terminate the worker loop. Engine paths
        // (FsChannel, CircuitJob, ReceptorAdapter) all eventually call user
        // code — this is the trust boundary at the dispatcher.
        try {
          r.accept ( v );                                  // hot: monomorphic receptor
        } catch ( Throwable ignored ) {
          // §15.4 #4: observability is implementation-defined. Silently drop
          // for now — recovery strategies belong to extension layers (§15.4).
        }
        if ( circuit.transitHasWork () ) {                 // guard: plain field read
          circuit.drainTransit ();                         // drain all transit (drain() loops internally)
        }
      }

      idx++;
      if ( idx >= QChunk.CAPACITY ) {
        QChunk next = (QChunk) QChunk.NEXT.getAcquire ( chunk );
        if ( next == null ) break;
        chunk = next;                     // exhausted chunk becomes garbage
        slots = chunk.slots;
        idx = 0;
      }
    }
    headChunk = chunk;
    headIndex = idx;
  }

  /**
   * Peek for ingress work. Returns non-null if a committed slot exists.
   */
  /// Peek at the next ingress slot. Returns non-null if work is available.
  /// Kept whole at 40 bytes, deliberately over the 35-byte `MaxInlineSize`.
  ///
  /// Extracting the rollover branch brought this to 34 bytes and did make it
  /// inline — and cost `SinkOps.sink_emit_batch` 3.6 ns across two reps
  /// (29.3 -> 32.9, against a 1.2 ns within-arm spread). Sink drives chunk
  /// turnover hardest, so the branch this method "rarely" takes is not rare
  /// there. The inline failures it causes are on cold paths, and the spin loop
  /// that calls it on the hot path was measured not to be a cost at all.
  public Object peek () {
    int idx = headIndex;
    if ( idx < QChunk.CAPACITY ) {
      return QChunk.SLOTS.getAcquire ( headChunk.slots, idx << 1 );
    }
    return QChunk.NEXT.getAcquire ( headChunk );
  }

}
