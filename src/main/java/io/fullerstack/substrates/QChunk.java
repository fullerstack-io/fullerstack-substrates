package io.fullerstack.substrates;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/// Leading isolation for [QChunk#claimed].
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
abstract class QChunkPad0 {
  int p00, p01, p02, p03, p04, p05, p06, p07;
  int p08, p09, p10, p11, p12, p13, p14, p15;
  int p16, p17, p18, p19, p20, p21, p22, p23;
  int p24, p25, p26, p27, p28, p29, p30, p31;
}

/// The producer's atomic slot counter, isolated on its own cache line.
abstract class QChunkClaim extends QChunkPad0 {

  /// ingress: atomic getAndAdd
  volatile int claimed;
}

/// Trailing isolation for [QChunk#claimed].
abstract class QChunkPad1 extends QChunkClaim {
  int q00, q01, q02, q03, q04, q05, q06, q07;
  int q08, q09, q10, q11, q12, q13, q14, q15;
  int q16, q17, q18, q19, q20, q21, q22, q23;
  int q24, q25, q26, q27, q28, q29, q30, q31;
}

/**
 * Unified chunk for both ingress (MPSC) and transit (single-threaded) queues.
 *
 * <p>Each chunk holds up to {@link #CAPACITY} receiver+value pairs in an
 * interleaved {@code Object[]} array: {@code [r0, v0, r1, v1, ...]}.
 *
 * <p>Ingress: producers claim slots via atomic {@code getAndAdd} on
 * {@code claimed}. Consumer reads committed slots via {@code getAcquire} on the
 * receiver position.
 *
 * <p>Chunks are never reused. An exhausted chunk is dropped and collected, and
 * {@code claimed} is never reset. That is load-bearing for correctness: a
 * producer can be preempted between reading the tail and claiming its slot, and
 * a recycled chunk would let it claim a slot in a chunk no longer in the chain —
 * an ABA, losing the emission (and hanging {@code await} when the lost admission
 * is a barrier marker). Because the counter only ever grows, such a producer
 * instead sees {@code slot >= CAPACITY}, takes the slow path, and walks
 * {@code next} to the live tail.
 *
 * <p>Transit: single pre-allocated chunk reused every drain cycle. Plain writes
 * (no atomics). Overflow chunks linked via {@link #next} for cascade depth &gt; 64.
 *
 * <p>The producer's {@code claimed} counter is separated from the consumer's
 * {@code slots} and {@code next} reads by {@link QChunkPad0} / {@link QChunkPad1};
 * see {@link QChunkPad0} for why the padding is structural rather than annotated.
 */
final class QChunk extends QChunkPad1 {

  static final int CAPACITY  = 128;
  static final int ARRAY_LEN = CAPACITY << 1;  // 256

  final    Object[] slots = new Object[ARRAY_LEN];   // interleaved [r0,v0,r1,v1,...]
  volatile QChunk   next;                            // link to next chunk (consumer reads)

  static final VarHandle SLOTS;
  static final VarHandle CLAIMED;
  static final VarHandle NEXT;

  static {
    try {
      MethodHandles.Lookup l = MethodHandles.lookup ();
      SLOTS = MethodHandles.arrayElementVarHandle ( Object[].class );
      CLAIMED = l.findVarHandle ( QChunk.class, "claimed", int.class );
      NEXT = l.findVarHandle ( QChunk.class, "next", QChunk.class );
    } catch ( ReflectiveOperationException e ) {
      throw new ExceptionInInitializerError ( e );
    }
  }
}
