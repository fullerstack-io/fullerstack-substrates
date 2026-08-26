package io.fullerstack.substrates;

import java.util.function.Consumer;

/**
 * Single-threaded transit queue using a pre-allocated {@link QChunk} buffer.
 *
 * <p>Zero allocation in steady state: one chunk allocated at construction,
 * reused every drain cycle. No CAS and no free list.
 *
 * <p>Not entirely free of memory ordering, though: {@link QChunk#next} is
 * {@code volatile} because the MPSC ingress queue requires it, so linking and
 * following an overflow chunk costs a volatile store and a volatile load that
 * this single-threaded queue does not need. That is a cost of sharing
 * {@code QChunk} with the ingress queue, and is worth accounting for when
 * comparing this against {@link TransitQueueRing}.
 *
 * <p>Overflow (cascade deeper than {@link QChunk#CAPACITY}) links a new chunk
 * via {@code next}.
 * After drain, overflow chunks become garbage (rare, GC handles them).
 */
final class TransitQueue {

  private final QChunk homeChunk = new QChunk ();  // pre-allocated, reused forever
  private       QChunk writeChunk;                  // current write target
  private       int    writeIndex;

  TransitQueue () {
    writeChunk = homeChunk;
  }

  void enqueue ( Consumer < Object > receiver, Object value ) {
    int idx = writeIndex;
    if ( idx < QChunk.CAPACITY ) {
      int base = idx << 1;
      writeChunk.slots[base] = receiver;       // plain write (single-threaded)
      writeChunk.slots[base + 1] = value;
      writeIndex = idx + 1;
    } else {
      enqueueSlow ( receiver, value );         // overflow: link new chunk
    }
  }

  private void enqueueSlow ( Consumer < Object > receiver, Object value ) {
    QChunk overflow = new QChunk ();            // rare allocation (cascade > CAPACITY)
    writeChunk.next = overflow;                 // NB volatile store: QChunk.next is
                                                // volatile for the MPSC ingress queue
    writeChunk = overflow;
    overflow.slots[0] = receiver;
    overflow.slots[1] = value;
    writeIndex = 1;
  }

  /**
   * Returns true if there is queued transit work. Plain field read
   * (single-threaded — only called from circuit worker).
   */
  boolean hasWork () {
    return writeIndex != 0;
  }

  boolean drain () {
    if ( writeIndex == 0 ) return false;
    drainSlots ();
    return true;
  }

  /**
   * Drain with read cursor chasing write cursor. When an {@code accept()}
   * call cascades (enqueues more transit work), {@code writeIndex} grows
   * and the read cursor naturally follows — all cascade depth is resolved
   * in a single drain call.
   */
  @SuppressWarnings ( "unchecked" )
  private void drainSlots () {
    int idx = 0;
    QChunk c = homeChunk;
    Object[] slots = c.slots;
    for ( ; ; ) {
      if ( c == writeChunk && idx >= writeIndex ) break;
      int base = idx << 1;
      ( (Consumer < Object >) slots[base] ).accept ( slots[base + 1] );
      slots[base] = null;
      slots[base + 1] = null;                  // clear for GC
      idx++;
      if ( idx >= QChunk.CAPACITY ) {
        QChunk next = c.next;
        if ( next == null ) break;             // safety exit
        c.next = null;                         // unlink overflow (becomes garbage)
        c = next;
        slots = c.slots;
        idx = 0;
      }
    }
    // Reset: reuse homeChunk for next cycle
    writeChunk = homeChunk;
    writeIndex = 0;
  }
}