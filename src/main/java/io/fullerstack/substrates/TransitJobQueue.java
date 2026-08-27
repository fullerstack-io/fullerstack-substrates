package io.fullerstack.substrates;

import java.util.function.Consumer;

/// Single-threaded transit queue over a **reusable chain of nodes**, as the
/// node-based counterpart to [TransitQueueRing]'s two parallel arrays.
///
/// Node reuse is safe here for the reason it is not safe on ingress. The hazard
/// that lost emissions and parked `await` came from *cross-thread* recycling — a
/// producer claiming a node the consumer had freed. Transit is owned entirely by
/// the circuit worker: one thread enqueues, the same thread drains, nothing else
/// can observe a node. So the chain is built once, grown on demand to the
/// deepest cascade seen, and then reused. No atomics, no memory ordering.
///
/// It shares [Job] with [JobQueue]. That was once thought unsafe — the original `TransitQueue`
/// shared a chunk type with the ingress queue and paid a `StoreLoad` fence per overflow link.
/// But the cause there was a **`volatile` declaration**, which forces its cost on every user of
/// the type. `Job.next` is a plain field and both queues take their ordering from the access
/// site: release/acquire on ingress, plain reads here. Nothing is forced on anyone, so one
/// carrier serves both and the recycling policy stays where it belongs — in the queue.
///
/// **Measured against [TransitQueueRing]** — 4 interleaved reps, 5 forks,
/// `-prof gc`, all TCK-green:
///
/// - `cyclic_emit_deep_100k`  ring 13.279  node 12.500
/// - `cyclic_emit_direct`     ring 17.602  node 16.605
/// - `cyclic_emit_registrar`  ring 16.842  node 17.165
///
/// Lower on three rows of four, but **no row is significant**: the deltas
/// (0.32-1.00 ns) sit well inside the within-arm ranges (1.3-6.2 ns), and
/// `registrar` flipped sign between two reps and four. On time, the two designs
/// are indistinguishable on this hardware.
///
/// Where the ring is unambiguously better is deep cascades. It allocates
/// **0.024 B/op with zero collections** on `cyclic_emit_deep_100k`; this
/// allocates 0.055 B/op and triggers 5, because the chain extends to cascade
/// depth. Negligible in absolute terms, but it is the one signal in the
/// comparison that is not noise.
final class TransitJobQueue {

  private final Job first = new Job ();

  private Job writeNode = first;   // next node to fill
  private Job readNode  = first;   // next node to dispatch

  void enqueue ( Consumer < Object > receiver, Object value ) {
    final Job n = writeNode;
    n.receiver = receiver;
    n.value    = value;
    final Job next = n.next;
    if ( next == null ) { grow ( n ); return; }
    writeNode = next;
  }

  /// Extends the chain by one node, and is the reason [#enqueue] stays small.
  ///
  /// Inlined, `enqueue` measured 48 bytes and PrintInlining reported "callee is
  /// too large" at the colder call sites — over the 35-byte `MaxInlineSize`, so
  /// it inlined only where the site was hot enough. That is a latent cliff, not
  /// a present cost, and it is the same shape as `FsCircuit.current()` at 76
  /// bytes and `FsCortex.getOrCreateCurrent` at 97.
  ///
  /// Unlike [IngressQueue#peek], whose rollover branch fires once per chunk and
  /// whose extraction cost `SinkOps` 3.6 ns, this one runs once per
  /// cascade-depth for the entire life of the circuit: after the deepest cascade
  /// ever seen, it is never reached again.
  private void grow ( final Job tail ) {
    final Job fresh = new Job ();
    tail.next = fresh;
    writeNode = fresh;
  }

  boolean hasWork () {
    return readNode != writeNode;
  }

  boolean drain () {
    if ( readNode == writeNode ) return false;
    do {
      final Job n = readNode;
      final Consumer < Object > r = n.receiver;
      final Object              v = n.value;
      n.receiver = null;            // clear for GC — the node itself is retained
      n.value    = null;
      readNode   = n.next;
      // §15.4 isolation: cascade dispatch reaches user code through fiber/flow
      // operator functions, same guard as the ingress drain.
      try {
        r.accept ( v );
      } catch ( Throwable ignored ) {
        // §15.4 #4: silently dropped; observability is impl-defined.
      }
    } while ( readNode != writeNode );
    // Drained: rewind both cursors so the next cycle refills from the start.
    readNode  = first;
    writeNode = first;
    return true;
  }
}
