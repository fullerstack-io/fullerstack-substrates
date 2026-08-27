package io.fullerstack.substrates;

import io.fullerstack.substrates.FsNodeWindow.Node;
import io.fullerstack.substrates.FsOperators.Wrap;

import java.util.function.Consumer;

/// **FsWindows** — backing strategies for `Flow.window(int)`.
///
/// `FsFlow` asks for a window operator and gets one; which of the strategies below it gets is
/// decided here, once per JVM, from the `io.fullerstack.substrates.window` system property.
/// The default is the shipped path and is untouched by anything in this file.
///
/// The point of the property is to measure the strategies against each other in **one binary**
/// — same jar, same JIT, one flag — because comparing across separate builds compares the
/// builds as much as it compares the strategies. Once a winner is settled the losers and this
/// switch go away.
///
/// | value | strategy | per emission |
/// |---|---|---|
/// | `legacy` *(default)* | `FsFlow.WindowCountWrap` | one store + `System.arraycopy` of `n-1` refs |
/// | `ring` | [RingWindow] | one masked store |
/// | `node` | [NodeWindow] | one pointer hop + one store |
final class FsWindows {

  private static final int LEGACY = 0;
  private static final int RING   = 1;
  private static final int NODE   = 2;

  private static final int IMPL = impl ();

  private FsWindows () {
  }

  private static int impl () {
    return switch ( System.getProperty ( "io.fullerstack.substrates.window", "legacy" ) ) {
      case "ring" -> RING;
      case "node" -> NODE;
      default     -> LEGACY;
    };
  }

  /// Physical buffer length for a window of `size` logical values.
  ///
  /// `FsWindow.at` masks the physical index with `buffer.length - 1`, so every buffer that
  /// reaches it must be a power of two. The logical capacity is unchanged — a `window(10)`
  /// still shows ten values; it just sits in a sixteen-slot buffer.
  static int physical ( int size ) {
    if ( size > ( 1 << 30 ) ) {
      throw new IllegalArgumentException ( "size must be <= " + ( 1 << 30 ) );
    }
    return size <= 1 ? 1 : Integer.highestOneBit ( size - 1 ) << 1;
  }

  /// The count-based window operator, per the selected strategy.
  static Wrap < Object > count ( int size ) {
    return switch ( IMPL ) {
      case RING -> downstream -> new RingWindow ( size, downstream );
      case NODE -> downstream -> new NodeWindow ( size, downstream );
      default   -> new FsFlow.WindowCountWrap ( size );
    };
  }

  /// **Ring** — one store per emission, no copying.
  ///
  /// `total` counts admissions and doubles as the ring cursor; `start` is derived from it
  /// rather than tracked, so an append is a masked store and two subtractions. The cursor is a
  /// `long` because a circuit emitting at 30 ns overflows an `int` one in about a minute.
  ///
  /// The window handed downstream has a `start` that wraps, which is why `FsWindow.at` masks.
  static final class RingWindow implements Consumer < Object > {

    private final int                 capacity;
    private final int                 mask;
    private final Object[]            buffer;
    private final Consumer < Object > downstream;
    private final FsWindow.Lease      lease = new FsWindow.Lease ();
    private       long                total;

    RingWindow ( int capacity, Consumer < Object > downstream ) {
      final int size = physical ( capacity );
      this.capacity   = capacity;
      this.mask       = size - 1;
      this.buffer     = new Object[ size ];
      this.downstream = downstream;
    }

    @Override
    public void accept ( Object value ) {
      final long cursor = total;
      buffer[ (int) ( cursor & mask ) ] = value;
      final long next = cursor + 1;
      total = next;
      final int length = next < capacity ? (int) next : capacity;
      final int start  = (int) ( ( next - length ) & mask );
      downstream.accept (
        new FsWindow <> ( buffer, start, length, false, lease, lease.latch () )
      );
    }
  }

  /// **Node** — a fixed circular chain, cycled; one pointer hop and one store per emission.
  ///
  /// The chain is built once at materialisation and never grows, so steady-state emission
  /// allocates only the view object — the same 40 bytes the array strategies allocate. Eviction
  /// is implicit: advancing onto a node overwrites the oldest value it held.
  ///
  /// See [FsNodeWindow] for what this trades — 24 bytes per element against four, on a
  /// structure whose terminal operations walk every element.
  static final class NodeWindow implements Consumer < Object > {

    private final int                 capacity;
    private final Node                origin;
    private final Consumer < Object > downstream;
    private final FsWindow.Lease      lease = new FsWindow.Lease ();
    private       Node                cursor;
    private       long                total;

    NodeWindow ( int capacity, Consumer < Object > downstream ) {
      final Node first = FsNodeWindow.chain ( capacity );
      this.capacity   = capacity;
      this.origin     = first;
      this.cursor     = first.prev;   // the first append advances onto `first`
      this.downstream = downstream;
    }

    @Override
    public void accept ( Object value ) {
      final Node node = cursor.next;
      node.value = value;
      cursor     = node;
      final long seen = total + 1;
      total = seen;
      final int  length = seen < capacity ? (int) seen : capacity;
      final Node head   = seen <= capacity ? origin : node.next;
      downstream.accept (
        new FsNodeWindow <> ( head, node, length, false, lease, lease.latch () )
      );
    }
  }
}
