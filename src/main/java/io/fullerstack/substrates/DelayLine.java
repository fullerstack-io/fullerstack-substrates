package io.fullerstack.substrates;

/// **DelayLine** — the worker-thread-local ring every retaining operator keeps its values in.
///
/// `delay`, `rolling` and both `window` forms all hold the last N admissions and differ only in
/// what they do with them. Before this each hand-rolled its own buffer, and between them used
/// three eviction strategies — two `System.arraycopy` shifts and a modulo against a non-constant
/// divisor. Nothing in the model supplied the storage, so each author decided independently.
/// See `docs/DECISIONS.md`.
///
/// The name is what instrumentation calls this: storage of the last N samples with indexed
/// access, whose accessors are *taps*. Not `Slots` — `Slot<T>` is already a Humainary type, a
/// named accessor into `State`, and a plural of it would name something unrelated.
///
/// ## Representation
///
/// A power-of-two ring held as `(head, length)`. Appending is a masked store; nothing moves.
///
/// ```
/// append : values[ ( head + length ) & mask ] = value
///          full ? head = ( head + 1 ) & mask : length++
/// at(i)  : values[ ( head + i ) & mask ]
/// ```
///
/// O(1) per admission at any capacity, where the shift this replaced was O(capacity) — perfasm
/// measured that `arraycopy` at 7.05% of a window profile at capacity 4 and 15.64% at 64, the
/// cost being the stub *call* rather than the copy length, which is why a capacity sweep looked
/// flat and hid it.
///
/// `head` is tracked rather than derived so the front can also be dropped without an append,
/// which is what a time-bounded window needs when several entries age out at once.
///
/// The physical length is rounded up to a power of two so the index is a mask rather than a
/// division. `capacity` stays the logical bound: a `delay(10)` still shows ten values, it just
/// sits in a sixteen-slot ring.
///
/// ## Capture times belong here
///
/// A time-bounded window needs a stamp per value. That parallel array is **owned by the line**,
/// not by the caller: an earlier revision kept it outside and had to expose the ring's physical
/// index so the caller could address its shadow array in step. A type that hands out its own
/// index space is the wrong abstraction, so the stamps moved in and the accessor went away.
///
/// Only a timed line allocates the array — see [#timed].
///
/// ## Allocation and confinement
///
/// Steady-state emission allocates nothing: the ring is built once per materialisation and
/// written in place. No atomics and no ordering — a line belongs to one materialised stage,
/// which runs only on its circuit's worker.
final class DelayLine {

  private final int      capacity;
  private final int      mask;
  private final Object[] values;

  /// Capture times, parallel to [#values]. Null unless the line was built by [#timed].
  private final long[] times;

  /// Physical index of the oldest retained value.
  private int head;

  /// How many values are retained — never above [#capacity].
  private int length;

  private DelayLine ( int capacity, boolean timestamped, Object seed ) {

    final int physical = physical ( capacity );

    this.capacity = capacity;
    this.mask     = physical - 1;
    this.values   = new Object[ physical ];
    this.times    = timestamped ? new long[ physical ] : null;

    if ( seed != null ) {
      for ( int i = 0; i < capacity; i++ ) values[ i ] = seed;
      length = capacity;
    }
  }

  /// Physical ring length for a logical capacity: the next power of two, so the index is a
  /// mask rather than a division. The logical bound is unchanged — a `delay(10)` still shows
  /// ten values, in a sixteen-slot ring.
  static int physical ( int size ) {
    if ( size > ( 1 << 30 ) ) {
      throw new IllegalArgumentException ( "size must be <= " + ( 1 << 30 ) );
    }
    return size <= 1 ? 1 : Integer.highestOneBit ( size - 1 ) << 1;
  }

  /// An empty line: [#size] grows with the first `capacity` admissions.
  static DelayLine of ( int capacity ) {
    return new DelayLine ( capacity, false, null );
  }

  /// A line that also records a capture time per value, for a time-bounded window.
  static DelayLine timed ( int capacity ) {
    return new DelayLine ( capacity, true, null );
  }

  /// A line pre-filled with `seed`, so it reads full from the first admission.
  ///
  /// `delay` needs this: it emits the value from `capacity` admissions ago, and for the first
  /// `capacity` admissions that is the seed the caller supplied.
  static DelayLine seeded ( int capacity, Object seed ) {
    return new DelayLine ( capacity, false, seed );
  }

  /// Appends a value, evicting the oldest once full. O(1).
  void append ( Object value ) {

    values[ ( head + length ) & mask ] = value;

    if ( length == capacity ) head = ( head + 1 ) & mask;
    else                      length++;
  }

  /// Appends a value with its capture time. Only valid on a [#timed] line.
  void append ( Object value, long time ) {

    final int slot = ( head + length ) & mask;
    times[ slot ] = time;
    append ( value );
  }

  /// Drops the `count` oldest values. O(1) — the front moves and nothing is copied.
  void dropOldest ( int count ) {

    head    = ( head + count ) & mask;
    length -= count;
  }

  /// How many values are currently retained — `capacity` once warmed up.
  int size () {
    return length;
  }

  /// Whether the line is holding its full capacity.
  boolean full () {
    return length == capacity;
  }

  /// The value at encounter position `index`, oldest first. O(1).
  Object at ( int index ) {
    return values[ ( head + index ) & mask ];
  }

  /// The capture time at encounter position `index`. Only valid on a [#timed] line.
  long timeAt ( int index ) {
    return times[ ( head + index ) & mask ];
  }

  /// The ring itself, for a [FsWindow] that reads it in place. Never copied, and never
  /// written by anyone but this line.
  Object[] buffer () {
    return values;
  }

  /// Physical index of the oldest retained value — a view's `start`. Wraps, which is what
  /// [FsWindow#at]'s masking exists for.
  int start () {
    return head;
  }
}
