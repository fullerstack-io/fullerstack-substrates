package io.fullerstack.substrates;

import io.humainary.substrates.api.Substrates.Capture;
import io.humainary.substrates.api.Substrates.Current;
import io.humainary.substrates.api.Substrates.Fault;
import io.humainary.substrates.api.Substrates.Pipe;
import io.humainary.substrates.api.Substrates.Idempotent;
import io.humainary.substrates.api.Substrates.Provided;
import io.humainary.substrates.api.Substrates.Reservoir;
import io.humainary.substrates.api.Substrates.State;
import io.humainary.substrates.api.Substrates.Tenure;
import io.humainary.substrates.api.Substrates.Subject;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/// An in-memory buffer of captures that is also a Substrate.
///
/// A Reservoir is created from a Source and is given its own identity (Subject)
/// that is a child of the Source it was created from. It subscribes to its
/// source to capture all emissions into an internal buffer.
///
/// @param <E> the class type of the emitted value
/// @see Capture
@Provided
@Tenure ( Tenure.EPHEMERAL )
public final class FsReservoir < E > implements Reservoir < E > {

  /// A capture of an emitted value from a channel with its associated subject.
  /// 2.10 spec §390-465: also carries `current()` (emitting context) and `state()`
  /// (per-emission measures; empty by default).
  private record Cap < E >(
    E emission,
    Subject < Pipe < E > > subject,
    Subject < Current > current,
    State state
  ) implements Capture < E > {
  }

  /// The subject identity for this reservoir.
  private final Subject < Reservoir < E > > subject;

  /// Bounded capacity (2.9 spec §6006-6012: oldest evicted when full).
  private final int capacity;

  /// Internal buffer storing captured emissions, bounded by `capacity`.
  /// ArrayDeque gives O(1) pollFirst for eviction.
  private final ArrayDeque < Cap < E > > buffer;

  /// Closed flag. Spec §9.1 requires `close()` be idempotent — explicit gate
  /// here keeps the contract intact if cleanup grows beyond `buffer.clear()`.
  private volatile boolean closed;

  /// Creates a new reservoir with the given subject identity and bounded capacity.
  ///
  /// @param subject  the subject identity for this reservoir
  /// @param capacity the maximum number of captures retained; older entries are evicted when full
  public FsReservoir ( Subject < Reservoir < E > > subject, int capacity ) {
    this.subject  = subject;
    this.capacity = capacity;
    this.buffer   = new ArrayDeque <> ( Math.max ( 16, Math.min ( capacity, 1024 ) ) );
  }

  /// Returns the subject identity of this reservoir.
  ///
  /// @return the subject of this reservoir
  @Override
  public Subject < Reservoir < E > > subject () {
    return subject;
  }

  /// Returns a stream representing the events that have accumulated since the
  /// reservoir was created or the last call to this method.
  ///
  /// @return A stream consisting of stored events captured from channels.
  /// @see Capture
  public Stream < Capture < E > > drain () {
    if ( closed ) throw new Fault ( subject, "drain", "reservoir is closed" );
    List < Cap < E > > snapshot = new ArrayList <> ( buffer );
    buffer.clear ();
    return snapshot.stream ().map ( c -> c );
  }

  /// Captures an emission with its channel subject. Evicts the oldest entry
  /// when the buffer is at capacity (2.9 spec §6006-6012). 2.10: also records
  /// the emitting `current` and per-emission `state` for `Capture.current()`
  /// and `Capture.state()`.
  void capture ( E emission, Subject < Pipe < E > > channelSubject,
                 Subject < Current > current, State state ) {
    if ( buffer.size () >= capacity ) buffer.pollFirst ();
    buffer.add ( new Cap <> ( emission, channelSubject, current, state ) );
  }

  /// Closes this reservoir, releasing the captured emissions buffer.
  @Idempotent
  @Override
  public void close () {
    if ( closed ) return;
    closed = true;
    buffer.clear ();
  }

  /// Reservoir.close() is fully synchronous (buffer clear, no queued work),
  /// so closeAwait() reduces to close() — the "await" has nothing to wait on.
  @Idempotent
  @Override
  public void closeAwait () {
    close ();
  }

}
