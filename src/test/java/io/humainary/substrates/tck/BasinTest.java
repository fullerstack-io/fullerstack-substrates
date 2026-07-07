// Copyright (c) 2025 William David Louth

package io.humainary.substrates.tck;

import org.junit.jupiter.api.Test;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/// Tests for the 3.0 Basin interface — the circuit-owned bounded buffer that
/// replaced the source-bound Reservoir.
///
/// This test class covers:
/// - Emission-order retention and drain-then-evict semantics
/// - Bounded capacity with oldest-first eviction
/// - Cross-circuit drain targets
/// - Memory release after drain operations
///
/// @author William David Louth
/// @since 3.0
final class BasinTest
  extends TestSupport {

  /// Drains the basin into a collecting pipe and awaits the circuit so the
  /// queued drain (and its forwarded emissions) have completed.
  private static < E > List < E > drain ( final Circuit circuit, final Basin < E > basin ) {
    final List < E > out = new ArrayList <> ();
    basin.drain ( circuit.pipe ( out::add ) );
    circuit.await ();
    return out;
  }

  @Test
  void testRetainsInEmissionOrderAndDrainEvicts () {

    final var cortex = cortex ();
    final var circuit = cortex.circuit ();

    try {

      final Basin < Integer > basin = circuit.basin ( 16 );
      final var feed = basin.pipe ();

      assertSame ( feed, basin.pipe (), "pipe() must return the same feed instance" );

      feed.emit ( 1 );
      feed.emit ( 2 );
      feed.emit ( 3 );

      circuit.await ();

      assertEquals ( List.of ( 1, 2, 3 ), drain ( circuit, basin ) );

      // drain evicts — a second drain sees nothing
      assertEquals ( List.of (), drain ( circuit, basin ) );

      // ...and only values emitted after the drain
      feed.emit ( 4 );
      circuit.await ();
      assertEquals ( List.of ( 4 ), drain ( circuit, basin ) );

    } finally {

      circuit.close ();

    }

  }

  @Test
  void testEvictsOldestWhenAtCapacity () {

    final var cortex = cortex ();
    final var circuit = cortex.circuit ();

    try {

      final Basin < Integer > basin = circuit.basin ( 3 );
      final var feed = basin.pipe ();

      for ( int i = 1; i <= 5; i++ ) {
        feed.emit ( i );
      }

      circuit.await ();

      // capacity 3 — the most recent three retained, oldest evicted
      assertEquals ( List.of ( 3, 4, 5 ), drain ( circuit, basin ) );

    } finally {

      circuit.close ();

    }

  }

  @Test
  void testDrainIntoForeignCircuitPipe () {

    final var cortex = cortex ();
    final var owner = cortex.circuit ();
    final var other = cortex.circuit ();

    try {

      final Basin < String > basin = owner.basin ( 8 );

      basin.pipe ().emit ( "cross" );
      owner.await ();

      final List < String > out = new ArrayList <> ();
      basin.drain ( other.pipe ( out::add ) );

      // drain forwards on the owner; delivery lands on the other circuit
      owner.await ();
      other.await ();

      assertEquals ( List.of ( "cross" ), out );

    } finally {

      other.close ();
      owner.close ();

    }

  }

  @Test
  void testDrainRejectsNullAndForeignPipes () {

    final var cortex = cortex ();
    final var circuit = cortex.circuit ();

    try {

      final Basin < Integer > basin = circuit.basin ( 4 );

      //noinspection DataFlowIssue
      assertThrows ( NullPointerException.class, () -> basin.drain ( null ) );

      // a caller-supplied Pipe implementation is not a runtime-provided pipe
      final Pipe < Integer > donor = circuit.pipe ();
      assertThrows ( Fault.class, () -> basin.drain ( new Pipe <> () {
        @Override
        public void emit ( final Integer emission ) { }

        @Override
        public Subject < Pipe < Integer > > subject () {
          return donor.subject ();
        }
      } ) );

    } finally {

      circuit.close ();

    }

  }

  /// Validates that drained items are released for garbage collection: after a
  /// drain forwards the retained values, the basin must not keep them reachable.
  @Test
  void testMemoryRetention () throws InterruptedException {

    final var cortex = cortex ();
    final var circuit = cortex.circuit ();

    try {

      final Basin < Object > basin = circuit.basin ( 1024 );
      final var feed = basin.pipe ();

      final int count = 100;
      final List < WeakReference < Object > > refs = new ArrayList <> ();

      for ( int i = 0; i < count; i++ ) {
        final var obj = new byte[1024 * 1024]; // 1MB
        refs.add ( new WeakReference <> ( obj ) );
        feed.emit ( obj );
      }

      circuit.await ();

      var drained = drain ( circuit, basin );
      assertEquals ( count, drained.size () );

      // Release strong refs
      //noinspection UnusedAssignment
      drained = null;

      for ( int i = 0; i < 10; i++ ) {
        System.gc ();
        Thread.sleep ( 100 );
      }

      final long retainedCount =
        refs.stream ()
          .filter ( r -> r.get () != null )
          .count ();

      if ( retainedCount > 0 ) {
        throw new AssertionError (
          "Memory leak detected! " + retainedCount + " objects retained."
        );
      }

    } finally {

      circuit.close ();

    }

  }

}
