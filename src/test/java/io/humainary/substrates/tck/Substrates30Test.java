// Copyright (c) 2025 William David Louth

package io.humainary.substrates.tck;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/// Contract tests for the surfaces added in Substrates 3.0.0 (post SNAPSHOT-1):
///
/// - `Circuit.pipe(List)` / `Circuit.pipe(Name, List)` — list fan-out (SNAPSHOT-2)
/// - `Circuit.cell(Name, initial)` — named cell
/// - `Cortex.pool(Function<Name, T>)` — root pool with once-per-name caching
///
/// Basin (SNAPSHOT-3) is covered in [BasinTest]; the source-mirroring
/// composition that replaced Tap (SNAPSHOT-4) is exercised via
/// `Conduit.pool(Flow)` in the conduit tests.
final class Substrates30Test
  extends TestSupport {

  @Nested
  @DisplayName ( "Circuit.pipe(List) — fan-out (3.0)" )
  class PipeFanOut {

    @Test
    @DisplayName ( "fans each emission out to all targets in list order" )
    void fanOutDispatchesInOrder () {
      final var cortex = cortex ();
      final var circuit = cortex.circuit ();
      try {
        final List < String > seen = new ArrayList <> ();
        final Pipe < Integer > a = circuit.pipe ( v -> seen.add ( "a" + v ) );
        final Pipe < Integer > b = circuit.pipe ( v -> seen.add ( "b" + v ) );

        final Pipe < Integer > fan = circuit.pipe ( List.of ( a, b ) );

        fan.emit ( 1 );
        fan.emit ( 2 );
        circuit.await ();

        assertEquals ( List.of ( "a1", "b1", "a2", "b2" ), seen );
      } finally {
        circuit.close ();
      }
    }

    @Test
    @DisplayName ( "duplicate targets receive the emission once per occurrence" )
    void duplicatesReceivePerOccurrence () {
      final var cortex = cortex ();
      final var circuit = cortex.circuit ();
      try {
        final AtomicInteger count = new AtomicInteger ();
        final Pipe < Integer > target = circuit.pipe ( v -> count.incrementAndGet () );

        final Pipe < Integer > fan = circuit.pipe ( List.of ( target, target ) );

        fan.emit ( 7 );
        circuit.await ();

        assertEquals ( 2, count.get () );
      } finally {
        circuit.close ();
      }
    }

    @Test
    @DisplayName ( "targets are snapshotted at creation" )
    void snapshotsTargets () {
      final var cortex = cortex ();
      final var circuit = cortex.circuit ();
      try {
        final AtomicInteger count = new AtomicInteger ();
        final List < Pipe < Integer > > targets = new ArrayList <> ();
        targets.add ( circuit.pipe ( v -> count.incrementAndGet () ) );

        final Pipe < Integer > fan = circuit.pipe ( targets );

        // later mutation of the caller's list must have no effect
        targets.add ( circuit.pipe ( v -> count.addAndGet ( 100 ) ) );

        fan.emit ( 1 );
        circuit.await ();

        assertEquals ( 1, count.get () );
      } finally {
        circuit.close ();
      }
    }

    @Test
    @DisplayName ( "null list and null elements are rejected synchronously" )
    void rejectsNulls () {
      final var cortex = cortex ();
      final var circuit = cortex.circuit ();
      try {
        //noinspection DataFlowIssue
        assertThrows ( NullPointerException.class,
          () -> circuit.pipe ( (List < Pipe < Integer > >) null ) );

        final List < Pipe < Integer > > withNull = new ArrayList <> ();
        withNull.add ( circuit.pipe () );
        withNull.add ( null );
        assertThrows ( NullPointerException.class, () -> circuit.pipe ( withNull ) );
      } finally {
        circuit.close ();
      }
    }

    @Test
    @DisplayName ( "non-provider target raises Fault" )
    void rejectsForeignTargets () {
      final var cortex = cortex ();
      final var circuit = cortex.circuit ();
      try {
        final Pipe < Integer > donor = circuit.pipe ();
        final Pipe < Integer > foreign = new Pipe <> () {
          @Override
          public void emit ( final Integer emission ) { }

          @Override
          public Subject < Pipe < Integer > > subject () {
            return donor.subject ();
          }
        };
        assertThrows ( Fault.class, () -> circuit.pipe ( List.of ( circuit.pipe (), foreign ) ) );
      } finally {
        circuit.close ();
      }
    }

    @Test
    @DisplayName ( "named form always mints — honors the name for empty and single lists" )
    void namedFormAlwaysMints () {
      final var cortex = cortex ();
      final var circuit = cortex.circuit ();
      try {
        final Name name = cortex.name ( "fan.named" );

        final Pipe < Integer > empty = circuit.pipe ( name, List.of () );
        assertEquals ( name, empty.subject ().name () );
        empty.emit ( 1 ); // named no-op — must not throw
        circuit.await ();

        final AtomicInteger count = new AtomicInteger ();
        final Pipe < Integer > target = circuit.pipe ( v -> count.incrementAndGet () );
        final Pipe < Integer > single = circuit.pipe ( name, List.of ( target ) );
        assertNotSame ( target, single, "named single-target form must mint a forwarder" );
        assertEquals ( name, single.subject ().name () );

        single.emit ( 1 );
        circuit.await ();
        assertEquals ( 1, count.get () );
      } finally {
        circuit.close ();
      }
    }

    @Test
    @DisplayName ( "empty unnamed list collapses to the no-op pipe; single-element to pipe(Pipe)" )
    void unnamedDegenerateForms () {
      final var cortex = cortex ();
      final var circuit = cortex.circuit ();
      try {
        final Pipe < Integer > none = circuit.pipe ( List.of () );
        none.emit ( 1 ); // no-op — must not throw
        circuit.await ();

        // single-element: equivalent to pipe(Pipe) — a same-circuit target is returned as-is
        final Pipe < Integer > target = circuit.pipe ( v -> { } );
        assertSame ( target, circuit.pipe ( List.of ( target ) ) );
      } finally {
        circuit.close ();
      }
    }
  }

  @Nested
  @DisplayName ( "Circuit.cell(Name, initial) — named cell (3.0)" )
  class NamedCell {

    @Test
    @DisplayName ( "binds the supplied name to the cell's subject" )
    void bindsName () {
      final var cortex = cortex ();
      final var circuit = cortex.circuit ();
      try {
        final Name name = cortex.name ( "cell.named" );
        final Cell < Integer > cell = circuit.cell ( name, 42 );

        assertEquals ( name, cell.subject ().name () );
        assertEquals ( 42, cell.get () );

        cell.pipe ().emit ( 43 );
        circuit.await ();
        assertEquals ( 43, cell.get () );
      } finally {
        circuit.close ();
      }
    }

    @Test
    @DisplayName ( "rejects nulls and closed circuit" )
    void guards () {
      final var cortex = cortex ();
      final var circuit = cortex.circuit ();
      final Name name = cortex.name ( "cell.guarded" );
      try {
        //noinspection DataFlowIssue
        assertThrows ( NullPointerException.class, () -> circuit.cell ( null, 1 ) );
        //noinspection DataFlowIssue
        assertThrows ( NullPointerException.class, () -> circuit.cell ( name, null ) );
      } finally {
        circuit.close ();
      }
      assertThrows ( Fault.class, () -> circuit.cell ( name, 1 ) );
    }
  }

  @Nested
  @DisplayName ( "Cortex.pool(Function) — root pool (3.0)" )
  class RootPool {

    @Test
    @DisplayName ( "materializes on demand and invokes the function exactly once per name" )
    void oncePerName () {
      final var cortex = cortex ();
      final AtomicInteger invocations = new AtomicInteger ();

      final Pool < String > pool =
        cortex.pool ( name -> name.path () + "#" + invocations.incrementAndGet () );

      final Name a = cortex.name ( "pool.a" );
      final Name b = cortex.name ( "pool.b" );

      final String first = pool.get ( a );
      assertEquals ( first, pool.get ( a ), "cached result must be replayed" );
      assertNotEquals ( first, pool.get ( b ) );
      assertEquals ( 2, invocations.get (), "one invocation per distinct name" );
    }

    @Test
    @DisplayName ( "caches a thrown failure and rethrows the same exception" )
    void cachesFailures () {
      final var cortex = cortex ();
      final AtomicInteger invocations = new AtomicInteger ();

      final Pool < String > pool =
        cortex.pool ( name -> {
          invocations.incrementAndGet ();
          throw new IllegalStateException ( "boom" );
        } );

      final Name a = cortex.name ( "pool.fail" );

      final var first = assertThrows ( IllegalStateException.class, () -> pool.get ( a ) );
      final var second = assertThrows ( IllegalStateException.class, () -> pool.get ( a ) );

      assertSame ( first, second, "the same exception instance must be rethrown" );
      assertEquals ( 1, invocations.get (), "the function must not be re-invoked" );
    }

    @Test
    @DisplayName ( "null result raises NullPointerException, cached per name" )
    void rejectsNullResults () {
      final var cortex = cortex ();
      final AtomicInteger invocations = new AtomicInteger ();

      final Pool < String > pool =
        cortex.pool ( name -> {
          invocations.incrementAndGet ();
          return null;
        } );

      final Name a = cortex.name ( "pool.null" );

      assertThrows ( NullPointerException.class, () -> pool.get ( a ) );
      assertThrows ( NullPointerException.class, () -> pool.get ( a ) );
      assertEquals ( 1, invocations.get () );
    }

    @Test
    @DisplayName ( "composes with derived views and circuit state (name-keyed cells)" )
    void composesWithCircuitState () {
      final var cortex = cortex ();
      final var circuit = cortex.circuit ();
      try {
        final Pool < Cell < Integer > > cells =
          cortex.pool ( name -> circuit.cell ( name, 0 ) );

        final var conduit = circuit.conduit ( Integer.class );
        conduit.subscribe (
          circuit.subscriber (
            cortex.name ( "pool.bridge" ),
            cells.pool ( Cell::pipe )
          )
        );

        final Name channel = cortex.name ( "pool.channel" );
        conduit.get ( channel ).emit ( 7 );
        circuit.await ();

        assertEquals ( 7, cells.get ( channel ).get () );
      } finally {
        circuit.close ();
      }
    }
  }

}
