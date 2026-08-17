package io.fullerstack.substrates;

import io.humainary.substrates.api.Substrates.Circuit;
import io.humainary.substrates.api.Substrates.Cortex;
import io.humainary.substrates.api.Substrates.Pipe;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.humainary.substrates.api.Substrates.cortex;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// §5.5 — await is a **positional** barrier, and concurrent awaiters suspend independently.
///
/// > "It suspends the calling execution context until every circuit operation accepted before
/// > the await call has completed."
///
/// > "Multiple caller contexts MAY call await concurrently; each suspends independently."
///
/// `awaitImpl` CASes a single `awaiterThread` slot and lets a later caller **piggyback** on the
/// first awaiter's marker.  That marker was admitted *before* the later caller's own work, so
/// the piggybacker can be released while its own emission is still queued — which is the
/// barrier clause violated, and the independence clause is what makes piggybacking illegal in
/// the first place.
///
/// The first test below fails on a piggybacking implementation and passes on one that gives
/// every awaiter its own marker.  It is written to make the race deterministic rather than
/// probable: the worker is held inside a receptor until the second awaiter has provably
/// admitted its emission *and* entered await.
final class AwaitBarrierTest {

  @Test
  @DisplayName ( "a second awaiter waits for its own work, not the first awaiter's marker" )
  void secondAwaiterIsNotReleasedByTheFirstAwaitersMarker () throws Exception {

    final Cortex cortex = cortex ();
    final Circuit circuit = cortex.circuit ( cortex.name ( "await.independent" ) );

    final List < String > seen = new CopyOnWriteArrayList <> ();
    final CountDownLatch blocking = new CountDownLatch ( 1 );
    final CountDownLatch workerIsBlocked = new CountDownLatch ( 1 );

    final Pipe < String > pipe =
      circuit.pipe (
        value -> {
          if ( "block".equals ( value ) ) {
            workerIsBlocked.countDown ();
            try {
              // Hold the circuit context so that everything admitted after this point is
              // provably still queued when the second awaiter calls await.
              blocking.await ( 5, TimeUnit.SECONDS );
            } catch ( InterruptedException e ) {
              Thread.currentThread ().interrupt ();
            }
          }
          seen.add ( value );
        }
      );

    pipe.emit ( "block" );
    assertTrue ( workerIsBlocked.await ( 5, TimeUnit.SECONDS ), "worker never entered the receptor" );

    // First awaiter: its marker is admitted here, BEHIND "block" but AHEAD of everything below.
    final CountDownLatch firstAwaiting = new CountDownLatch ( 1 );
    final Thread first = new Thread ( () -> {
      firstAwaiting.countDown ();
      circuit.await ();
    }, "awaiter-first" );
    first.start ();
    assertTrue ( firstAwaiting.await ( 5, TimeUnit.SECONDS ) );
    Thread.sleep ( 100 );                       // let the first marker land

    // Now admit work AFTER the first awaiter's marker, then await from a second thread.
    pipe.emit ( "second-work" );

    final AtomicBoolean sawOwnWork = new AtomicBoolean ();
    final CountDownLatch secondDone = new CountDownLatch ( 1 );
    final Thread second = new Thread ( () -> {
      circuit.await ();
      sawOwnWork.set ( seen.contains ( "second-work" ) );
      secondDone.countDown ();
    }, "awaiter-second" );
    second.start ();
    Thread.sleep ( 100 );                       // let the second awaiter register

    blocking.countDown ();                      // release the worker

    assertTrue ( secondDone.await ( 5, TimeUnit.SECONDS ), "second awaiter never returned" );
    first.join ( 5_000 );
    second.join ( 5_000 );
    circuit.close ();

    assertTrue (
      sawOwnWork.get (),
      "await returned before work admitted before the call had completed: the second awaiter "
        + "was released by the FIRST awaiter's earlier marker (§5.5 barrier + independence)"
    );
  }

  @Test
  @DisplayName ( "every concurrent awaiter observes its own emission" )
  void concurrentAwaitersEachGetTheirOwnBarrier () throws Exception {

    final Cortex cortex = cortex ();
    final Circuit circuit = cortex.circuit ( cortex.name ( "await.many" ) );

    final List < Integer > seen = new CopyOnWriteArrayList <> ();
    final Pipe < Integer > pipe = circuit.pipe ( seen::add );

    final int awaiters = 12;
    final List < Integer > missed = new CopyOnWriteArrayList <> ();
    final CountDownLatch start = new CountDownLatch ( 1 );
    final CountDownLatch done = new CountDownLatch ( awaiters );

    for ( int i = 0; i < awaiters; i++ ) {
      final int value = i;
      new Thread ( () -> {
        try {
          start.await ();
          pipe.emit ( value );
          circuit.await ();
          if ( !seen.contains ( value ) ) missed.add ( value );
        } catch ( InterruptedException e ) {
          Thread.currentThread ().interrupt ();
        } finally {
          done.countDown ();
        }
      }, "awaiter-" + i ).start ();
    }

    start.countDown ();
    assertTrue ( done.await ( 10, TimeUnit.SECONDS ), "awaiters did not finish" );
    circuit.close ();

    assertTrue (
      missed.isEmpty (),
      "awaiters returned before their own work completed: " + missed
    );
  }

  @Test
  @DisplayName ( "await after closure returns immediately" )
  void awaitAfterClosureReturnsImmediately () {

    final Cortex cortex = cortex ();
    final Circuit circuit = cortex.circuit ( cortex.name ( "await.closed" ) );
    circuit.pipe ( ( Object v ) -> { } ).emit ( 1 );
    circuit.close ();

    final long started = System.nanoTime ();
    circuit.await ();
    assertTrue (
      System.nanoTime () - started < TimeUnit.SECONDS.toNanos ( 2 ),
      "await hung after closure (§5.5)"
    );
  }

  @Test
  @DisplayName ( "awaiters racing close are all released" )
  void awaitersRacingCloseAreReleased () throws Exception {

    final Cortex cortex = cortex ();
    final Circuit circuit = cortex.circuit ( cortex.name ( "await.race" ) );
    circuit.pipe ( ( Object v ) -> { } ).emit ( 1 );

    final int n = 6;
    final CountDownLatch done = new CountDownLatch ( n );
    for ( int i = 0; i < n; i++ ) {
      new Thread ( () -> {
        circuit.await ();
        done.countDown ();
      }, "racer-" + i ).start ();
    }

    circuit.close ();

    assertTrue (
      done.await ( 10, TimeUnit.SECONDS ),
      "an awaiter was left waiting past closure (§5.5: await MUST return immediately)"
    );
    assertFalse ( Thread.currentThread ().isInterrupted () );
  }
}
