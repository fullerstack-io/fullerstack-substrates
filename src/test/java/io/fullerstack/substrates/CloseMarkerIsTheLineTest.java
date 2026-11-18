package io.fullerstack.substrates;

import io.humainary.substrates.api.Substrates.Circuit;
import io.humainary.substrates.api.Substrates.Cortex;
import io.humainary.substrates.api.Substrates.Pipe;
import io.humainary.substrates.api.Substrates.Pulse;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static io.humainary.substrates.api.Substrates.cortex;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The owner's rule for a circuit's close (2026-11-18): **the close marker is the line.** Everything
/// queued ahead of it is processed to completion, its whole cascade included; anything queued behind
/// it is dropped, unrun. §9.1: "Queued operations whose position in the circuit's logical processing
/// order (§7.6.1) falls strictly after the component's own close job MUST be silently dropped". §9.3
/// step 1: "New emissions from caller contexts MUST NOT be processed."
///
/// `submit` already drops, on the caller's side, anything submitted once close has been accepted, so
/// the only work that can sit behind the marker belongs to a caller that read the circuit as open
/// just before `close()` and linked just after it. That race cannot be staged through the public
/// API, so these cases place the admission with `FsCircuit.admitAsIfRacingClose` and
/// `FsCircuit.awaitImpl` — the same two calls the racing producer and the racing awaiter would have
/// made — while the worker is held inside a receptor ahead of the marker. None sleeps: each waits on
/// a latch or on an observable thread state.
@DisplayName ( "Nothing is processed behind a circuit's close marker" )
@Timeout ( 30 )
final class CloseMarkerIsTheLineTest {

  private static Runnable holdWorker ( final Circuit circuit ) {
    final CountDownLatch release = new CountDownLatch ( 1 );
    final Pipe < Integer > gate = circuit.pipe ( _ -> {
      try {
        release.await ();
      } catch ( final InterruptedException e ) {
        Thread.currentThread ().interrupt ();
      }
    } );
    gate.emit ( 0 );
    return release::countDown;
  }

  private static void spinUntil ( final BooleanSupplier condition ) {
    while ( !condition.getAsBoolean () ) Thread.onSpinWait ();
  }

  private static boolean waiting ( final Thread thread ) {
    final Thread.State state = thread.getState ();
    return state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING;
  }

  @Test
  @DisplayName ( "§9.1 / §9.3: [a, CLOSE, b] — a and the cascade it raises are delivered; b, linked behind the marker while the worker was still inside a's turn, is not" )
  void anAdmissionBehindTheMarkerIsDroppedAndTheOneAheadCompletes () {
    final Cortex cortex = cortex ();
    final FsCircuit circuit = (FsCircuit) cortex.circuit ( cortex.name ( "home" ) );
    try {
      final List < String > seen = new CopyOnWriteArrayList <> ();
      final Pipe < String > second = circuit.pipe ( v -> seen.add ( v + "''" ) );
      final Pipe < String > first = circuit.pipe ( v -> {
        seen.add ( v + "'" );
        second.emit ( v );
      } );
      final Pipe < String > entry = circuit.pipe ( v -> {
        seen.add ( v );
        first.emit ( v );
      } );
      final FsPipe < String > behind = (FsPipe < String >) circuit.< String >pipe ( seen::add );

      final Runnable release = holdWorker ( circuit );
      entry.emit ( "a" );
      circuit.close ();
      circuit.admitAsIfRacingClose ( behind.receiver (), "b" );
      release.run ();
      circuit.await ();

      assertEquals ( List.of ( "a", "a'", "a''" ), seen, "a is ahead of the marker and completes; b is behind it" );
    } finally {
      circuit.close ();
    }
  }

  @Test
  @DisplayName ( "§9.1: every admission behind the marker is dropped, not only the first — and none of them runs later" )
  void everyAdmissionBehindTheMarkerIsDropped () {
    final Cortex cortex = cortex ();
    final FsCircuit circuit = (FsCircuit) cortex.circuit ( cortex.name ( "home" ) );
    try {
      final List < Integer > seen = new CopyOnWriteArrayList <> ();
      final FsPipe < Integer > pipe = (FsPipe < Integer >) circuit.< Integer >pipe ( seen::add );

      final Runnable release = holdWorker ( circuit );
      pipe.emit ( 1 );
      circuit.close ();
      for ( int i = 2; i <= 100; i++ ) circuit.admitAsIfRacingClose ( pipe.receiver (), i );
      release.run ();
      circuit.await ();
      // The worker is gone; nothing admitted from here on can run either.
      circuit.admitAsIfRacingClose ( pipe.receiver (), 101 );
      circuit.await ();

      assertEquals ( List.of ( 1 ), seen );
      assertTrue ( circuit.pulse ().isEmpty (), "the worker stopped at the marker" );
    } finally {
      circuit.close ();
    }
  }

  @Test
  @DisplayName ( "§5.5 / §9.3: an await marker linked behind the close marker is never dispatched, and its caller still returns — through the closed-and-terminated check" )
  void anAwaitBehindTheMarkerReturnsWithoutItsMarkerBeingDispatched () throws InterruptedException {
    final Cortex cortex = cortex ();
    final FsCircuit circuit = (FsCircuit) cortex.circuit ( cortex.name ( "home" ) );
    try {
      final List < String > seen = new CopyOnWriteArrayList <> ();
      final Pipe < String > ahead = circuit.pipe ( seen::add );
      final FsPipe < String > behind = (FsPipe < String >) circuit.< String >pipe ( seen::add );
      final AtomicBoolean returned = new AtomicBoolean ();
      final AtomicBoolean markerDispatched = new AtomicBoolean ( true );

      final Runnable release = holdWorker ( circuit );
      ahead.emit ( "a" );
      circuit.close ();
      circuit.admitAsIfRacingClose ( behind.receiver (), "b" );

      // An awaiter that read the circuit as open just before close(): its marker goes behind the
      // close marker. It parks only after it has linked the marker and spun, so a parked awaiter is
      // one whose marker is in the queue.
      final Thread awaiter = Thread.ofPlatform ().start ( () -> {
        circuit.awaitImpl ();
        markerDispatched.set ( FsCircuit.AWAIT_BARRIER.get ().released );
        returned.set ( true );
      } );
      spinUntil ( () -> waiting ( awaiter ) );

      release.run ();
      awaiter.join ();

      assertTrue ( returned.get (), "the awaiter returned" );
      assertFalse ( markerDispatched.get (), "its marker sat behind the close marker and was not dispatched" );
      assertEquals ( List.of ( "a" ), seen );
    } finally {
      circuit.close ();
    }
  }

  @Test
  @DisplayName ( "§9.1: closeAwait() returns once the marker has run, with an admission behind the marker left unrun" )
  void closeAwaitReturnsAtTheMarker () throws InterruptedException {
    final Cortex cortex = cortex ();
    final FsCircuit circuit = (FsCircuit) cortex.circuit ( cortex.name ( "home" ) );
    try {
      final List < String > seen = new CopyOnWriteArrayList <> ();
      final Pipe < String > ahead = circuit.pipe ( seen::add );
      final FsPipe < String > behind = (FsPipe < String >) circuit.< String >pipe ( seen::add );

      final Runnable release = holdWorker ( circuit );
      ahead.emit ( "a" );
      final Thread closer = Thread.ofPlatform ().start ( circuit::closeAwait );
      // closeAwait() has requested close and is joining the worker.
      spinUntil ( () -> circuit.isClosed () && waiting ( closer ) );
      circuit.admitAsIfRacingClose ( behind.receiver (), "b" );

      release.run ();
      closer.join ();

      assertEquals ( List.of ( "a" ), seen );
    } finally {
      circuit.close ();
    }
  }

  @Test
  @DisplayName ( "§5.7: a pulse probe admitted behind the close marker does not traverse the queue — pulse() is empty" )
  void aPulseBehindTheMarkerIsEmpty () throws InterruptedException {
    final Cortex cortex = cortex ();
    final Circuit circuit = cortex.circuit ( cortex.name ( "home" ) );
    try {
      final AtomicReference < Optional < Pulse > > pulse = new AtomicReference <> ();

      final Runnable release = holdWorker ( circuit );
      circuit.close ();
      // pulse() admits its probe whenever the worker is alive, so this one is public API all the
      // way: the probe is linked behind the marker, and the caller parks only after linking it.
      final Thread prober = Thread.ofPlatform ().start ( () -> pulse.set ( circuit.pulse () ) );
      spinUntil ( () -> waiting ( prober ) );

      release.run ();
      prober.join ();

      assertTrue ( pulse.get ().isEmpty (), "the probe was behind the marker" );
    } finally {
      circuit.close ();
    }
  }

}
