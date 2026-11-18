package io.fullerstack.substrates;

import io.humainary.substrates.api.Substrates.Cell;
import io.humainary.substrates.api.Substrates.Circuit;
import io.humainary.substrates.api.Substrates.Conduit;
import io.humainary.substrates.api.Substrates.Cortex;
import io.humainary.substrates.api.Substrates.Fault;
import io.humainary.substrates.api.Substrates.Pipe;
import io.humainary.substrates.api.Substrates.Port;
import io.humainary.substrates.api.Substrates.Subject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.humainary.substrates.api.Substrates.cortex;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/// §9.3 step 1: "New emissions from caller contexts MUST NOT be processed." Step 3: "Emissions
/// already admitted at the time of marking MAY or MAY NOT complete — this is explicitly
/// implementation-defined. An implementation MUST document its choice." §5.3: transit raised while an
/// ingress item is processed is drained before the next ingress item — causal completion.
///
/// This provider's documented choice (`docs/CONFORMANCE.md`, the owner's ruling of 2026-11-18): close
/// stops *ingress* from taking new emissions. An emission admitted ahead of the close is processed to
/// completion, every hop of the cascade it raises included, whether or not `close()` had been called
/// by the time the worker reached it.
///
/// Every case holds the worker behind a latch while one caller admits its work and calls `close()`,
/// so the cascade always runs with close already accepted. None sleeps.
@DisplayName ( "A cascade admitted ahead of a circuit's close completes after it" )
final class CascadeCompletesAfterCloseTest {

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

  @Test
  @DisplayName ( "§5.3 / §9.3: [emit(x), close(), await] — conduit → pool-backed subscriber with a flow → second conduit → registered pipe → cell: the cell holds x's result" )
  void threeHopChainCompletesAfterClose () {
    final Cortex cortex = cortex ();
    final Circuit circuit = cortex.circuit ( cortex.name ( "home" ) );
    try {
      final Conduit < Integer > first = circuit.conduit ( cortex.name ( "first" ) );
      final Conduit < Integer > second = circuit.conduit ( cortex.name ( "second" ) );
      final Cell < Integer > cell = circuit.cell ( cortex.name ( "result" ), 0 );

      second.subscribe ( circuit.subscriber ( cortex.name ( "to-cell" ),
        ( _, registrar ) -> registrar.register ( cell.pipe () ) ) );
      first.subscribe ( circuit.subscriber ( cortex.name ( "to-second" ),
        second.pool ( cortex.< Integer >flow ().map ( x -> x * 2 ) ) ) );
      final Pipe < Integer > entry = first.get ( cortex.name ( "ch" ) );
      circuit.await ();

      final Runnable release = holdWorker ( circuit );
      entry.emit ( 21 );
      circuit.close ();
      release.run ();
      circuit.await ();

      assertEquals ( 42, cell.get (), "21 was admitted ahead of the close; its cascade runs to the cell" );
    } finally {
      circuit.close ();
    }
  }

  @Test
  @DisplayName ( "§5.3 / §6.3 / §9.3: a registered pipe's queued delivery raised after close was accepted is still performed" )
  void registeredPipeHopCompletesAfterClose () {
    final Cortex cortex = cortex ();
    final Circuit circuit = cortex.circuit ( cortex.name ( "home" ) );
    try {
      final Conduit < Integer > conduit = circuit.conduit ( cortex.name ( "numbers" ) );
      final List < Integer > seen = new CopyOnWriteArrayList <> ();
      final Pipe < Integer > sink = circuit.pipe ( seen::add );
      conduit.subscribe ( circuit.subscriber ( cortex.name ( "s" ),
        ( _, registrar ) -> registrar.register ( sink ) ) );
      final Pipe < Integer > entry = conduit.get ( cortex.name ( "ch" ) );
      circuit.await ();

      final Runnable release = holdWorker ( circuit );
      entry.emit ( 1 );
      entry.emit ( 2 );
      circuit.close ();
      release.run ();
      circuit.await ();

      assertEquals ( List.of ( 1, 2 ), seen );
    } finally {
      circuit.close ();
    }
  }

  @Test
  @DisplayName ( "§5.3 / §9.3 / §11.5: a Port update and emit raised in a cascade after close was accepted both take effect" )
  void portUpdateAndEmitCompleteAfterClose () {
    final Cortex cortex = cortex ();
    final Circuit circuit = cortex.circuit ( cortex.name ( "home" ) );
    try {
      final Conduit < Integer > conduit = circuit.conduit ( cortex.name ( "numbers" ) );
      final Port < Integer > total = circuit.port ( cortex.name ( "total" ), 100 );
      final List < Integer > seen = new CopyOnWriteArrayList <> ();
      final Pipe < Integer > out = circuit.pipe ( seen::add );
      conduit.subscribe ( circuit.subscriber ( cortex.name ( "s" ),
        ( _, registrar ) -> registrar.register ( v -> {
          total.update ( v, Integer::sum );
          total.emit ( out );
        } ) ) );
      final Pipe < Integer > entry = conduit.get ( cortex.name ( "ch" ) );
      circuit.await ();

      final Runnable release = holdWorker ( circuit );
      entry.emit ( 5 );
      circuit.close ();
      release.run ();
      circuit.await ();

      assertEquals ( List.of ( 105 ), seen, "the update, the emit and the emitted value's delivery are three queued hops behind the dispatch" );
    } finally {
      circuit.close ();
    }
  }

  @Test
  @DisplayName ( "§9.3 step 1: an emission submitted from a caller context after close() returned is dropped, silently" )
  void emissionSubmittedAfterCloseReturnedIsDropped () {
    final Cortex cortex = cortex ();
    final Circuit circuit = cortex.circuit ( cortex.name ( "home" ) );
    try {
      final List < Integer > seen = new CopyOnWriteArrayList <> ();
      final Pipe < Integer > pipe = circuit.pipe ( seen::add );

      final Runnable release = holdWorker ( circuit );
      pipe.emit ( 1 );
      circuit.close ();
      pipe.emit ( 2 );
      release.run ();
      circuit.await ();

      assertEquals ( List.of ( 1 ), seen, "1 is ahead of the close; 2 was submitted after it was accepted" );
    } finally {
      circuit.close ();
    }
  }

  @Test
  @DisplayName ( "§9.1 open-required / §5.3: a factory called from a cascade after close was accepted is rejected with the circuit's Fault, and the cascade's emissions still complete" )
  void factoryInACascadeAfterCloseIsRejectedWhileItsEmissionsComplete () {
    final Cortex cortex = cortex ();
    final Circuit circuit = cortex.circuit ( cortex.name ( "home" ) );
    try {
      final List < Integer > seen = new CopyOnWriteArrayList <> ();
      final AtomicReference < Subject < ? > > rejectedBy = new AtomicReference <> ();
      final Pipe < Integer > last = circuit.pipe ( seen::add );
      final Pipe < Integer > middle = circuit.pipe ( v -> {
        last.emit ( v + 1 );
        try {
          circuit.conduit ( cortex.name ( "late" ) );
        } catch ( final Fault fault ) {
          rejectedBy.set ( fault.subject () );
        }
        last.emit ( v + 2 );
      } );
      final Pipe < Integer > entry = circuit.pipe ( middle::emit );

      final Runnable release = holdWorker ( circuit );
      entry.emit ( 10 );
      circuit.close ();
      release.run ();
      circuit.await ();

      assertSame ( circuit.subject (), rejectedBy.get (), "the factory is open-required: the circuit's own guard rejects it" );
      assertEquals ( List.of ( 11, 12 ), seen, "the emissions around the rejected factory call complete" );
    } finally {
      circuit.close ();
    }
  }

  @Test
  @DisplayName ( "§5.3 cyclic safety / §9.3: a long finite self-feeding cascade started ahead of the close runs to its end after it, at constant stack depth" )
  void longFiniteSelfFeedingCascadeCompletesAfterClose () {
    final Cortex cortex = cortex ();
    final Circuit circuit = cortex.circuit ( cortex.name ( "home" ) );
    try {
      final int hops = 1_000_000;
      final AtomicInteger last = new AtomicInteger ( -1 );
      final AtomicInteger depthAtStart = new AtomicInteger ();
      final AtomicInteger depthAtEnd = new AtomicInteger ();
      final AtomicReference < Pipe < Integer > > self = new AtomicReference <> ();
      self.set ( circuit.pipe ( n -> {
        if ( n == hops ) depthAtStart.set ( Thread.currentThread ().getStackTrace ().length );
        if ( n > 0 ) {
          self.get ().emit ( n - 1 );
        } else {
          depthAtEnd.set ( Thread.currentThread ().getStackTrace ().length );
          last.set ( n );
        }
      } ) );

      final Runnable release = holdWorker ( circuit );
      self.get ().emit ( hops );
      circuit.close ();
      release.run ();
      circuit.await ();

      assertEquals ( 0, last.get (), "the countdown reached its end" );
      // The first hop is the ingress dispatch and the rest run from the transit drain, so the two
      // depths differ by the fixed frames between those two call sites — not by the hop count.
      final int growth = Math.abs ( depthAtEnd.get () - depthAtStart.get () );
      assertEquals ( true, growth < 8, "stack depth moved by " + growth + " frames over " + hops + " hops" );
    } finally {
      circuit.close ();
    }
  }

}
