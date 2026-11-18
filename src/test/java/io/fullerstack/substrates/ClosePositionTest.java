package io.fullerstack.substrates;

import io.humainary.substrates.api.Substrates.Circuit;
import io.humainary.substrates.api.Substrates.Conduit;
import io.humainary.substrates.api.Substrates.Cortex;
import io.humainary.substrates.api.Substrates.Name;
import io.humainary.substrates.api.Substrates.Pipe;
import io.humainary.substrates.api.Substrates.Subscriber;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static io.humainary.substrates.api.Substrates.cortex;
import static org.junit.jupiter.api.Assertions.assertEquals;

/// §7.6.1 (3.15.3): "A subscription's window ends at the earliest of three close jobs: its own, its
/// subscriber's, and its source's" — each a position in the circuit's logical processing order, never
/// the moment a caller called `close()`. Every case here is one caller context plus the circuit, held
/// apart by an explicit latch where the order of admission has to be fixed; none sleeps, and each
/// quiesces with `await` before asserting.
///
/// No kit scenario reaches these orderings. The 3.15.3 review found them by reading.
@DisplayName ( "A close ends a window at the close job's position" )
final class ClosePositionTest {

  /// Holds the worker inside a callback until the caller has admitted everything it means to, so the
  /// admissions behind it are processed in exactly the order one caller made them.
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
  /// §9.1 lets a provider process or drop work positioned ahead of a close job, and requires the
  /// choice to be documented. This provider's documented choice (`docs/CONFORMANCE.md`) is to
  /// process it, which is also what §7.6.1's window says of `1`; the test pins that choice. That `2`
  /// is not delivered is §9.1's MUST.
  @DisplayName ( "§7.6.1 / §9.1 (documented choice): [subscribe, emit 1, source close, emit 2] from one caller delivers 1 and not 2" )
  void sourceCloseEndsTheWindowAtItsOwnPosition () {
    final Cortex cortex = cortex ();
    final Circuit circuit = cortex.circuit ( cortex.name ( "home" ) );
    try {
      final Conduit < Integer > conduit = circuit.conduit ( cortex.name ( "numbers" ), Integer.class );
      final Pipe < Integer > channel = conduit.get ( cortex.name ( "ch" ) );
      final List < Integer > seen = new CopyOnWriteArrayList <> ();
      final AtomicInteger closed = new AtomicInteger ();

      final Runnable release = holdWorker ( circuit );
      conduit.subscribe ( circuit.subscriber ( cortex.name ( "s" ),
        ( _, registrar ) -> registrar.register ( seen::add ) ), _ -> closed.incrementAndGet () );
      channel.emit ( 1 );
      conduit.close ();
      channel.emit ( 2 );
      release.run ();
      circuit.await ();

      assertEquals ( List.of ( 1 ), seen, "1 is inside the window [registration job, source close job); 2 is behind the close job" );
      assertEquals ( 1, closed.get (), "onClose fires once" );
    } finally {
      circuit.close ();
    }
  }

  @Test
  @DisplayName ( "§7.6.1 / §9.1: a source's close is one position — an emission raised by one subscription's onClose reaches no other" )
  void sourceCloseEndsEveryWindowAtOnePosition () {
    final Cortex cortex = cortex ();
    final Circuit circuit = cortex.circuit ( cortex.name ( "home" ) );
    try {
      final Conduit < Integer > conduit = circuit.conduit ( cortex.name ( "numbers" ), Integer.class );
      final Pipe < Integer > channel = conduit.get ( cortex.name ( "ch" ) );
      final List < String > seen = new CopyOnWriteArrayList <> ();
      final AtomicInteger closed = new AtomicInteger ();

      for ( final String id : List.of ( "a", "b" ) ) {
        conduit.subscribe ( circuit.subscriber ( cortex.name ( id ),
          ( _, registrar ) -> registrar.register ( v -> seen.add ( id + v ) ) ), _ -> {
          closed.incrementAndGet ();
          channel.emit ( 9 );          // raised behind the source's close job (§9.1: dropped)
        } );
      }
      channel.emit ( 1 );
      circuit.await ();

      conduit.close ();
      circuit.await ();

      // Order between two subscriptions' receptors on one channel is not asserted.
      assertEquals ( Set.of ( "a1", "b1" ), Set.copyOf ( seen ), "nothing positioned behind the source's close job is delivered" );
      assertEquals ( 2, seen.size (), "each delivered once: " + seen );
      assertEquals ( 2, closed.get (), "each subscription's onClose fires once" );
    } finally {
      circuit.close ();
    }
  }

  @Test
  @DisplayName ( "§7.6.1: a subscriber's close is one position — its subscriptions on two sources end together" )
  void subscriberCloseEndsAllItsSubscriptionsAtOnePosition () {
    final Cortex cortex = cortex ();
    final Circuit circuit = cortex.circuit ( cortex.name ( "home" ) );
    try {
      final Name ch = cortex.name ( "ch" );
      final Conduit < Integer > left = circuit.conduit ( cortex.name ( "left" ), Integer.class );
      final Conduit < Integer > right = circuit.conduit ( cortex.name ( "right" ), Integer.class );
      final List < String > seen = new CopyOnWriteArrayList <> ();
      final AtomicInteger closed = new AtomicInteger ();

      final Subscriber < Integer > subscriber = circuit.subscriber ( cortex.name ( "s" ),
        ( subject, registrar ) -> {
          final String source = subject.enclosure ().orElseThrow ().name ().toString ();
          registrar.register ( v -> seen.add ( source + v ) );
        } );
      for ( final Conduit < Integer > conduit : List.of ( left, right ) ) {
        conduit.subscribe ( subscriber, _ -> {
          closed.incrementAndGet ();
          left.get ( ch ).emit ( 9 );    // raised behind the subscriber's close job
          right.get ( ch ).emit ( 9 );
        } );
      }
      left.get ( ch ).emit ( 1 );
      right.get ( ch ).emit ( 1 );
      circuit.await ();

      subscriber.close ();
      circuit.await ();

      assertEquals ( List.of ( "left1", "right1" ), seen, "both windows end at the subscriber's one close job" );
      assertEquals ( 2, closed.get (), "each subscription's onClose fires once" );
    } finally {
      circuit.close ();
    }
  }

  @Test
  @DisplayName ( "§7.6.1: a window emptied by a transit close fires onClose once, ahead of everything behind its registration job" )
  void emptyWindowFiresOnCloseNoLaterThanItsRegistration () throws Exception {
    final Cortex cortex = cortex ();
    final Circuit circuit = cortex.circuit ( cortex.name ( "home" ) );
    try {
      final Conduit < Integer > conduit = circuit.conduit ( cortex.name ( "numbers" ), Integer.class );
      final Pipe < Integer > channel = conduit.get ( cortex.name ( "ch" ) );
      final List < String > order = new CopyOnWriteArrayList <> ();
      final CountDownLatch admitted = new CountDownLatch ( 1 );

      final Subscriber < Integer > subscriber = circuit.subscriber ( cortex.name ( "s" ),
        ( _, registrar ) -> registrar.register ( v -> order.add ( "delivered " + v ) ) );
      final Pipe < Integer > closer = circuit.pipe ( _ -> {
        try {
          admitted.await ();
        } catch ( final InterruptedException e ) {
          Thread.currentThread ().interrupt ();
        }
        subscriber.close ();            // transit: executes ahead of the registration job in ingress
      } );
      final Pipe < Integer > marker = circuit.pipe ( _ -> order.add ( "marker" ) );

      closer.emit ( 0 );                // ingress 1: its callback waits for the admissions below
      conduit.subscribe ( subscriber, _ -> order.add ( "onClose" ) );   // ingress 2: the registration job
      marker.emit ( 0 );                // ingress 3: the first position behind the registration job
      channel.emit ( 1 );               // ingress 4: would be delivered to an installed subscription
      admitted.countDown ();
      circuit.await ();

      assertEquals ( List.of ( "onClose", "marker" ), order, "onClose once, no later than the registration job; the subscription is never selected" );
    } finally {
      circuit.close ();
    }
  }

}
