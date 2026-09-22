package io.fullerstack.substrates;

import io.humainary.substrates.api.Substrates.Circuit;
import io.humainary.substrates.api.Substrates.Conduit;
import io.humainary.substrates.api.Substrates.Cortex;
import io.humainary.substrates.api.Substrates.Name;
import io.humainary.substrates.api.Substrates.Subscription;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static io.humainary.substrates.api.Substrates.cortex;
import static org.junit.jupiter.api.Assertions.assertEquals;

/// §7.6.1 (3.3.0): a subscription's visibility window runs from the point its registration job
/// *executes* to the point its close job executes, in the circuit's logical processing order. A close
/// raised from inside a cascade is transit and executes before an add job still waiting in ingress —
/// the window is empty. The provider once installed such a subscription anyway (the add job checked
/// only the conduit's own latch), leaving a closed subscription on the roster that received every
/// later emission and could never be retired, because retire is idempotent and had already run.
/// No kit scenario reaches this ordering; the 3.3.0 verification round found it by reading.
@DisplayName ( "A subscription closed before its registration executes is never installed" )
final class SubscriptionCloseBeforeAddTest {

  @Test
  @DisplayName ( "close from inside a cascade, ahead of the add job, leaves an empty window" )
  void closeAheadOfAddInstallsNothing () throws Exception {
    final Cortex cortex = cortex ();
    final Circuit circuit = cortex.circuit ( cortex.name ( "home" ) );
    try {
      final Conduit < String > conduit = circuit.conduit ( cortex.name ( "words" ), String.class );
      final Name channel = cortex.name ( "ch" );

      final CountDownLatch handleReady = new CountDownLatch ( 1 );
      final AtomicReference < Subscription > late = new AtomicReference < > ();
      final CopyOnWriteArrayList < String > seenByLate = new CopyOnWriteArrayList < > ();

      // The first subscriber, installed and settled, will close the SECOND subscription from inside
      // the cascade of emission X — after the caller has queued that subscription's add job.
      conduit.subscribe ( circuit.subscriber ( cortex.name ( "first" ), ( subject, registrar ) ->
        registrar.register ( v -> {
          if ( "X".equals ( v ) ) {
            try {
              handleReady.await ();
            } catch ( final InterruptedException e ) {
              Thread.currentThread ().interrupt ();
            }
            late.get ().close ();                     // transit: executes before the add job below
          }
        } ) ) );
      circuit.await ();

      conduit.get ( channel ).emit ( "X" );          // ingress 1: its cascade blocks until the handle exists
      final Subscription second = conduit.subscribe ( circuit.subscriber ( cortex.name ( "second" ),
        ( subject, registrar ) -> registrar.register ( seenByLate::add ) ) );   // ingress 2: the add job
      late.set ( second );
      handleReady.countDown ();
      circuit.await ();

      conduit.get ( channel ).emit ( "Y" );          // would reach an installed-but-closed subscription
      circuit.await ();

      assertEquals ( 0, seenByLate.size (), "a subscription whose close executed before its registration is never installed: " + seenByLate );
    } finally {
      circuit.close ();
    }
  }

}
