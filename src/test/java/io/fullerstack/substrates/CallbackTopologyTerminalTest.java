package io.fullerstack.substrates;

import io.humainary.substrates.api.Substrates.Circuit;
import io.humainary.substrates.api.Substrates.Conduit;
import io.humainary.substrates.api.Substrates.Cortex;
import io.humainary.substrates.api.Substrates.Pipe;
import io.humainary.substrates.api.Substrates.Subscription;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import static io.humainary.substrates.api.Substrates.cortex;
import static org.junit.jupiter.api.Assertions.assertEquals;

/// §7.6.1 through a composed terminal: "a topology change inside a cascade takes effect at
/// its own position", and the recipients of an emission are "exactly those subscriptions
/// effective at that position" — the position at which the channel processes it.
///
/// The 3.3.0 kit exercises this through the plain channel pipe
/// (`CallbackTopologyContractTest`). This provider had a second path: a fiber or flow whose
/// target is a conduit pipe used to submit the channel's dispatch consumer as built at its
/// last rebuild, so a subscribe or close raised in the same callback as the emit — and
/// therefore ahead of the delivery in transit — was not seen by that delivery. The kit's plain
/// path passed while `fiber.pipe(conduit.get(name))`, the shape the doctrine recommends,
/// did not. Each case below is run through both terminals.
@DisplayName ( "A topology change inside a cascade is visible through a fiber or flow terminal" )
final class CallbackTopologyTerminalTest {

  private Cortex             cortex;
  private Circuit            circuit;
  private Conduit < String > conduit;
  private Pipe < String >    channel;

  @BeforeEach
  void setUp () {
    cortex  = cortex ();
    circuit = cortex.circuit ( cortex.name ( "terminal" ) );
    conduit = circuit.conduit ( cortex.name ( "c" ), String.class );
    channel = conduit.get ( cortex.name ( "x" ) );
  }

  @AfterEach
  void tearDown () {
    circuit.close ();
  }

  // ─── the two terminals under test ───

  private Pipe < String > viaFiber () {
    return cortex.fiber ( String.class ).guard ( _ -> true ).pipe ( channel );
  }

  private Pipe < String > viaFlow () {
    return cortex.flow ( String.class ).map ( v -> v ).pipe ( channel );
  }

  // ─── helpers ───

  private record Seen ( List < String > values, Subscription subscription ) {}

  private Seen subscribe ( String name ) {
    final List < String > seen = new CopyOnWriteArrayList <> ();
    final Subscription sub = conduit.subscribe ( circuit.subscriber (
      cortex.name ( name ), ( _, r ) -> r.register ( (String v) -> seen.add ( v ) ) ) );
    return new Seen ( seen, sub );
  }

  /// Runs `inside` on the worker as one callback — a receptor of a circuit pipe — so every
  /// operation it performs is transit work of one cascade (§5.3), then drains.
  private void inCascade ( Runnable inside ) {
    final Pipe < String > trigger = circuit.pipe ( (String _) -> inside.run () );
    trigger.emit ( "go" );
    circuit.await ();
  }

  // ─── subscribe, then emit, in one callback ───

  private void subscribeThenEmit ( Supplier < Pipe < String > > terminal ) {
    final Seen s1 = subscribe ( "s1" );
    channel.emit ( "warm" );          // s1 discovered; the channel has rebuilt once
    circuit.await ();
    final Pipe < String > via = terminal.get ();
    final Seen[] s2 = new Seen[1];
    inCascade ( () -> {
      s2[0] = subscribe ( "s2" );    // transit: registration job
      via.emit ( "E" );              // transit: the chain, then the channel's delivery
    } );
    channel.emit ( "F" );
    circuit.await ();
    // §7.6.1 consequence 2: s2 is effective from the point its registration executes, which
    // is ahead of E's delivery in transit — so s2 sees E.
    assertEquals ( List.of ( "E", "F" ), s2[0].values () );
    assertEquals ( List.of ( "warm", "E", "F" ), s1.values () );
  }

  @Test
  @DisplayName ( "fiber terminal: a subscription registered earlier in the callback receives the emission" )
  void fiber_subscribeThenEmit () {
    subscribeThenEmit ( this::viaFiber );
  }

  @Test
  @DisplayName ( "flow terminal: a subscription registered earlier in the callback receives the emission" )
  void flow_subscribeThenEmit () {
    subscribeThenEmit ( this::viaFlow );
  }

  // ─── close, then emit, in one callback ───

  private void closeThenEmit ( Supplier < Pipe < String > > terminal ) {
    final Seen s1 = subscribe ( "s1" );
    final Seen s2 = subscribe ( "s2" );
    channel.emit ( "warm" );
    circuit.await ();
    final Pipe < String > via = terminal.get ();
    inCascade ( () -> {
      s1.subscription ().close ();   // transit: close job — s1's window ends here
      via.emit ( "E" );              // delivered at a later position
    } );
    channel.emit ( "F" );
    circuit.await ();
    // §7.6.1: "An emission a channel processes … at or after its close job, does not select
    // that subscription." The former terminal kept delivering to s1 until the next ingress
    // emission rebuilt the channel.
    assertEquals ( List.of ( "warm" ), s1.values () );
    assertEquals ( List.of ( "warm", "E", "F" ), s2.values () );
  }

  @Test
  @DisplayName ( "fiber terminal: a subscription closed earlier in the callback is not selected" )
  void fiber_closeThenEmit () {
    closeThenEmit ( this::viaFiber );
  }

  @Test
  @DisplayName ( "flow terminal: a subscription closed earlier in the callback is not selected" )
  void flow_closeThenEmit () {
    closeThenEmit ( this::viaFlow );
  }
}
