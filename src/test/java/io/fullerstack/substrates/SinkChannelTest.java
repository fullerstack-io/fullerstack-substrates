package io.fullerstack.substrates;

import io.humainary.substrates.api.Substrates.Basin;
import io.humainary.substrates.api.Substrates.Capture;
import io.humainary.substrates.api.Substrates.Circuit;
import io.humainary.substrates.api.Substrates.Conduit;
import io.humainary.substrates.api.Substrates.Cortex;
import io.humainary.substrates.api.Substrates.Flow;
import io.humainary.substrates.api.Substrates.Name;
import io.humainary.substrates.api.Substrates.Pipe;
import io.humainary.substrates.api.Substrates.Port;
import io.humainary.substrates.api.Substrates.Sink;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static io.humainary.substrates.api.Substrates.cortex;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// A sink channel is a pipe this provider mints, and the rest of the runtime has to treat it as
/// one. None of this is covered by the Substrates TCK, which passed unchanged while every case
/// below was broken.
///
/// The channel cannot be an ordinary [FsPipe]: it stamps its `Capture` before the queue hop,
/// which is what gives §11.1 the *caller's* context for an external emission. Everything here
/// exists because that one difference used to leak.
@DisplayName ( "Sink channels behave as provider pipes" )
final class SinkChannelTest {

  private Cortex  cortex;
  private Circuit circuit;
  private Name    ch;

  @BeforeEach
  void setUp () {
    cortex  = cortex ();
    circuit = cortex.circuit ();
    ch      = cortex.name ( "ch" );
  }

  @AfterEach
  void tearDown () {
    circuit.close ();
  }

  private Sink < Integer > sinkInto ( ConcurrentLinkedQueue < Capture < Integer > > out ) {
    return circuit.sink ( circuit.pipe ( ( Capture < Integer > c ) -> out.add ( c ) ) );
  }

  // ─── provider guards: a sink channel is not "from another provider" ───────────────────────

  @Test
  @DisplayName ( "a Port may emit into a sink channel" )
  void portEmitsIntoSinkChannel () {
    final var seen = new ConcurrentLinkedQueue < Capture < Integer > > ();
    final Pipe < Integer > channel = sinkInto ( seen ).get ( ch );
    final Port < Integer > port = circuit.port ( 7 );

    assertDoesNotThrow ( () -> port.emit ( channel ) );
    circuit.await ();
    assertEquals ( List.of ( 7 ), seen.stream ().map ( Capture::emission ).toList () );
  }

  @Test
  @DisplayName ( "a Basin may drain into a sink channel" )
  void basinDrainsIntoSinkChannel () {
    final var seen = new ConcurrentLinkedQueue < Capture < Integer > > ();
    final Pipe < Integer > channel = sinkInto ( seen ).get ( ch );
    final Basin < Integer > basin = circuit.basin ( 4 );

    basin.pipe ().emit ( 1 );
    basin.pipe ().emit ( 2 );
    assertDoesNotThrow ( () -> basin.drain ( channel ) );
    circuit.await ();
    assertEquals ( List.of ( 1, 2 ), seen.stream ().map ( Capture::emission ).toList () );
  }

  @Test
  @DisplayName ( "a sink channel may be a fan-out target" )
  void sinkChannelIsAFanOutTarget () {
    final var seen = new ConcurrentLinkedQueue < Capture < Integer > > ();
    final Pipe < Integer > channel = sinkInto ( seen ).get ( ch );

    assertDoesNotThrow ( () -> circuit.pipe ( List.of ( channel ) ).emit ( 3 ) );
    circuit.await ();
    assertEquals ( List.of ( 3 ), seen.stream ().map ( Capture::emission ).toList () );
  }

  @Test
  @DisplayName ( "a sink channel may be another sink's endpoint" )
  void sinkChannelIsAValidEndpoint () {
    final Pipe < Capture < Integer > > inner =
      circuit.< Capture < Integer > >sink (
        circuit.pipe ( ( Capture < Capture < Integer > > c ) -> { } ) ).get ( cortex.name ( "nested" ) );

    assertDoesNotThrow ( () -> circuit.sink ( inner ) );
  }

  // ─── ownership: a pipe emits to its own circuit ───────────────────────────────────────────

  @Test
  @DisplayName ( "a cross-circuit endpoint still routes through the sink's own circuit" )
  void crossCircuitEndpointRoutesThroughOwningCircuit () {
    final Circuit away = cortex.circuit ( cortex.name ( "away" ) );
    try {
      final var endpointThread = new ConcurrentLinkedQueue < String > ();
      final Pipe < Capture < Integer > > far =
        away.pipe ( ( Capture < Integer > c ) -> endpointThread.add ( Thread.currentThread ().getName () ) );

      circuit.sink ( far ).get ( ch ).emit ( 1 );
      circuit.await ();
      away.await ();

      // Delivered on the endpoint's circuit — reached by a hop the sink's circuit made,
      // rather than straight off the calling thread.
      assertEquals ( 1, endpointThread.size () );
      assertTrue ( endpointThread.peek ().contains ( "away" ), endpointThread.toString () );
    } finally {
      away.close ();
    }
  }

  // ─── the documented usage patterns, run as the javadoc writes them ────────────────────────

  @Test
  @DisplayName ( "a Basin may be the endpoint that buffers captures" )
  void basinIsAValidEndpoint () {
    final Basin < Capture < Integer > > basin = circuit.basin ( 8 );
    final Sink < Integer > sink = circuit.sink ( basin.pipe () );
    sink.get ( ch ).emit ( 1 );

    final var drained = new ConcurrentLinkedQueue < Integer > ();
    basin.drain ( circuit.pipe ( ( Capture < Integer > c ) -> drained.add ( c.emission () ) ) );
    circuit.await ();

    assertEquals ( List.of ( 1 ), List.copyOf ( drained ) );
  }

  @Test
  @DisplayName ( "capture.subject() is the sink channel, taken after flow processing" )
  void captureSubjectIsTheSinkChannelAfterProcessing () {
    final var subjects = new ConcurrentLinkedQueue < Name > ();
    final Sink < Integer > sink = circuit.sink (
      circuit.pipe ( ( Capture < Integer > c ) -> subjects.add ( c.subject ().name () ) ) );

    final Flow < Integer, Integer > doubling = cortex.< Integer >flow ().map ( v -> v * 2 );
    sink.pool ( doubling::pipe ).get ( ch ).emit ( 5 );
    circuit.await ();

    assertEquals ( List.of ( ch ), List.copyOf ( subjects ) );
  }

  @Test
  @DisplayName ( "a value suppressed by a fiber mints no capture" )
  void suppressedValueMintsNoCapture () {
    final var minted = new ConcurrentLinkedQueue < Integer > ();
    final Sink < Integer > sink = circuit.sink (
      circuit.pipe ( ( Capture < Integer > c ) -> minted.add ( c.emission () ) ) );

    final Pipe < Integer > guarded =
      sink.pool ( cortex.< Integer >fiber ().guard ( v -> v > 10 )::pipe ).get ( ch );
    guarded.emit ( 1 );
    guarded.emit ( 99 );
    circuit.await ();

    assertEquals ( List.of ( 99 ), List.copyOf ( minted ) );
  }

  @Test
  @DisplayName ( "the endpoint may be wrapped to filter the output" )
  void endpointMayBeWrappedToFilterOutput () {
    final var out = new ConcurrentLinkedQueue < Integer > ();
    final Pipe < Capture < Integer > > raw =
      circuit.pipe ( ( Capture < Integer > c ) -> out.add ( c.emission () ) );

    final Sink < Integer > sink = circuit.sink (
      cortex.< Capture < Integer > >fiber ().guard ( c -> c.emission () > 10 ).pipe ( raw ) );
    sink.get ( ch ).emit ( 2 );
    sink.get ( ch ).emit ( 42 );
    circuit.await ();

    assertEquals ( List.of ( 42 ), List.copyOf ( out ) );
  }

  @Test
  @DisplayName ( "a sink may be bridged onto a source via subscriber(Name, Pool)" )
  void sinkMayBeBridgedOntoASource () {
    final var bridged = new ConcurrentLinkedQueue < String > ();
    final Sink < Integer > sink = circuit.sink (
      circuit.pipe ( ( Capture < Integer > c ) -> bridged.add ( c.subject ().name () + "=" + c.emission () ) ) );

    final Conduit < Integer > source = circuit.conduit ();
    source.subscribe ( circuit.subscriber ( cortex.name ( "bridge" ), sink ) );
    source.get ( cortex.name ( "alpha" ) ).emit ( 7 );
    circuit.await ();

    assertEquals ( List.of ( "alpha=7" ), List.copyOf ( bridged ) );
  }
}
