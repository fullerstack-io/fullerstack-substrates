package io.fullerstack.substrates;

import io.humainary.substrates.api.Substrates.Capture;
import io.humainary.substrates.api.Substrates.Circuit;
import io.humainary.substrates.api.Substrates.Cortex;
import io.humainary.substrates.api.Substrates.Name;
import io.humainary.substrates.api.Substrates.Pipe;
import io.humainary.substrates.api.Substrates.Sink;
import io.humainary.substrates.api.Substrates.Ticker;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static io.humainary.substrates.api.Substrates.cortex;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/// `Capture#current()` is the emitting execution context, and the accessor names three cases:
/// the caller for an ingress emission, the circuit for a transit cascade, and — the one that was
/// wrong — the *owning circuit* for a circuit-internal mechanism, "not the ticker's scheduling
/// thread".
///
/// The first two are covered by the Substrates TCK. The third is not, and a ticker feeding a sink
/// stamped every capture with the scheduler thread until the tick was routed through a
/// circuit-owned pipe.
@DisplayName ( "Capture provenance" )
final class CaptureProvenanceTest {

  private Set < Name > contextsSeenBy ( Circuit owner, java.util.function.Consumer < Pipe < Long > > emitter ) {

    final var seen = new ConcurrentLinkedQueue < Name > ();
    final Sink < Long > sink = owner.sink (
      owner.pipe ( ( Capture < Long > c ) -> seen.add ( c.current ().name () ) ) );

    emitter.accept ( sink.get ( cortex ().name ( "ch" ) ) );
    owner.await ();
    return seen.stream ().collect ( Collectors.toUnmodifiableSet () );
  }

  @Test
  @DisplayName ( "a ticker emission is attributed to the owning circuit, not its scheduler" )
  void tickerEmissionIsAttributedToTheCircuit () throws Exception {

    final Cortex cortex = cortex ();
    final Circuit home  = cortex.circuit ( cortex.name ( "home" ) );

    try {
      final var seen = new ConcurrentLinkedQueue < Name > ();
      final Sink < Long > sink = home.sink (
        home.pipe ( ( Capture < Long > c ) -> seen.add ( c.current ().name () ) ) );

      final Ticker ticker =
        home.ticker ( cortex.name ( "t" ), Duration.ofMillis ( 20 ), sink.get ( cortex.name ( "tick" ) ) );
      Thread.sleep ( 150 );
      ticker.close ();
      home.await ();

      assertFalse ( seen.isEmpty (), "ticker produced no captures" );
      assertEquals ( Set.of ( cortex.name ( "home" ) ),
        seen.stream ().collect ( Collectors.toUnmodifiableSet () ),
        "a tick must report the owning circuit, never the scheduler thread" );
    } finally {
      home.close ();
    }
  }

  @Test
  @DisplayName ( "an external emission is attributed to the calling thread" )
  void externalEmissionIsAttributedToTheCaller () throws Exception {

    final Cortex cortex = cortex ();
    final Circuit home  = cortex.circuit ( cortex.name ( "home" ) );

    try {
      final var seen = new ConcurrentLinkedQueue < Name > ();
      final Sink < Long > sink = home.sink (
        home.pipe ( ( Capture < Long > c ) -> seen.add ( c.current ().name () ) ) );
      final Pipe < Long > channel = sink.get ( cortex.name ( "ch" ) );

      final ExecutorService foreign =
        Executors.newSingleThreadExecutor ( r -> new Thread ( r, "FOREIGN" ) );
      foreign.submit ( () -> channel.emit ( 1L ) ).get ();
      foreign.shutdown ();
      home.await ();

      assertEquals ( Set.of ( cortex.name ( "thread.FOREIGN" ) ),
        seen.stream ().collect ( Collectors.toUnmodifiableSet () ) );
    } finally {
      home.close ();
    }
  }

  @Test
  @DisplayName ( "a cross-circuit emission is attributed to the emitting circuit" )
  void crossCircuitEmissionIsAttributedToTheEmitter () {

    final Cortex cortex = cortex ();
    final Circuit home  = cortex.circuit ( cortex.name ( "home" ) );
    final Circuit away  = cortex.circuit ( cortex.name ( "away" ) );

    try {
      final var seen = new ConcurrentLinkedQueue < Name > ();
      final Sink < Long > sink = home.sink (
        home.pipe ( ( Capture < Long > c ) -> seen.add ( c.current ().name () ) ) );
      final Pipe < Long > channel = sink.get ( cortex.name ( "ch" ) );

      away.pipe ( ( Long v ) -> channel.emit ( v ) ).emit ( 1L );
      away.await ();
      home.await ();

      assertEquals ( Set.of ( cortex.name ( "away" ) ),
        seen.stream ().collect ( Collectors.toUnmodifiableSet () ) );
    } finally {
      home.close ();
      away.close ();
    }
  }
}
