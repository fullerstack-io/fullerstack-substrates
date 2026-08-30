package io.fullerstack.substrates;

import io.humainary.substrates.api.Substrates.Capture;
import io.humainary.substrates.api.Substrates.Circuit;
import io.humainary.substrates.api.Substrates.Cortex;
import io.humainary.substrates.api.Substrates.Name;
import io.humainary.substrates.api.Substrates.Pipe;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static io.humainary.substrates.api.Substrates.cortex;
import static org.junit.jupiter.api.Assertions.assertEquals;

/// `Flow.pipe(Pipe)` and `Fiber.pipe(Pipe)` both promise their stages run "on that pipe's
/// circuit", and their Threading Guarantee is what makes a `scan` slot or a `window` ring safe
/// without synchronization.
///
/// The guarantee held for every target shape except one: a target that is not an [FsPipe] — a
/// sink channel is the only such pipe this provider mints — fell to a branch that ran the whole
/// chain inline on the calling thread. Stateful operators were mutated off-worker, and the TCK
/// passed throughout.
@DisplayName ( "Flow and Fiber stages run on the target pipe's circuit" )
final class FlowFiberAttachmentTest {

  private Cortex  cortex;
  private Circuit home;
  private Circuit away;

  @BeforeEach
  void setUp () {
    cortex = cortex ();
    home   = cortex.circuit ( cortex.name ( "home" ) );
    away   = cortex.circuit ( cortex.name ( "away" ) );
  }

  @AfterEach
  void tearDown () {
    home.close ();
    away.close ();
  }

  /// Emits into `attach(target)` from a foreign thread and returns the thread each stage ran on.
  private List < String > stageThreads ( Circuit owner,
                                         Function < Pipe < Integer >, Pipe < Integer > > attach,
                                         Pipe < Integer > target ) {

    final var threads = new ConcurrentLinkedQueue < String > ();
    final Pipe < Integer > in = attach.apply ( target );

    final ExecutorService foreign = Executors.newSingleThreadExecutor ( r -> new Thread ( r, "FOREIGN" ) );
    try {
      foreign.submit ( () -> in.emit ( 1 ) ).get ();
    } catch ( Exception e ) {
      throw new AssertionError ( e );
    } finally {
      foreign.shutdown ();
    }

    home.await ();
    away.await ();
    return List.copyOf ( threads );
  }

  private void assertRunsOn ( Circuit owner, Pipe < Integer > target ) {

    final var flowThreads  = new ConcurrentLinkedQueue < String > ();
    final var fiberThreads = new ConcurrentLinkedQueue < String > ();

    final Pipe < Integer > viaFlow = cortex.< Integer >flow ()
      .map ( v -> { flowThreads.add ( Thread.currentThread ().getName () ); return v; } )
      .pipe ( target );

    final Pipe < Integer > viaFiber = cortex.< Integer >fiber ()
      .peek ( v -> fiberThreads.add ( Thread.currentThread ().getName () ) )
      .pipe ( target );

    final ExecutorService foreign = Executors.newSingleThreadExecutor ( r -> new Thread ( r, "FOREIGN" ) );
    try {
      foreign.submit ( () -> { viaFlow.emit ( 1 ); viaFiber.emit ( 2 ); } ).get ();
    } catch ( Exception e ) {
      throw new AssertionError ( e );
    } finally {
      foreign.shutdown ();
    }

    home.await ();
    away.await ();

    final String expected = "circuit-" + owner.subject ().name ();
    assertEquals ( List.of ( expected ), List.copyOf ( flowThreads ),  "flow stage thread" );
    assertEquals ( List.of ( expected ), List.copyOf ( fiberThreads ), "fiber stage thread" );
  }

  @Test
  @DisplayName ( "target is a circuit receptor pipe" )
  void circuitPipeTarget () {
    assertRunsOn ( home, home.pipe ( ( Integer v ) -> { } ) );
  }

  @Test
  @DisplayName ( "target is a conduit channel" )
  void conduitChannelTarget () {
    assertRunsOn ( home, home.< Integer >conduit ().get ( cortex.name ( "c" ) ) );
  }

  @Test
  @DisplayName ( "target is a sink channel — the pipe that is not an FsPipe" )
  void sinkChannelTarget () {
    final Name s = cortex.name ( "s" );
    assertRunsOn ( home,
      home.sink ( home.pipe ( ( Capture < Integer > c ) -> { } ) ).get ( s ) );
  }

  @Test
  @DisplayName ( "target is a fan-out pipe" )
  void fanOutTarget () {
    assertRunsOn ( home, home.pipe ( List.of ( home.pipe ( ( Integer v ) -> { } ) ) ) );
  }

  @Test
  @DisplayName ( "target belongs to another circuit" )
  void crossCircuitTarget () {
    assertRunsOn ( away, away.pipe ( ( Integer v ) -> { } ) );
  }

  @Test
  @DisplayName ( "target is a sink channel on another circuit" )
  void crossCircuitSinkChannelTarget () {
    assertRunsOn ( away,
      away.sink ( away.pipe ( ( Capture < Integer > c ) -> { } ) ).get ( cortex.name ( "s2" ) ) );
  }
}
