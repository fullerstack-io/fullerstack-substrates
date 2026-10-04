package io.fullerstack.substrates;

import io.humainary.substrates.api.Substrates.Circuit;
import io.humainary.substrates.api.Substrates.Cortex;
import io.humainary.substrates.api.Substrates.Pipe;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.humainary.substrates.api.Substrates.cortex;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// §5.1 (3.6.0): "`chance` draws from an implementation-defined random source, so replay reproduces
/// its decisions only where the implementation documents a way to seed that source". This provider's
/// way is `-Dio.fullerstack.substrates.seed`; these tests hold it to that. The replay cases run a
/// fresh JVM per run, because the seed and the construction ordinal are fixed per JVM — the same
/// process cannot replay itself from the start.
@DisplayName ( "chance is reproducible from its seed" )
final class ChanceReplayTest {

  private static final int EMISSIONS = 2_000;

  @Test
  @DisplayName ( "the same seed and ordinal give the same decisions, and the rate is p" )
  void decisionsAreAFunctionOfSeedAndOrdinal () {
    final List < Integer > a = new ArrayList <> ();
    final List < Integer > b = new ArrayList <> ();
    final List < Integer > c = new ArrayList <> ();
    final FsOperators.Chance < Integer > first  = new FsOperators.Chance <> ( 0.3, a::add, 7 );
    final FsOperators.Chance < Integer > second = new FsOperators.Chance <> ( 0.3, b::add, 7 );
    final FsOperators.Chance < Integer > other  = new FsOperators.Chance <> ( 0.3, c::add, 8 );
    for ( int i = 0; i < EMISSIONS; i++ ) {
      first.accept ( i );
      second.accept ( i );
      other.accept ( i );
    }
    assertEquals ( a, b, "same seed and ordinal" );
    assertNotEquals ( a, c, "a different ordinal is a different stream" );
    // 0.3 × 2000 = 600, σ ≈ 20.5; ±5σ keeps this deterministic test meaningful without flaking.
    assertTrue ( Math.abs ( a.size () - 600 ) < 103, "pass rate near p, was " + a.size () );
  }

  @Test
  @DisplayName ( "a run replays exactly under its seed, and not under another" )
  void aRunReplaysUnderItsSeed () throws Exception {
    final String run1 = run ( "42" );
    final String run2 = run ( "42" );
    final String run3 = run ( "43" );
    assertEquals ( run1, run2, "same seed, same program: same decisions" );
    assertNotEquals ( run1, run3, "a different seed draws differently" );
  }

  @Test
  @DisplayName ( "an unseeded run publishes the seed that replays it" )
  void anUnseededRunPublishesItsSeed () throws Exception {
    final String unseeded  = run ( null );
    final String published = unseeded.substring ( 0, unseeded.indexOf ( '|' ) );
    assertEquals ( unseeded, run ( published ), "replaying with the published seed" );
  }

  /// Runs [Program] in a fresh JVM and returns "seed|passed values".
  private static String run ( final String seed ) throws IOException, InterruptedException {
    final Path java = Paths.get ( System.getProperty ( "java.home" ), "bin", "java" );
    final List < String > cmd = new ArrayList <> ( List.of ( java.toString (), "--enable-preview" ) );
    if ( seed != null ) cmd.add ( "-D" + FsOperators.Chance.SEED_PROPERTY + "=" + seed );
    cmd.addAll ( List.of ( "-cp", System.getProperty ( "java.class.path" ), Program.class.getName () ) );
    final Process process = new ProcessBuilder ( cmd ).redirectErrorStream ( true ).start ();
    final String out = new String ( process.getInputStream ().readAllBytes (), StandardCharsets.UTF_8 ).trim ();
    assertTrue ( process.waitFor ( 60, TimeUnit.SECONDS ), "child JVM finished" );
    assertEquals ( 0, process.exitValue (), "child JVM failed: " + out );
    return out;
  }

  /// A circuit with two `chance` fibers on two pipes, fed the same values. Prints the seed it ran
  /// under and every value each fiber passed, in delivery order.
  static final class Program {

    public static void main ( final String[] args ) {
      final Cortex cortex = cortex ();
      final Circuit circuit = cortex.circuit ();
      final List < String > passed = new ArrayList <> ();
      final Pipe < Integer > left  = cortex.< Integer > fiber ().chance ( 0.5 ).pipe ( circuit.pipe ( v -> passed.add ( "L" + v ) ) );
      final Pipe < Integer > right = cortex.< Integer > fiber ().chance ( 0.25 ).pipe ( circuit.pipe ( v -> passed.add ( "R" + v ) ) );
      for ( int i = 0; i < EMISSIONS; i++ ) {
        left.emit ( i );
        right.emit ( i );
      }
      circuit.await ();
      circuit.close ();
      System.out.println ( System.getProperty ( FsOperators.Chance.SEED_PROPERTY ) + "|" + String.join ( ",", passed ) );
    }
  }
}
