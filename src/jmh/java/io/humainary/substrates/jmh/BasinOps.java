// Copyright (c) 2025 William David Louth

package io.humainary.substrates.jmh;

import io.humainary.substrates.api.Substrates;
import org.openjdk.jmh.annotations.*;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.openjdk.jmh.annotations.Level.Iteration;
import static org.openjdk.jmh.annotations.Level.Trial;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

///
/// Benchmark for the 3.0 `Basin` buffer — the replacement for the removed
/// source-bound `Reservoir`.
///
/// Capture-from-a-source is now the composition
///
/// ```java
/// Basin<Capture<E>> basin = circuit.basin ( capacity );
/// source.subscribe ( circuit.subscriber ( name, circuit.sink ( basin.pipe () ) ) );
/// basin.drain ( circuit.pipe ( receptor ) );
/// ```
///
/// Measures the hot-path impact of sink-capture into a basin during emission,
/// the queued drain, and the raw basin feed (direct `basin.pipe().emit`).
///

@State ( Scope.Benchmark )
@BenchmarkMode ( AverageTime )
@OutputTimeUnit ( NANOSECONDS )
@Fork ( 1 )
@Warmup ( iterations = 3, time = 1 )
@Measurement ( iterations = 5, time = 1 )

public class BasinOps
  implements Substrates {

  private static final String NAME_STR   = "test";
  private static final int    VALUE      = 42;
  private static final int    COUNT      = 100;
  private static final int    BATCH_SIZE = 1000;

  private Cortex  cortex;
  private Circuit circuit;
  private Name    name;

  /// Wires a fresh source conduit whose emissions are sink-captured into a
  /// fresh basin, returning the emitting pipe alongside the basin.
  private record Captured ( Substrates.Pipe < Integer > pipe, Substrates.Basin < Substrates.Capture < Integer > > basin ) { }

  private Captured captured () {

    final var conduit =
      circuit.< Integer >conduit ();

    final Basin < Capture < Integer > > basin =
      circuit.basin ( COUNT );

    conduit.subscribe (
      circuit.subscriber (
        name,
        circuit.sink ( basin.pipe () )
      )
    );

    return
      new Captured (
        conduit.get ( name ),
        basin
      );

  }

  /// Queued drain into a counting receptor; returns the drained count once the
  /// circuit barrier guarantees the drain job has run.
  private long drainCount ( Basin < Capture < Integer > > basin ) {

    final long[] count = new long[1];

    basin.drain (
      circuit.pipe (
        _ -> count[0]++
      )
    );

    circuit.await ();

    return count[0];

  }

  ///
  /// BASELINE: 100 emissions without any capture (pure emission cost).
  ///

  @Benchmark
  @OperationsPerInvocation ( COUNT )
  public void baseline_emit_no_basin_await () {

    final var conduit =
      circuit.< Integer >conduit ();

    final var pipe =
      conduit.get ( name );

    for ( int i = 0; i < COUNT; i++ ) {
      pipe.emit ( VALUE + i );
    }

    circuit.await ();

  }

  ///
  /// BATCHED BASELINE: 1000 emissions without any capture.
  ///

  @Benchmark
  @OperationsPerInvocation ( BATCH_SIZE )
  public void baseline_emit_no_basin_await_batch () {

    final var conduit =
      circuit.< Integer >conduit ();

    final var pipe =
      conduit.get ( name );

    for ( int i = 0; i < BATCH_SIZE; i++ ) {
      pipe.emit ( VALUE + i );
    }

    circuit.await ();

  }

  ///
  /// HOT PATH: 100 emissions with a sink capturing into a basin.
  ///

  @Benchmark
  @OperationsPerInvocation ( COUNT )
  public void basin_emit_with_capture_await () {

    final var wired = captured ();

    for ( int i = 0; i < COUNT; i++ ) {
      wired.pipe.emit ( VALUE + i );
    }

    circuit.await ();

  }

  ///
  /// BATCHED HOT PATH: 1000 emissions with a sink capturing into a basin
  /// (basin capacity 100 — eviction in play, as with the bounded reservoir).
  ///

  @Benchmark
  @OperationsPerInvocation ( BATCH_SIZE )
  public void basin_emit_with_capture_await_batch () {

    final var wired = captured ();

    for ( int i = 0; i < BATCH_SIZE; i++ ) {
      wired.pipe.emit ( VALUE + i );
    }

    circuit.await ();

  }

  ///
  /// PATTERN: 100 emissions burst, then single queued drain.
  ///

  @Benchmark
  @OperationsPerInvocation ( COUNT )
  public long basin_burst_then_drain_await () {

    final var wired = captured ();

    for ( int i = 0; i < COUNT; i++ ) {
      wired.pipe.emit ( VALUE + i );
    }

    circuit.await ();

    return drainCount ( wired.basin );

  }

  ///
  /// BATCHED: 1000 emissions burst (capacity 100), then single queued drain.
  ///

  @Benchmark
  @OperationsPerInvocation ( BATCH_SIZE )
  public long basin_burst_then_drain_await_batch () {

    final var wired = captured ();

    for ( int i = 0; i < BATCH_SIZE; i++ ) {
      wired.pipe.emit ( VALUE + i );
    }

    circuit.await ();

    return drainCount ( wired.basin );

  }

  ///
  /// PATTERN: 5 cycles of (20 emissions + drain).
  ///

  @Benchmark
  @OperationsPerInvocation ( COUNT )
  public long basin_emit_drain_cycles_await () {

    final var wired = captured ();

    long total = 0;

    for ( int cycle = 0; cycle < 5; cycle++ ) {

      for ( int i = 0; i < 20; i++ ) {
        wired.pipe.emit ( VALUE + i );
      }

      circuit.await ();

      total += drainCount ( wired.basin );

    }

    return total;

  }

  ///
  /// PROCESS: 100 emissions + drain that sums the captured values.
  ///

  @Benchmark
  @OperationsPerInvocation ( COUNT )
  public int basin_process_emissions_await () {

    final var wired = captured ();

    for ( int i = 0; i < COUNT; i++ ) {
      wired.pipe.emit ( VALUE + i );
    }

    circuit.await ();

    final int[] sum = new int[1];

    wired.basin.drain (
      circuit.pipe (
        ( Capture < Integer > c ) -> sum[0] += c.emission ()
      )
    );

    circuit.await ();

    return sum[0];

  }

  ///
  /// RAW FEED: 100 direct emissions into the basin's own pipe (no sink, no
  /// capture minting) — the floor cost of the buffer itself.
  ///

  @Benchmark
  @OperationsPerInvocation ( COUNT )
  public long basin_raw_feed_drain_await () {

    final Basin < Integer > basin =
      circuit.basin ( COUNT );

    final var feed =
      basin.pipe ();

    for ( int i = 0; i < COUNT; i++ ) {
      feed.emit ( VALUE + i );
    }

    final long[] count = new long[1];

    basin.drain (
      circuit.pipe (
        _ -> count[0]++
      )
    );

    circuit.await ();

    return count[0];

  }

  @Setup ( Iteration )
  public void setupIteration () {

    circuit =
      cortex.circuit (
        name
      );

  }

  @Setup ( Trial )
  public void setupTrial () {

    cortex =
      Substrates.cortex ();

    name =
      cortex.name (
        NAME_STR
      );

  }

  @TearDown ( Iteration )
  public void tearDownIteration () {

    circuit.close ();

  }

}
