// Copyright (c) 2025 William David Louth

package io.humainary.substrates.jmh;

import io.humainary.substrates.api.Substrates;
import org.openjdk.jmh.annotations.*;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.openjdk.jmh.annotations.Level.Iteration;
import static org.openjdk.jmh.annotations.Level.Trial;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

///
/// Benchmark for the 3.0 source-mirroring composition — the replacement for the
/// removed `Tap`.
///
/// A source is mirrored into a second conduit through a pool-backed subscriber:
///
/// ```java
/// source.subscribe ( circuit.subscriber ( name, mirror.pool ( flow ) ) )
/// ```
///
/// Measures:
/// 1. Bridge creation — subscriber + subscribe overhead (the former tap creation)
/// 2. Emission through an identity mirror vs the plain-conduit baseline
/// 3. Emission through a transforming (Integer → String) mirror
/// 4. Fan-out through two mirrors on the same source
///

@State ( Scope.Benchmark )
@BenchmarkMode ( AverageTime )
@OutputTimeUnit ( NANOSECONDS )
@Fork ( 1 )
@Warmup ( iterations = 3, time = 1 )
@Measurement ( iterations = 5, time = 1 )

public class MirrorOps
  implements Substrates {

  private static final String NAME_STR   = "test";
  private static final int    VALUE      = 42;
  private static final int    BATCH_SIZE = 1000;

  private Cortex  cortex;
  private Name    name;
  private Circuit circuit;

  private Flow < Integer, String > intToString;

  // Bridge-creation benchmarks: fresh source + mirror each invocation is too
  // heavy; we measure the subscriber+subscribe bridge on a standing pair.
  private Conduit < Integer > conduit;
  private Conduit < Integer > bridgeMirror;

  // Baseline: conduit → 1 direct subscriber (no mirror in path)
  private Pipe < Integer > baselinePipe;

  // Identity mirror: source → pool-backed subscriber → mirror → 1 subscriber
  private Pipe < Integer > identityPipe;

  // String mirror: source → flow(pool) subscriber → mirror<String> → 1 subscriber
  private Pipe < Integer > stringPipe;

  // Multi-mirror: source → 2 identity mirrors → 2 subscribers (fan-out)
  private Pipe < Integer > multiPipe;

  //
  // BRIDGE CREATION BENCHMARKS (former tap creation)
  //

  ///
  /// Identity bridge creation — subscriber minting plus source subscription.
  ///

  @Benchmark
  public Subscription mirror_bridge_create_identity () {

    return
      conduit.subscribe (
        circuit.subscriber (
          name,
          bridgeMirror.pool ( p -> p )
        )
      );

  }

  ///
  /// Batched bridge creation — amortized overhead.
  ///

  @Benchmark
  @OperationsPerInvocation ( BATCH_SIZE )
  public Subscription mirror_bridge_create_batch () {

    Subscription result = null;

    for ( int i = 0; i < BATCH_SIZE; i++ ) {
      result =
        conduit.subscribe (
          circuit.subscriber (
            name,
            bridgeMirror.pool ( p -> p )
          )
        );
    }

    return result;

  }

  //
  // BASELINE EMISSION BENCHMARKS
  //

  ///
  /// Baseline emission through conduit without a mirror.
  ///

  @Benchmark
  @OperationsPerInvocation ( BATCH_SIZE )
  public void baseline_emit_batch_await () {

    for ( int i = 0; i < BATCH_SIZE; i++ ) {
      baselinePipe.emit ( VALUE + i );
    }

    circuit.await ();

  }

  //
  // MIRRORED EMISSION BENCHMARKS
  //

  ///
  /// Emission through an identity mirror (no transformation cost).
  ///

  @Benchmark
  @OperationsPerInvocation ( BATCH_SIZE )
  public void mirror_emit_identity_batch_await () {

    for ( int i = 0; i < BATCH_SIZE; i++ ) {
      identityPipe.emit ( VALUE + i );
    }

    circuit.await ();

  }

  ///
  /// Emission through a transforming (allocating) Integer → String mirror.
  ///

  @Benchmark
  @OperationsPerInvocation ( BATCH_SIZE )
  public void mirror_emit_string_batch_await () {

    for ( int i = 0; i < BATCH_SIZE; i++ ) {
      stringPipe.emit ( VALUE + i );
    }

    circuit.await ();

  }

  ///
  /// Single emission through the identity mirror.
  ///

  @Benchmark
  public void mirror_emit_identity_single () {

    identityPipe.emit ( VALUE );

  }

  ///
  /// Single emission through the identity mirror with await.
  ///

  @Benchmark
  public void mirror_emit_identity_single_await () {

    identityPipe.emit ( VALUE );
    circuit.await ();

  }

  ///
  /// Emission fanning out through two mirrors on the same source.
  ///

  @Benchmark
  @OperationsPerInvocation ( BATCH_SIZE )
  public void mirror_emit_multi_batch_await () {

    for ( int i = 0; i < BATCH_SIZE; i++ ) {
      multiPipe.emit ( VALUE + i );
    }

    circuit.await ();

  }

  ///
  /// Bridge lifecycle — create, mirror one emission, close the subscription.
  ///

  @Benchmark
  public void mirror_bridge_lifecycle () {

    final var mirror =
      circuit.< String >conduit ();

    mirror.subscribe (
      circuit.subscriber (
        cortex.name ( "lifecycle-sub" ),
        ( _, registrar ) ->
          registrar.register ( Receptor.of ( String.class ) )
      )
    );

    final var subscription =
      conduit.subscribe (
        circuit.subscriber (
          name,
          mirror.pool ( intToString )
        )
      );

    conduit.get ( name ).emit ( VALUE );
    circuit.await ();

    subscription.close ();
    mirror.close ();
    circuit.await ();

  }

  @Setup ( Iteration )
  public void setupIteration () {

    circuit =
      cortex.circuit (
        name
      );

    conduit =
      circuit.conduit (
        Integer.class
      );

    bridgeMirror =
      circuit.conduit (
        Integer.class
      );

    // ─────────────────────────────────────────────────────────────────
    // BASELINE: conduit → 1 direct subscriber (no mirror in path)
    // ─────────────────────────────────────────────────────────────────
    final Conduit < Integer > baselineConduit =
      circuit.conduit (
        Integer.class
      );

    baselineConduit.subscribe (
      circuit.subscriber (
        cortex.name ( "baseline-sub" ),
        ( _, registrar ) ->
          registrar.register ( Receptor.of ( Integer.class ) )
      )
    );

    baselinePipe =
      baselineConduit.get ( name );

    // ─────────────────────────────────────────────────────────────────
    // IDENTITY MIRROR: source → pool-backed subscriber → mirror → sub
    // ─────────────────────────────────────────────────────────────────
    final Conduit < Integer > identitySource =
      circuit.conduit (
        Integer.class
      );

    final Conduit < Integer > identityMirror =
      circuit.conduit (
        Integer.class
      );

    identityMirror.subscribe (
      circuit.subscriber (
        cortex.name ( "identity-sub" ),
        ( _, registrar ) ->
          registrar.register ( Receptor.of ( Integer.class ) )
      )
    );

    identitySource.subscribe (
      circuit.subscriber (
        cortex.name ( "identity-bridge" ),
        identityMirror.pool ( p -> p )
      )
    );

    identityPipe =
      identitySource.get ( name );

    // ─────────────────────────────────────────────────────────────────
    // STRING MIRROR: source<Integer> → flow bridge → mirror<String> → sub
    // ─────────────────────────────────────────────────────────────────
    final Conduit < Integer > stringSource =
      circuit.conduit (
        Integer.class
      );

    final Conduit < String > stringMirror =
      circuit.conduit (
        String.class
      );

    stringMirror.subscribe (
      circuit.subscriber (
        cortex.name ( "string-sub" ),
        ( _, registrar ) ->
          registrar.register ( Receptor.of ( String.class ) )
      )
    );

    stringSource.subscribe (
      circuit.subscriber (
        cortex.name ( "string-bridge" ),
        stringMirror.pool ( intToString )
      )
    );

    stringPipe =
      stringSource.get ( name );

    // ─────────────────────────────────────────────────────────────────
    // MULTI-MIRROR: source → 2 identity mirrors → 2 subscribers
    // ─────────────────────────────────────────────────────────────────
    final Conduit < Integer > multiSource =
      circuit.conduit (
        Integer.class
      );

    for ( int m = 1; m <= 2; m++ ) {

      final Conduit < Integer > mirror =
        circuit.conduit (
          Integer.class
        );

      mirror.subscribe (
        circuit.subscriber (
          cortex.name ( "multi-sub-" + m ),
          ( _, registrar ) ->
            registrar.register ( Receptor.of ( Integer.class ) )
        )
      );

      multiSource.subscribe (
        circuit.subscriber (
          cortex.name ( "multi-bridge-" + m ),
          mirror.pool ( p -> p )
        )
      );

    }

    multiPipe =
      multiSource.get ( name );

    // Warm up - ensure all subscriptions are registered
    circuit.await ();

  }

  @Setup ( Trial )
  public void setupTrial () {

    cortex =
      Substrates.cortex ();

    name =
      cortex.name (
        NAME_STR
      );

    intToString =
      cortex.flow ( Integer.class ).map ( Object::toString );

  }

  @TearDown ( Iteration )
  public void tearDownIteration () {

    circuit.close ();

  }

}
