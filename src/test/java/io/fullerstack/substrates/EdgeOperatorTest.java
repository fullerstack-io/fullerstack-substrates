package io.fullerstack.substrates;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.humainary.substrates.api.Substrates;
import io.humainary.substrates.api.Substrates.Receptor;

/// Regression tests for `Fiber.edge(initial, biPredicate)` and the distinction
/// from `Fiber.guard(initial, biPredicate)`.
///
/// These differ in how `prev` advances (API §6.2.3):
/// - **edge** — "transition between *consecutive* values": `prev` advances on
///   **every** input, so the predicate always sees the immediately preceding value.
/// - **guard** — "on pass, `prev` advances to `curr`": `prev` advances **only on
///   pass**.
///
/// `edge` was previously implemented by reusing `guard`'s operator, so a
/// transition predicate that never passes early (e.g. `prev >= 5 && curr < 5`)
/// left `prev` pinned at the seed and never fired. The pair below locks the fix.
final class EdgeOperatorTest {

  private static List < Integer > collect (
      java.util.function.Function < Substrates.Fiber < Integer >, Substrates.Fiber < Integer > > op,
      int... inputs ) {

    final var cortex  = Substrates.cortex ();
    final var circuit = cortex.circuit ();
    try {
      final List < Integer > out = new ArrayList <> ();
      final var target = circuit.pipe ( ( Receptor < Integer > ) out::add );
      final var source = op.apply ( cortex.fiber ( Integer.class ) ).pipe ( target );
      for ( int v : inputs ) source.emit ( v );
      circuit.await ();
      return out;
    } finally {
      circuit.close ();
    }
  }

  @Test
  @DisplayName ( "edge advances prev on every input and fires the transition" )
  void edgeFiresOnConsecutiveTransition () {
    // prev climbs 0→1→9→9→9, then the 9→2 transition satisfies prev>=5 && curr<5.
    final var out = collect (
      f -> f.edge ( 0, ( prev, curr ) -> prev >= 5 && curr < 5 ),
      1, 9, 9, 9, 2 );
    assertThat ( out ).containsExactly ( 2 );
  }

  @Test
  @DisplayName ( "edge generalizes diff" )
  void edgeGeneralizesDiff () {
    final var out = collect (
      f -> f.edge ( 0, ( prev, curr ) -> !prev.equals ( curr ) ),
      1, 1, 2, 2, 3 );
    assertThat ( out ).containsExactly ( 1, 2, 3 );
  }

  @Test
  @DisplayName ( "guard advances prev only on pass (distinct from edge)" )
  void guardAdvancesOnlyOnPass () {
    // Same predicate/inputs as the edge test: guard never passes (prev pinned at
    // the seed 0, which is never >= 5), so it emits nothing — by contract.
    final var out = collect (
      f -> f.guard ( 0, ( prev, curr ) -> prev >= 5 && curr < 5 ),
      1, 9, 9, 9, 2 );
    assertThat ( out ).isEmpty ();
  }

  @Test
  @DisplayName ( "guard passes when the pairwise predicate holds, advancing on pass" )
  void guardPassesOnHold () {
    // Strictly-increasing filter: prev = last *passed* value.
    final var out = collect (
      f -> f.guard ( 0, ( prev, curr ) -> curr > prev ),
      1, 3, 2, 5, 4 );
    assertThat ( out ).containsExactly ( 1, 3, 5 );
  }
}
