package io.fullerstack.substrates;

import io.humainary.substrates.api.Substrates.Circuit;
import io.humainary.substrates.api.Substrates.Cortex;
import io.humainary.substrates.api.Substrates.Pipe;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.IntStream;

import static io.humainary.substrates.api.Substrates.cortex;
import static org.junit.jupiter.api.Assertions.assertEquals;

/// The transit ring starts at 8 slots and doubles when full. The growth runs **inside** a drain —
/// a receptor that emits is adding to the same ring the drain is walking — so the copy has to
/// relocate the unread span and rebase both cursors without losing or reordering anything.
///
/// Nothing else exercises this: the ring is sized so that "simultaneous entries ≈ 1" for ordinary
/// cascades, and a benchmark suite that never enters the state cannot measure it. These are the
/// states it never enters.
@DisplayName ( "Transit ring capacity" )
final class TransitCapacityTest {

  private Cortex  cortex;
  private Circuit circuit;

  @BeforeEach
  void setUp () {
    cortex  = cortex ();
    circuit = cortex.circuit ();
  }

  @AfterEach
  void tearDown () {
    circuit.close ();
  }

  /// One receptor emits `fanOut` values into a collector, from inside a cascade. Every value
  /// beyond the eighth forces the ring to grow mid-drain.
  private List < Integer > cascadeOf ( int fanOut ) {

    final var collected = new ConcurrentLinkedQueue < Integer > ();
    final Pipe < Integer > collector = circuit.pipe ( ( Integer v ) -> collected.add ( v ) );

    circuit.pipe ( ( Integer ignored ) -> {
      for ( int i = 0; i < fanOut; i++ ) collector.emit ( i );
    } ).emit ( 0 );

    circuit.await ();
    return List.copyOf ( collected );
  }

  @Test
  @DisplayName ( "a cascade that fits the initial ring loses nothing" )
  void withinInitialCapacity () {
    assertEquals ( IntStream.range ( 0, 7 ).boxed ().toList (), cascadeOf ( 7 ) );
  }

  @Test
  @DisplayName ( "a cascade exactly at the initial capacity loses nothing" )
  void atInitialCapacity () {
    assertEquals ( IntStream.range ( 0, 8 ).boxed ().toList (), cascadeOf ( 8 ) );
  }

  @Test
  @DisplayName ( "a cascade one past capacity grows the ring in order" )
  void onePastCapacity () {
    assertEquals ( IntStream.range ( 0, 9 ).boxed ().toList (), cascadeOf ( 9 ) );
  }

  @Test
  @DisplayName ( "a cascade forcing several doublings stays gap-free and ordered" )
  void manyDoublings () {
    assertEquals ( IntStream.range ( 0, 5000 ).boxed ().toList (), cascadeOf ( 5000 ) );
  }

  @Test
  @DisplayName ( "the ring survives repeated grow-and-drain cycles" )
  void repeatedGrowth () {
    for ( int round = 0; round < 5; round++ ) {
      assertEquals ( IntStream.range ( 0, 200 ).boxed ().toList (), cascadeOf ( 200 ),
        "round " + round );
    }
  }

  @Test
  @DisplayName ( "siblings run before grandchildren — growth must not change cascade order" )
  void breadthFirstOrderSurvivesGrowth () {

    final var order = new ArrayList < String > ();
    final Pipe < String > leaf = circuit.pipe ( ( String s ) -> order.add ( s ) );

    // 12 children (past the 8-slot ring); each child emits one grandchild.
    final Pipe < Integer > child = circuit.pipe ( ( Integer i ) -> {
      order.add ( "child" + i );
      leaf.emit ( "grandchild" + i );
    } );

    circuit.pipe ( ( Integer ignored ) -> {
      for ( int i = 0; i < 12; i++ ) child.emit ( i );
    } ).emit ( 0 );
    circuit.await ();

    final List < String > expected = new ArrayList <> ();
    for ( int i = 0; i < 12; i++ ) expected.add ( "child" + i );
    for ( int i = 0; i < 12; i++ ) expected.add ( "grandchild" + i );
    assertEquals ( expected, order, "cascade is iterative: all siblings, then their children" );
  }
}
