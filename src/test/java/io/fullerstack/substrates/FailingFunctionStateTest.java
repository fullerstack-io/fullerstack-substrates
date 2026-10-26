package io.fullerstack.substrates;

import io.humainary.substrates.api.Substrates.Circuit;
import io.humainary.substrates.api.Substrates.Cortex;
import io.humainary.substrates.api.Substrates.Fiber;
import io.humainary.substrates.api.Substrates.Name;
import io.humainary.substrates.api.Substrates.Pipe;
import io.humainary.substrates.api.Substrates.State;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.humainary.substrates.api.Substrates.cortex;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/// 3.12.0 §6.2.3 *State across a failing function*: when an operator's function signals an error,
/// "the emission is dropped (§15.4), and the operator's state is left as it was before that input:
/// the failed input counts toward nothing, whether a batch, a run, a confirmation, or a latch" —
/// except that Relate's and Edge's previous input advance to it. And §8.1 / §16.3: a write whose
/// (name, type, value) matches the existing entry "MUST return the same state instance", the slot
/// keeping its position.
///
/// The 3.12.0 kit tests this rule only for `relate` and `scan`, and the newest-slot case of the
/// State rule, so every case here passed the kit while broken. Found by the 3.12.0 review.
@DisplayName ( "3.12.0: operator state across a failing function, and same-instance State writes" )
final class FailingFunctionStateTest {

  private static List < Integer > run ( final Fiber < Integer > fiber, final int... inputs ) {
    final Cortex cortex = cortex ();
    final Circuit circuit = cortex.circuit ( cortex.name ( "failing-function" ) );
    try {
      final List < Integer > out = new CopyOnWriteArrayList <> ();
      final Pipe < Integer > pipe = fiber.pipe ( circuit.pipe ( out::add ) );
      for ( final int v : inputs ) pipe.emit ( v );
      circuit.await ();
      return List.copyOf ( out );
    } finally {
      circuit.close ();
    }
  }

  @Test
  @DisplayName ( "edge: prev advances to an input whose predicate throws" )
  void edgeAdvancesPreviousInputAcrossAThrow () {
    // (0,1) passes; (1,2) throws, prev → 2; (2,3) passes only if prev advanced (3 - 2 == 1).
    final List < Integer > out = run (
      cortex ().fiber ( Integer.class ).edge ( 0, ( prev, curr ) -> {
        if ( curr == 2 ) throw new ArithmeticException ( "trigger" );
        return curr - prev == 1;
      } ),
      1, 2, 3 );
    assertEquals ( List.of ( 1, 3 ), out );
  }

  @Test
  @DisplayName ( "tumble: an input whose combiner throws is not counted toward the batch" )
  void tumbleDoesNotCountAFailedInput () {
    // size 3, sum. 2 throws and counts toward nothing: the batch is 1, 3, 4 → 8.
    final List < Integer > out = run (
      cortex ().fiber ( Integer.class ).tumble ( 3, ( acc, v ) -> {
        if ( v == 2 ) throw new ArithmeticException ( "trigger" );
        return acc + v;
      }, 0 ),
      1, 2, 3, 4 );
    assertEquals ( List.of ( 8 ), out );
  }

  @Test
  @DisplayName ( "tumble: a throw on what would be the last input does not discard the batch" )
  void tumbleKeepsTheBatchWhenTheLastInputThrows () {
    // size 2. 1 is combined; 5 throws (not counted); 2 completes the batch → 3.
    final List < Integer > out = run (
      cortex ().fiber ( Integer.class ).tumble ( 2, ( acc, v ) -> {
        if ( v == 5 ) throw new ArithmeticException ( "trigger" );
        return acc + v;
      }, 0 ),
      1, 5, 2 );
    assertEquals ( List.of ( 3 ), out );
  }

  @Test
  @DisplayName ( "integrate: a fire predicate that throws leaves the accumulator as it was" )
  void integrateKeepsStateWhenFireThrows () {
    // Sum, fire at >= 10. Input 4 makes 4 + 4 = 8 and the predicate throws on 8: state stays 4.
    // Then 6 → 10 fires. Had the state moved to 8, 6 would make 14.
    final List < Integer > out = run (
      cortex ().fiber ( Integer.class ).integrate ( 0, Integer::sum, s -> {
        if ( s == 8 ) throw new ArithmeticException ( "trigger" );
        return s >= 10;
      } ),
      4, 4, 6 );
    assertEquals ( List.of ( 10 ), out );
  }

  @Test
  @DisplayName ( "State: an equal write to an older slot returns the same instance" )
  void equalWriteToAnOlderSlotReturnsTheSameInstance () {
    final Cortex cortex = cortex ();
    final Name a = cortex.name ( "a" );
    final Name b = cortex.name ( "b" );
    final State state = cortex.state ().state ( a, 1 ).state ( b, 2 );   // b newest, a older
    assertSame ( state, state.state ( a, 1 ), "equal write to the older slot must return the same instance" );
  }

}
