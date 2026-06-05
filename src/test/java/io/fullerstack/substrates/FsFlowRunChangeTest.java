package io.fullerstack.substrates;

import io.humainary.substrates.api.Substrates;
import io.humainary.substrates.api.Substrates.Change;
import io.humainary.substrates.api.Substrates.Run;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/// 3.0 Flow.run() / Flow.change() — run-length operators.
///
/// `run()` emits a Run per admission carrying value + consecutive-run length;
/// `change()` emits a Change only at a run boundary with the closed run's value
/// and terminal length plus the value opening the next run. Spec §4222-4655.
class FsFlowRunChangeTest {

  // ── run() ────────────────────────────────────────────────────────────────

  @Test
  @DisplayName ( "run() — first admission emits with length=1" )
  void runFirstAdmissionHasLengthOne () {
    final List < Run < Integer > > out = new ArrayList <> ();
    final var cortex  = Substrates.cortex ();
    final var circuit = cortex.circuit ();
    try {
      final var flow = cortex.< Integer > flow ( Integer.class ).run ();
      final var sink = circuit.< Run < Integer > > pipe ( out::add );
      final var pipe = flow.pipe ( sink );
      pipe.emit ( 7 );
      circuit.await ();
      assertThat ( out ).hasSize ( 1 );
      assertThat ( out.get ( 0 ).emission () ).isEqualTo ( 7 );
      assertThat ( out.get ( 0 ).length () ).isEqualTo ( 1L );
    } finally {
      circuit.close ();
    }
  }

  @Test
  @DisplayName ( "run() — consecutive equals increment length" )
  void runConsecutiveIncrementsLength () {
    final List < Run < String > > out = new ArrayList <> ();
    final var cortex  = Substrates.cortex ();
    final var circuit = cortex.circuit ();
    try {
      final var flow = cortex.< String > flow ( String.class ).run ();
      final var sink = circuit.< Run < String > > pipe ( out::add );
      final var pipe = flow.pipe ( sink );
      pipe.emit ( "A" );
      pipe.emit ( "A" );
      pipe.emit ( "A" );
      circuit.await ();
      assertThat ( out ).hasSize ( 3 );
      assertThat ( out.get ( 0 ).length () ).isEqualTo ( 1L );
      assertThat ( out.get ( 1 ).length () ).isEqualTo ( 2L );
      assertThat ( out.get ( 2 ).length () ).isEqualTo ( 3L );
      assertThat ( out ).allMatch ( r -> "A".equals ( r.emission () ) );
    } finally {
      circuit.close ();
    }
  }

  @Test
  @DisplayName ( "run() — change resets length to 1" )
  void runChangeResetsLength () {
    final List < Run < Integer > > out = new ArrayList <> ();
    final var cortex  = Substrates.cortex ();
    final var circuit = cortex.circuit ();
    try {
      final var flow = cortex.< Integer > flow ( Integer.class ).run ();
      final var sink = circuit.< Run < Integer > > pipe ( out::add );
      final var pipe = flow.pipe ( sink );
      pipe.emit ( 1 );  // (1, 1)
      pipe.emit ( 1 );  // (1, 2)
      pipe.emit ( 2 );  // (2, 1) — change
      pipe.emit ( 2 );  // (2, 2)
      pipe.emit ( 2 );  // (2, 3)
      pipe.emit ( 1 );  // (1, 1) — return
      circuit.await ();
      assertThat ( out ).extracting ( r -> r.emission () + "x" + r.length () )
        .containsExactly ( "1x1", "1x2", "2x1", "2x2", "2x3", "1x1" );
    } finally {
      circuit.close ();
    }
  }

  @Test
  @DisplayName ( "run() — value equality is Objects.equals, not identity" )
  void runUsesObjectsEquals () {
    final List < Run < String > > out = new ArrayList <> ();
    final var cortex  = Substrates.cortex ();
    final var circuit = cortex.circuit ();
    try {
      final var flow = cortex.< String > flow ( String.class ).run ();
      final var sink = circuit.< Run < String > > pipe ( out::add );
      final var pipe = flow.pipe ( sink );
      // Different instances, value-equal.
      pipe.emit ( new String ( "hi" ) );
      pipe.emit ( new String ( "hi" ) );
      circuit.await ();
      assertThat ( out ).hasSize ( 2 );
      assertThat ( out.get ( 1 ).length () ).isEqualTo ( 2L );
    } finally {
      circuit.close ();
    }
  }

  // ── change() ─────────────────────────────────────────────────────────────

  @Test
  @DisplayName ( "change() — first admission emits nothing" )
  void changeFirstAdmissionEmitsNothing () {
    final List < Change < Integer > > out = new ArrayList <> ();
    final var cortex  = Substrates.cortex ();
    final var circuit = cortex.circuit ();
    try {
      final var flow = cortex.< Integer > flow ( Integer.class ).change ();
      final var sink = circuit.< Change < Integer > > pipe ( out::add );
      final var pipe = flow.pipe ( sink );
      pipe.emit ( 42 );
      circuit.await ();
      assertThat ( out ).isEmpty ();
    } finally {
      circuit.close ();
    }
  }

  @Test
  @DisplayName ( "change() — consecutive equals emit nothing" )
  void changeConsecutiveEqualsEmitNothing () {
    final List < Change < String > > out = new ArrayList <> ();
    final var cortex  = Substrates.cortex ();
    final var circuit = cortex.circuit ();
    try {
      final var flow = cortex.< String > flow ( String.class ).change ();
      final var sink = circuit.< Change < String > > pipe ( out::add );
      final var pipe = flow.pipe ( sink );
      pipe.emit ( "X" );
      pipe.emit ( "X" );
      pipe.emit ( "X" );
      circuit.await ();
      assertThat ( out ).isEmpty ();
    } finally {
      circuit.close ();
    }
  }

  @Test
  @DisplayName ( "change() — boundary emits closed run's value, length, and new value" )
  void changeBoundaryReportsClosedRun () {
    final List < Change < Integer > > out = new ArrayList <> ();
    final var cortex  = Substrates.cortex ();
    final var circuit = cortex.circuit ();
    try {
      final var flow = cortex.< Integer > flow ( Integer.class ).change ();
      final var sink = circuit.< Change < Integer > > pipe ( out::add );
      final var pipe = flow.pipe ( sink );
      pipe.emit ( 1 );  // opens run-of-1, no emit
      pipe.emit ( 1 );  // still in run, no emit
      pipe.emit ( 1 );  // still in run, no emit
      pipe.emit ( 2 );  // boundary — emit (1, 2, length=3)
      circuit.await ();
      assertThat ( out ).hasSize ( 1 );
      Change < Integer > c = out.get ( 0 );
      assertThat ( c.from () ).isEqualTo ( 1 );
      assertThat ( c.to () ).isEqualTo ( 2 );
      assertThat ( c.length () ).isEqualTo ( 3L );
    } finally {
      circuit.close ();
    }
  }

  @Test
  @DisplayName ( "change() — multiple boundaries; trailing open run is not reported" )
  void changeMultipleBoundaries () {
    final List < Change < String > > out = new ArrayList <> ();
    final var cortex  = Substrates.cortex ();
    final var circuit = cortex.circuit ();
    try {
      final var flow = cortex.< String > flow ( String.class ).change ();
      final var sink = circuit.< Change < String > > pipe ( out::add );
      final var pipe = flow.pipe ( sink );
      pipe.emit ( "A" );  // open A-run
      pipe.emit ( "A" );  // A x2
      pipe.emit ( "B" );  // emit (A→B, len=2)
      pipe.emit ( "B" );  // B x2
      pipe.emit ( "B" );  // B x3
      pipe.emit ( "C" );  // emit (B→C, len=3)
      pipe.emit ( "C" );  // C-run continues — not reported
      circuit.await ();
      assertThat ( out ).hasSize ( 2 );
      assertThat ( out.get ( 0 ).from () ).isEqualTo ( "A" );
      assertThat ( out.get ( 0 ).to () ).isEqualTo ( "B" );
      assertThat ( out.get ( 0 ).length () ).isEqualTo ( 2L );
      assertThat ( out.get ( 1 ).from () ).isEqualTo ( "B" );
      assertThat ( out.get ( 1 ).to () ).isEqualTo ( "C" );
      assertThat ( out.get ( 1 ).length () ).isEqualTo ( 3L );
    } finally {
      circuit.close ();
    }
  }

  @Test
  @DisplayName ( "change() — alternating values report each closed run" )
  void changeAlternating () {
    final List < Change < Integer > > out = new ArrayList <> ();
    final var cortex  = Substrates.cortex ();
    final var circuit = cortex.circuit ();
    try {
      final var flow = cortex.< Integer > flow ( Integer.class ).change ();
      final var sink = circuit.< Change < Integer > > pipe ( out::add );
      final var pipe = flow.pipe ( sink );
      pipe.emit ( 1 );  // open
      pipe.emit ( 2 );  // emit (1→2, len=1)
      pipe.emit ( 1 );  // emit (2→1, len=1)
      pipe.emit ( 2 );  // emit (1→2, len=1)
      circuit.await ();
      assertThat ( out ).hasSize ( 3 );
      assertThat ( out ).allMatch ( c -> c.length () == 1L );
    } finally {
      circuit.close ();
    }
  }

  // ── composition ──────────────────────────────────────────────────────────

  @Test
  @DisplayName ( "run().map() — projects Run into a display string" )
  void runComposesWithMap () {
    final List < String > out = new ArrayList <> ();
    final var cortex  = Substrates.cortex ();
    final var circuit = cortex.circuit ();
    try {
      final var flow = cortex.< Integer > flow ( Integer.class )
        .run ()
        .map ( r -> r.emission () + " x" + r.length () );
      final var sink = circuit.< String > pipe ( out::add );
      final var pipe = flow.pipe ( sink );
      pipe.emit ( 5 );
      pipe.emit ( 5 );
      pipe.emit ( 7 );
      circuit.await ();
      assertThat ( out ).containsExactly ( "5 x1", "5 x2", "7 x1" );
    } finally {
      circuit.close ();
    }
  }

  @Test
  @DisplayName ( "change().map() — projects Change into a transition string" )
  void changeComposesWithMap () {
    final List < String > out = new ArrayList <> ();
    final var cortex  = Substrates.cortex ();
    final var circuit = cortex.circuit ();
    try {
      final var flow = cortex.< String > flow ( String.class )
        .change ()
        .map ( c -> c.from () + " -> " + c.to () + " (" + c.length () + ")" );
      final var sink = circuit.< String > pipe ( out::add );
      final var pipe = flow.pipe ( sink );
      pipe.emit ( "open" );
      pipe.emit ( "open" );
      pipe.emit ( "closed" );
      circuit.await ();
      assertThat ( out ).containsExactly ( "open -> closed (2)" );
    } finally {
      circuit.close ();
    }
  }

  // ── independence ─────────────────────────────────────────────────────────

  @Test
  @DisplayName ( "run() — independent state per materialisation" )
  void runIndependentPerMaterialisation () {
    final List < Run < Integer > > outA = new ArrayList <> ();
    final List < Run < Integer > > outB = new ArrayList <> ();
    final var cortex  = Substrates.cortex ();
    final var circuit = cortex.circuit ();
    try {
      final var flow = cortex.< Integer > flow ( Integer.class ).run ();
      final var sinkA = circuit.< Run < Integer > > pipe ( outA::add );
      final var sinkB = circuit.< Run < Integer > > pipe ( outB::add );
      final var pipeA = flow.pipe ( sinkA );
      final var pipeB = flow.pipe ( sinkB );
      pipeA.emit ( 1 );
      pipeA.emit ( 1 );
      pipeB.emit ( 9 );          // B's first emission — length must be 1, not 3
      pipeB.emit ( 9 );
      circuit.await ();
      assertThat ( outA ).extracting ( r -> r.length () ).containsExactly ( 1L, 2L );
      assertThat ( outB ).extracting ( r -> r.length () ).containsExactly ( 1L, 2L );
    } finally {
      circuit.close ();
    }
  }
}
