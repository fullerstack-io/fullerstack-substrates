package io.fullerstack.substrates;

import io.humainary.substrates.api.Substrates.Circuit;
import io.humainary.substrates.api.Substrates.Cortex;
import io.humainary.substrates.api.Substrates.Fault;
import io.humainary.substrates.api.Substrates.Pipe;
import io.humainary.substrates.api.Substrates.Window;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static io.humainary.substrates.api.Substrates.cortex;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

/// §6.4.1 (MUST since 3.12.0): a Window is valid during the callback that receives it, and "a
/// reference handed to one callback MUST NOT become valid again in a later one". The lease was the
/// owner thread plus a generation that moved only when the same window stage emitted again, so a
/// window kept in a field and read from an unrelated later callback on the same worker validated —
/// the kit checks only use from outside the circuit, which the owner check already catches.
///
/// A later ingress item is a later callback by definition (§5.3 drains transit fully first), so the
/// lease now also ends with the cascade that minted it. Inside that cascade it stays valid, so a
/// window delivered one queued hop later through a same-circuit pipe — its receiving callback —
/// still works.
@DisplayName ( "§6.4.1: a Window's lease ends with the cascade that minted it" )
final class WindowCascadeLeaseTest {

  @Test
  @DisplayName ( "a window kept in a field faults when read from a later ingress item's callback" )
  void windowReadFromALaterCascadeFaults () {
    final Cortex cortex = cortex ();
    final Circuit circuit = cortex.circuit ( cortex.name ( "window-lease" ) );
    try {
      final AtomicReference < Window < Integer > > kept = new AtomicReference <> ();
      final AtomicReference < Throwable > raised = new AtomicReference <> ();

      final Pipe < Integer > windows = cortex.flow ( Integer.class ).window ( 2 )
        .pipe ( circuit.pipe ( kept::set ) );
      final Pipe < String > later = circuit.pipe ( ignored -> {
        try {
          kept.get ().first ();
        } catch ( final Throwable t ) {
          raised.set ( t );
        }
      } );

      windows.emit ( 1 );        // ingress 1: the window is minted and kept
      later.emit ( "read" );     // ingress 2: a later callback on the same worker reads it
      circuit.await ();

      assertInstanceOf ( Fault.class, raised.get (), "a window read in a later cascade must fault" );
    } finally {
      circuit.close ();
    }
  }

  @Test
  @DisplayName ( "a window delivered one queued hop later, through a same-circuit pipe, is valid in its receiving callback" )
  void windowDeliveredThroughAPipeHopIsValid () {
    final Cortex cortex = cortex ();
    final Circuit circuit = cortex.circuit ( cortex.name ( "window-hop" ) );
    try {
      final List < Integer > firsts = new CopyOnWriteArrayList <> ();
      final AtomicReference < Throwable > raised = new AtomicReference <> ();
      final Pipe < Window < Integer > > receiving = circuit.pipe ( w -> {
        try {
          firsts.add ( w.first () );
        } catch ( final Throwable t ) {
          raised.set ( t );
        }
      } );
      // A receptor that forwards each window to another same-circuit pipe: a transit hop.
      final Pipe < Integer > windows = cortex.flow ( Integer.class ).window ( 2 )
        .pipe ( circuit.pipe ( receiving::emit ) );

      windows.emit ( 1 );
      windows.emit ( 2 );
      windows.emit ( 3 );
      circuit.await ();

      assertNull ( raised.get (), "the receiving callback one hop downstream holds a valid window" );
      assertEquals ( List.of ( 1, 1, 2 ), firsts );
    } finally {
      circuit.close ();
    }
  }

  @Test
  @DisplayName ( "a window stays stable across a same-circuit emission queued from its own callback" )
  void windowStableAcrossItsOwnQueuedEmission () {
    final Cortex cortex = cortex ();
    final Circuit circuit = cortex.circuit ( cortex.name ( "window-stable" ) );
    try {
      final List < Integer > seen = new CopyOnWriteArrayList <> ();
      final AtomicReference < Pipe < Integer > > head = new AtomicReference <> ();
      head.set ( cortex.flow ( Integer.class ).window ( 2 ).pipe ( circuit.pipe ( w -> {
        final int before = w.last ();
        if ( before == 1 ) head.get ().emit ( 2 );   // queued behind this callback, same circuit
        seen.add ( before );
        seen.add ( w.last () );                       // still the window this callback was handed
      } ) ) );

      head.get ().emit ( 1 );
      circuit.await ();

      assertEquals ( List.of ( 1, 1, 2, 2 ), seen );
    } finally {
      circuit.close ();
    }
  }

}
