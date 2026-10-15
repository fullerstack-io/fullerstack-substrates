package io.fullerstack.substrates;

import io.humainary.substrates.api.Substrates.Circuit;
import io.humainary.substrates.api.Substrates.Cortex;
import io.humainary.substrates.api.Substrates.Ticker;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.humainary.substrates.api.Substrates.cortex;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// SPEC §11.4 bounded catch-up: when scheduling falls more than one interval behind, the ticker re-anchors
/// the grid to the current time — "a one-time phase shift in the grid". The ticks after a re-anchor follow
/// one interval apart from the new anchor, however many ticks came before it.
///
/// The scheduler is held up deliberately, late in a run, to force a re-anchor; the gap that follows must be
/// about the hold, not the number of ticks already emitted times the interval.
class TickerReanchorTest {

  @Test
  @DisplayName ( "after a re-anchor late in a run, ticks resume one interval apart" )
  void reanchorLateInARunIsAPhaseShiftNotAFreeze () throws Exception {
    Cortex cortex = cortex ();
    Circuit circuit = cortex.circuit ( cortex.name ( "ticker.reanchor" ) );
    long interval = 1_000_000L;                 // 1 ms
    int holdAt = 2_000;                          // ticks before the hold: a freeze would be ~2 s
    long holdNanos = 20_000_000L;                // 20 ms: more than one interval behind
    long[] at = new long[holdAt + 50];
    CountDownLatch held = new CountDownLatch ( 1 );
    CountDownLatch done = new CountDownLatch ( 1 );
    FsCircuit fs = (FsCircuit) circuit;
    Ticker ticker = circuit.ticker ( cortex.name ( "t" ), Duration.ofNanos ( interval ), circuit.pipe ( ( Long s ) -> {
      if ( s < at.length ) at[(int) (long) s] = System.nanoTime ();
      if ( s == holdAt ) {
        // hold up the ticker's scheduler, so the next grid point is missed by more than an interval
        fs.scheduler ().execute ( () -> { try { Thread.sleep ( holdNanos / 1_000_000 ); } catch ( InterruptedException e ) { Thread.currentThread ().interrupt (); } held.countDown (); } );
      }
      if ( s == at.length - 1 ) done.countDown ();
    } ) );
    assertTrue ( held.await ( 10, TimeUnit.SECONDS ), "the scheduler was held" );
    boolean resumed = done.await ( 2, TimeUnit.SECONDS );
    ticker.close ();
    circuit.close ();
    assertTrue ( resumed, "ticks resumed after the re-anchor within 2 s" );
    long worst = 0;
    for ( int s = holdAt + 1; s < at.length; s++ ) worst = Math.max ( worst, at[s] - at[s - 1] );
    assertTrue ( worst < holdNanos + 200 * interval, "worst gap after the hold " + worst / 1_000_000 + " ms: a phase shift, not a freeze of ~" + holdAt * interval / 1_000_000 + " ms" );
  }

}
