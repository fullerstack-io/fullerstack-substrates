package io.fullerstack.substrates;

import io.humainary.substrates.api.Substrates;
import io.humainary.substrates.api.Substrates.Basin;
import io.humainary.substrates.api.Substrates.Capture;
import io.humainary.substrates.api.Substrates.Circuit;
import io.humainary.substrates.api.Substrates.Conduit;
import io.humainary.substrates.api.Substrates.Cortex;
import io.humainary.substrates.api.Substrates.Name;
import io.humainary.substrates.api.Substrates.Pipe;
import io.humainary.substrates.api.Substrates.Subscription;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/// **D-1d — topology operations in the replay log.**
///
/// SPEC §5.1: *"Replaying emissions alone is insufficient because subscription
/// operations affect pipeline topology (§7.6), which determines how subsequent
/// emissions are routed."*
///
/// Two arms over the SAME primary run, in which a second subscriber is
/// registered mid-stream (a topology change):
///
/// - **Arm 1** — replica replays emissions only. Prediction: DIVERGES.
/// - **Arm 2** — replica replays emissions AND the subscription operation, in
///   admitted order. Prediction: CONVERGES.
///
/// **F4** fires if Arm 1 converges anyway — which would mean the specification's
/// requirement is stronger than the mechanism needs, and replication could be
/// simpler. That would be the more interesting outcome, so it is worth the test.
///
/// The observable state is an order-sensitive hash chain per fold, so both
/// missing emissions and misordered ones show up as divergence.
final class TwinTopologyReplayTest {

  private static final int BEFORE = 200;
  private static final int AFTER = 200;

  /// Replays a stream into a fresh circuit. `withTopology` decides whether the
  /// mid-stream subscriber registration is replayed alongside the emissions.
  private static long[] replay(final Cortex cortex, final String label,
      final List<Integer> stream, final int topologyAt, final boolean withTopology) {

    final Circuit circuit = cortex.circuit(cortex.name(label));
    try {
      final Conduit<Integer> conduit = circuit.conduit(cortex.name("topo.conduit"), Integer.class);
      final Name channel = cortex.name("ch0");
      final long[] foldA = { 17L };
      final long[] foldB = { 17L };

      // subscriber A is part of the initial topology on both arms
      final Pipe<Integer> pipeA = circuit.pipe(v -> foldA[0] = foldA[0] * 31 + v);
      conduit.subscribe(circuit.subscriber(cortex.name("topo.a"),
          (subject, registrar) -> registrar.register(pipeA)));
      circuit.await();

      for (int i = 0; i < stream.size(); i++) {
        if (i == topologyAt && withTopology) {
          // the topology operation, replayed in its admitted position
          final Pipe<Integer> pipeB = circuit.pipe(v -> foldB[0] = foldB[0] * 31 + v);
          conduit.subscribe(circuit.subscriber(cortex.name("topo.b"),
              (subject, registrar) -> registrar.register(pipeB)));
          circuit.await();
        }
        conduit.get(channel).emit(stream.get(i));
      }
      circuit.await();
      return new long[] { foldA[0], foldB[0] };

    } finally {
      circuit.close();
    }
  }

  @Test
  void emissionsAloneDivergeWhenTopologyChangesMidStream() {

    final Cortex cortex = Substrates.cortex();
    final Circuit primary = cortex.circuit(cortex.name("topo.primary"));

    final long[] primaryState;
    final List<Integer> stream = new ArrayList<>();

    try {
      final Conduit<Integer> conduit = primary.conduit(cortex.name("topo.conduit"), Integer.class);
      final Name channel = cortex.name("ch0");
      final long[] foldA = { 17L };
      final long[] foldB = { 17L };

      final Pipe<Integer> pipeA = primary.pipe(v -> foldA[0] = foldA[0] * 31 + v);
      conduit.subscribe(primary.subscriber(cortex.name("topo.a"),
          (subject, registrar) -> registrar.register(pipeA)));

      final Basin<Capture<Integer>> basin = primary.basin(1 << 14);
      conduit.subscribe(primary.subscriber(cortex.name("topo.capture"),
          primary.sink(basin.pipe())));
      primary.await();

      for (int i = 0; i < BEFORE; i++) conduit.get(channel).emit(i);
      primary.await();

      // ── the topology change, mid-stream ──
      final Pipe<Integer> pipeB = primary.pipe(v -> foldB[0] = foldB[0] * 31 + v);
      final Subscription subB = conduit.subscribe(primary.subscriber(cortex.name("topo.b"),
          (subject, registrar) -> registrar.register(pipeB)));
      primary.await();

      for (int i = BEFORE; i < BEFORE + AFTER; i++) conduit.get(channel).emit(i);
      primary.await();

      final List<Capture<Integer>> captures = new ArrayList<>();
      basin.drain(primary.pipe(captures::add));
      primary.await();
      for (final Capture<Integer> c : captures) stream.add(c.emission());

      primaryState = new long[] { foldA[0], foldB[0] };
      subB.close();
      primary.await();

    } finally {
      primary.close();
    }

    assertEquals(BEFORE + AFTER, stream.size(), "every emission captured once");

    // ── Arm 1: emissions only ──
    final long[] arm1 = replay(cortex, "topo.arm1", stream, BEFORE, false);
    // ── Arm 2: emissions + the topology operation in admitted order ──
    final long[] arm2 = replay(cortex, "topo.arm2", stream, BEFORE, true);

    System.out.printf("%nD-1d topology replay%n"
            + "  primary : foldA=%d foldB=%d%n"
            + "  arm1 (emissions only)  : foldA=%d foldB=%d%n"
            + "  arm2 (with topology op): foldA=%d foldB=%d%n",
        primaryState[0], primaryState[1], arm1[0], arm1[1], arm2[0], arm2[1]);

    // Arm 2 must converge on BOTH folds
    assertEquals(primaryState[0], arm2[0], "arm2 foldA must converge");
    assertEquals(primaryState[1], arm2[1], "arm2 foldB must converge");

    // Arm 1 must NOT: foldB never received the post-topology emissions.
    // If this assertion fails, F4 has fired — emissions alone were sufficient,
    // and SPEC §5.1's requirement is stronger than the mechanism needs.
    assertNotEquals(primaryState[1], arm1[1],
        "F4: emissions-only replay converged despite a mid-stream topology change");

    System.out.println("  => emissions-only replay DIVERGED on the post-change fold, "
        + "as SPEC §5.1 requires; full operation log converged.");
  }
}
