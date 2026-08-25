package io.fullerstack.substrates;

import io.humainary.substrates.api.Substrates;
import io.humainary.substrates.api.Substrates.Basin;
import io.humainary.substrates.api.Substrates.Capture;
import io.humainary.substrates.api.Substrates.Circuit;
import io.humainary.substrates.api.Substrates.Conduit;
import io.humainary.substrates.api.Substrates.Cortex;
import io.humainary.substrates.api.Substrates.Name;
import io.humainary.substrates.api.Substrates.Pipe;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/// **D-1a — digital-twin convergence, in-process.**
///
/// The specification's RATIONALE (§1, "Buffered Capture as the Digital Twin
/// Bridge") claims a replica fed ONLY the primary's ordered emission stream
/// converges to the primary's state. This is the cheapest falsifier of that
/// claim — one address space, no serialization, no network — isolating the two
/// failure modes that need neither:
///
/// - **F1 divergence** — replica state differs from the primary after replay.
/// - **F2 ordering** — the replica observes an order the primary did not
///   (SPEC §16.1(2)).
///
/// Both circuits run the SAME topology: a conduit of named channels, each
/// folding its emissions into an ORDER-SENSITIVE hash chain (`h = h*31 + v`),
/// so a permutation of the same multiset yields a different value — which is
/// what makes F2 detectable at all (asserted explicitly at the end). The
/// primary additionally carries the `Sink` → `Basin` capture bridge (§11.1).
/// Emissions are interleaved across channels from several caller contexts, so
/// both routing and ingress ordering are exercised. The replica is fed
/// exclusively by draining the basin and re-emitting each capture into the
/// channel named by `capture.subject().name()` — the provenance the Capture
/// envelope carries.
final class DigitalTwinConvergenceTest {

  private static final int CHANNELS = 4;
  private static final int PER_PRODUCER = 500;
  private static final int PRODUCERS = 3;

  /// One twin side: its circuit, its conduit, and the per-channel hash chains
  /// the emissions fold into.
  private record Side(Circuit circuit, Conduit<Integer> conduit, Map<String, long[]> chains) {}

  /// Builds identical topology on either side, so any divergence is
  /// attributable to the emission stream and not to the wiring.
  private static Side build(final Cortex cortex, final Circuit circuit, final List<Name> names) {
    final Conduit<Integer> conduit = circuit.conduit(cortex.name("twin.conduit"), Integer.class);
    final Map<String, long[]> chains = new LinkedHashMap<>();
    final Map<String, Pipe<Integer>> folds = new LinkedHashMap<>();
    for (final Name n : names) {
      final String key = n.path().toString();
      final long[] chain = { 17L };
      chains.put(key, chain);
      folds.put(key, circuit.pipe(v -> chain[0] = chain[0] * 31 + v));
    }
    conduit.subscribe(circuit.subscriber(cortex.name("twin.fold"),
        (subject, registrar) -> registrar.register(folds.get(subject.name().path().toString()))));
    return new Side(circuit, conduit, chains);
  }

  @Test
  void replicaConvergesOnPrimaryFromCaptureStreamAlone() throws Exception {

    final Cortex cortex = Substrates.cortex();
    final List<Name> names = new ArrayList<>();
    for (int c = 0; c < CHANNELS; c++) names.add(cortex.name("ch" + c));

    final Circuit primaryCircuit = cortex.circuit(cortex.name("twin.primary"));
    final Circuit replicaCircuit = cortex.circuit(cortex.name("twin.replica"));

    try {
      final Side primary = build(cortex, primaryCircuit, names);
      final Side replica = build(cortex, replicaCircuit, names);

      // the capture bridge — primary only (SPEC §11.1)
      final Basin<Capture<Integer>> basin = primaryCircuit.basin(1 << 16);
      primary.conduit().subscribe(primaryCircuit.subscriber(cortex.name("twin.capture"),
          primaryCircuit.sink(basin.pipe())));
      primaryCircuit.await();
      replicaCircuit.await();

      // admit interleaved emissions from several caller contexts
      final CountDownLatch start = new CountDownLatch(1);
      final List<Thread> producers = new ArrayList<>();
      for (int p = 0; p < PRODUCERS; p++) {
        final int id = p;
        final Thread t = Thread.ofVirtual().unstarted(() -> {
          try {
            start.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
          for (int i = 0; i < PER_PRODUCER; i++) {
            primary.conduit().get(names.get((i + id) % CHANNELS)).emit(id * 1_000_000 + i);
          }
        });
        producers.add(t);
        t.start();
      }
      start.countDown();
      for (final Thread t : producers) t.join();
      primaryCircuit.await();

      // replicate: drain the ordered capture stream, replay into the replica
      final List<Capture<Integer>> captures = new ArrayList<>();
      basin.drain(primaryCircuit.pipe(captures::add));
      primaryCircuit.await();

      assertEquals(PRODUCERS * PER_PRODUCER, captures.size(),
          "every admitted emission must be captured exactly once");

      for (final Capture<Integer> c : captures) {
        replica.conduit().get(cortex.name(c.subject().name().path().toString()))
            .emit(c.emission());
      }
      replicaCircuit.await();

      // F1 — convergence, per channel
      for (final Name n : names) {
        final String key = n.path().toString();
        assertEquals(primary.chains().get(key)[0], replica.chains().get(key)[0],
            "replica chain diverged on channel " + key + " (F1)");
      }

      // F2 — prove the F1 assertion could actually fail: reverse one channel's
      // stream and show the chain value changes. If this passes trivially, the
      // convergence assertion above is not testing ordering at all.
      final String first = names.getFirst().path().toString();
      final List<Integer> ch0 = new ArrayList<>();
      for (final Capture<Integer> c : captures) {
        if (c.subject().name().path().toString().equals(first)) ch0.add(c.emission());
      }
      long permuted = 17L;
      for (int i = ch0.size() - 1; i >= 0; i--) permuted = permuted * 31 + ch0.get(i);
      assertNotEquals(primary.chains().get(first)[0], permuted,
          "hash chain is not order-sensitive — F2 would be undetectable");

      System.out.printf("D-1a PASS: %d emissions captured, %d channels converged; "
              + "ch0 chain primary=%d replica=%d (reversed=%d)%n",
          captures.size(), CHANNELS,
          primary.chains().get(first)[0], replica.chains().get(first)[0], permuted);

    } finally {
      replicaCircuit.close();
      primaryCircuit.close();
    }
  }
}
