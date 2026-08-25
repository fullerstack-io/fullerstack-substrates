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

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// **D-1b — payload scaling of the ordered-stream synchronization.**
///
/// The RATIONALE (§1) claims the synchronization payload is the ordered event
/// stream rather than continuous state snapshots, "reducing bandwidth
/// requirements to the volume of actual state changes." Restated falsifiably:
///
/// > stream payload scales with CHANGE COUNT and is independent of STATE SIZE;
/// > snapshot payload scales with STATE SIZE and is independent of change count.
///
/// **F3 (pre-registered):** stream payload grows > 20% across a 64× state-size
/// sweep at fixed change count ⇒ the bandwidth thesis is REFUTED as stated.
///
/// Matched control: periodic full-state snapshot — what conventional
/// replication does — encoding every channel's current value each interval.
///
/// Encoding was declared before measuring (STRATEGY.md D-1b) so it cannot be
/// tuned to the answer: varint channel ordinal + zig-zag varint value, no
/// compression, no framing tricks. The claim under test is the SHAPE of the
/// scaling, not a favourable constant.
final class TwinPayloadScalingTest {

  /// varint + zig-zag: the declared wire form. Encodes the ordered stream as
  /// (channel ordinal, value) pairs — the provenance a Capture already carries.
  private static void varint(final ByteArrayOutputStream out, long v) {
    while ((v & ~0x7FL) != 0) {
      out.write((int) ((v & 0x7F) | 0x80));
      v >>>= 7;
    }
    out.write((int) v);
  }

  private static void zigzag(final ByteArrayOutputStream out, final long v) {
    varint(out, (v << 1) ^ (v >> 63));
  }

  /// Bytes needed to keep a replica converged by shipping the ordered stream.
  private static int streamBytes(final List<Capture<Integer>> captures,
      final Map<String, Integer> ordinals) {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (final Capture<Integer> c : captures) {
      varint(out, ordinals.get(c.subject().name().path().toString()));
      zigzag(out, c.emission());
    }
    return out.size();
  }

  /// Bytes needed by the CONTROL: full-state snapshot every `intervalChanges`
  /// changes — every channel's current value, whether or not it moved.
  private static int snapshotBytes(final int channels, final int changes,
      final int intervalChanges, final long[] finalState) {
    final int snapshots = Math.max(1, changes / intervalChanges);
    final ByteArrayOutputStream one = new ByteArrayOutputStream();
    for (int ch = 0; ch < channels; ch++) {
      varint(one, ch);
      zigzag(one, finalState[ch % finalState.length]);
    }
    return one.size() * snapshots;
  }

  /// Runs a primary circuit with `channels` channels and `changes` emissions,
  /// returns the captured ordered stream plus the ordinal table.
  private record Run(List<Capture<Integer>> captures, Map<String, Integer> ordinals,
                     long[] state) {}

  private static Run run(final Cortex cortex, final int channels, final int changes) {
    final Circuit circuit = cortex.circuit(cortex.name("payload.p" + channels + "." + changes));
    try {
      final Conduit<Integer> conduit =
          circuit.conduit(cortex.name("payload.conduit"), Integer.class);
      final Map<String, Integer> ordinals = new LinkedHashMap<>();
      final List<Name> names = new ArrayList<>();
      final long[] state = new long[channels];
      final Map<String, Pipe<Integer>> folds = new LinkedHashMap<>();
      for (int c = 0; c < channels; c++) {
        final Name n = cortex.name("c" + c);
        names.add(n);
        ordinals.put(n.path().toString(), c);
        final int idx = c;
        folds.put(n.path().toString(), circuit.pipe(v -> state[idx] = state[idx] * 31 + v));
      }
      conduit.subscribe(circuit.subscriber(cortex.name("payload.fold"),
          (subject, registrar) -> registrar.register(folds.get(subject.name().path().toString()))));

      final Basin<Capture<Integer>> basin = circuit.basin(1 << 18);
      conduit.subscribe(circuit.subscriber(cortex.name("payload.capture"),
          circuit.sink(basin.pipe())));
      circuit.await();

      for (int i = 0; i < changes; i++) {
        conduit.get(names.get(i % channels)).emit(i);
      }
      circuit.await();

      final List<Capture<Integer>> captures = new ArrayList<>();
      basin.drain(circuit.pipe(captures::add));
      circuit.await();
      return new Run(captures, ordinals, state);
    } finally {
      circuit.close();
    }
  }

  @Test
  void streamPayloadTracksChangesNotStateSize() {

    final Cortex cortex = Substrates.cortex();

    // ── Sweep A: state size 4 → 256 channels, FIXED 2000 changes ──
    System.out.println("\nSWEEP A — fixed 2000 changes, state size varies");
    System.out.printf("%8s %12s %14s%n", "channels", "streamBytes", "snapshotBytes");
    final List<Integer> streamA = new ArrayList<>();
    for (final int channels : new int[] { 4, 16, 64, 256 }) {
      final Run r = run(cortex, channels, 2000);
      assertEquals(2000, r.captures().size(), "all changes must be captured");
      final int s = streamBytes(r.captures(), r.ordinals());
      final int snap = snapshotBytes(channels, 2000, 100, r.state());
      streamA.add(s);
      System.out.printf("%8d %12d %14d%n", channels, s, snap);
    }

    // F3: stream payload must not grow materially with state size
    final int smallest = streamA.getFirst();
    final int largest = streamA.getLast();
    final double growth = (largest - smallest) / (double) smallest;
    System.out.printf("stream payload growth across 64x state size: %+.1f%%  (F3 fires > +20%%)%n",
        growth * 100);
    assertTrue(growth <= 0.20,
        "F3 REFUTED THESIS: stream payload grew " + Math.round(growth * 100)
            + "% across a 64x state-size sweep");

    // ── Sweep B: changes 100 → 10000, FIXED 64 channels ──
    System.out.println("\nSWEEP B — fixed 64 channels, change count varies");
    System.out.printf("%8s %12s %14s%n", "changes", "streamBytes", "snapshotBytes");
    final List<Integer> streamB = new ArrayList<>();
    for (final int changes : new int[] { 100, 1000, 10000 }) {
      final Run r = run(cortex, 64, changes);
      final int s = streamBytes(r.captures(), r.ordinals());
      final int snap = snapshotBytes(64, changes, 100, r.state());
      streamB.add(s);
      System.out.printf("%8d %12d %14d%n", changes, s, snap);
    }
    // stream must scale roughly linearly with change count (the claim's other half)
    final double ratio = streamB.getLast() / (double) streamB.getFirst();
    System.out.printf("stream payload ratio across 100x changes: %.1fx (linear would be ~100x)%n",
        ratio);
    assertTrue(ratio > 50, "stream payload must scale with change count");

    // ── the drone-like shape: 13 channels, 1 change/s, 5-minute flight ──
    final Run drone = run(cortex, 13, 300);
    final int droneStream = streamBytes(drone.captures(), drone.ordinals());
    final int droneSnapshot = snapshotBytes(13, 300, 1, drone.state());   // 1 Hz snapshot
    System.out.printf("%nDRONE SHAPE (13 channels, 300 changes over a 5-min flight):%n"
            + "  ordered stream : %d bytes total (%.1f bytes/change)%n"
            + "  1 Hz snapshots : %d bytes total%n"
            + "  ratio          : %.1fx less%n",
        droneStream, droneStream / 300.0, droneSnapshot,
        droneSnapshot / (double) droneStream);
  }
}
