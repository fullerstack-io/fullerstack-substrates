package io.fullerstack.substrates;

import io.humainary.substrates.api.Substrates;
import io.humainary.substrates.api.Substrates.Circuit;
import io.humainary.substrates.api.Substrates.Conduit;
import io.humainary.substrates.api.Substrates.Cortex;
import io.humainary.substrates.api.Substrates.Fault;
import io.humainary.substrates.api.Substrates.Name;
import io.humainary.substrates.api.Substrates.Pipe;
import io.humainary.substrates.api.Substrates.Port;
import io.humainary.substrates.api.Substrates.Subject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Tests for the 2.9 Port primitive (SPEC §11.6).
///
/// Port grants queued mutation authority — replace, update, update-with-arg,
/// and emit-current-value — without exposing a read accessor. Every op is
/// queued: caller-thread ops land on ingress, owning-worker ops land on
/// transit. Lines 1762-1767 codify this routing; §15.4 governs failure
/// isolation; lines 1784-1787 specify the null-return Fault behaviour.
class FsPortTest {

  // ── replace / emit visibility (caller-thread ops) ─────────────────────────

  @Test
  @DisplayName("emit forwards the seed value to the target pipe")
  void emitForwardsSeed() {
    Cortex cortex = Substrates.cortex();
    Circuit circuit = cortex.circuit(cortex.name("port.seed"));
    try {
      Port<Integer> port = circuit.port(7);
      List<Integer> observed = new CopyOnWriteArrayList<>();
      Pipe<Integer> sink = circuit.pipe(observed::add);
      port.emit(sink);
      circuit.await();
      assertThat(observed).containsExactly(7);
    } finally {
      circuit.close();
    }
  }

  @Test
  @DisplayName("replace from external thread is visible to a subsequent emit after await")
  void replaceVisibleAfterAwait() {
    Cortex cortex = Substrates.cortex();
    Circuit circuit = cortex.circuit(cortex.name("port.replace"));
    try {
      Port<Integer> port = circuit.port(1);
      List<Integer> observed = new CopyOnWriteArrayList<>();
      Pipe<Integer> sink = circuit.pipe(observed::add);
      port.replace(2);
      port.replace(3);
      port.emit(sink);
      circuit.await();
      assertThat(observed).containsExactly(3);
    } finally {
      circuit.close();
    }
  }

  @Test
  @DisplayName("update(fn) applies the transform; emit observes the new value")
  void updateApplied() {
    Cortex cortex = Substrates.cortex();
    Circuit circuit = cortex.circuit(cortex.name("port.update"));
    try {
      Port<Integer> port = circuit.port(10);
      List<Integer> observed = new CopyOnWriteArrayList<>();
      Pipe<Integer> sink = circuit.pipe(observed::add);
      port.update(x -> x + 5);
      port.update(x -> x * 2);
      port.emit(sink);
      circuit.await();
      assertThat(observed).containsExactly(30);
    } finally {
      circuit.close();
    }
  }

  @Test
  @DisplayName("update(arg, fn) carries the argument to the worker")
  void updateWithArg() {
    Cortex cortex = Substrates.cortex();
    Circuit circuit = cortex.circuit(cortex.name("port.update.arg"));
    try {
      Port<Integer> port = circuit.port(10);
      List<Integer> observed = new CopyOnWriteArrayList<>();
      Pipe<Integer> sink = circuit.pipe(observed::add);
      port.update(100, Integer::sum);
      port.emit(sink);
      circuit.await();
      assertThat(observed).containsExactly(110);
    } finally {
      circuit.close();
    }
  }

  // ── Ordering (SPEC §11.6 deterministic ordering) ──────────────────────────

  @Test
  @DisplayName("port operations are processed in admission order from a single caller")
  void deterministicOrdering() {
    Cortex cortex = Substrates.cortex();
    Circuit circuit = cortex.circuit(cortex.name("port.order"));
    try {
      Port<List<Integer>> port = circuit.port(new ArrayList<>());
      List<Integer> snapshot = new CopyOnWriteArrayList<>();
      Pipe<List<Integer>> sink = circuit.pipe(v -> snapshot.addAll(v));
      for (int i = 0; i < 10; i++) {
        int captured = i;
        // append by replacement: build a new list each time (port values
        // SHOULD be effectively immutable per §11.6 line 1798)
        port.update(prev -> {
          List<Integer> next = new ArrayList<>(prev);
          next.add(captured);
          return next;
        });
      }
      port.emit(sink);
      circuit.await();
      assertThat(snapshot).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    } finally {
      circuit.close();
    }
  }

  // ── Failure isolation (SPEC §11.6 §15.4) ──────────────────────────────────

  @Test
  @DisplayName("update fn that throws retains the previous value; sibling ops still process")
  void updateThrowsRetainsValue() {
    Cortex cortex = Substrates.cortex();
    Circuit circuit = cortex.circuit(cortex.name("port.throw"));
    try {
      Port<Integer> port = circuit.port(5);
      List<Integer> observed = new CopyOnWriteArrayList<>();
      Pipe<Integer> sink = circuit.pipe(observed::add);
      port.update(x -> { throw new RuntimeException("nope"); });
      port.emit(sink);            // sees retained 5
      port.update(x -> x + 1);    // still processes
      port.emit(sink);            // sees 6
      circuit.await();
      assertThat(observed).containsExactly(5, 6);
    } finally {
      circuit.close();
    }
  }

  @Test
  @DisplayName("update fn returning null retains the previous value (Fault surfaced on isolation path)")
  void updateNullReturnRetainsValue() {
    Cortex cortex = Substrates.cortex();
    Circuit circuit = cortex.circuit(cortex.name("port.null"));
    try {
      Port<Integer> port = circuit.port(5);
      List<Integer> observed = new CopyOnWriteArrayList<>();
      Pipe<Integer> sink = circuit.pipe(observed::add);
      port.update(x -> null);     // §1784-1787: null-return surfaces as Fault, isolated by §15.4
      port.emit(sink);            // sees retained 5
      circuit.await();
      assertThat(observed).containsExactly(5);
    } finally {
      circuit.close();
    }
  }

  @Test
  @DisplayName("update(arg, fn) that throws retains the previous value")
  void updateArgThrowsRetainsValue() {
    Cortex cortex = Substrates.cortex();
    Circuit circuit = cortex.circuit(cortex.name("port.arg.throw"));
    try {
      Port<Integer> port = circuit.port(5);
      List<Integer> observed = new CopyOnWriteArrayList<>();
      Pipe<Integer> sink = circuit.pipe(observed::add);
      port.update(1, (cur, a) -> { throw new RuntimeException("nope"); });
      port.emit(sink);
      circuit.await();
      assertThat(observed).containsExactly(5);
    } finally {
      circuit.close();
    }
  }

  // ── Worker-thread routing (SPEC §11.6 lines 1762-1767, §5.3) ──────────────

  @Test
  @DisplayName("port.update from the worker is queued (non-immediate) but processed in cascade order")
  void workerThreadUpdateIsTransitWork() {
    Cortex cortex = Substrates.cortex();
    Circuit circuit = cortex.circuit(cortex.name("port.cascade"));
    try {
      Port<Integer> port = circuit.port(0);
      List<Integer> observed = new CopyOnWriteArrayList<>();
      Pipe<Integer> sink = circuit.pipe(observed::add);

      // Trigger conduit: when an emission arrives, the callback runs on the
      // worker thread. Inside the callback we issue update + emit; these go
      // onto the TRANSIT queue and process in admission order before the
      // next ingress item.
      Conduit<Integer> trigger = circuit.conduit(cortex.name("trigger"), Integer.class);
      trigger.subscribe(circuit.subscriber(cortex.name("sub"),
        (subj, reg) -> reg.register(v -> {
          port.update(x -> x + v);
          port.emit(sink);                  // sees the post-update value
        })));

      trigger.get(cortex.name("x")).emit(5);
      trigger.get(cortex.name("x")).emit(7);
      circuit.await();
      assertThat(observed).containsExactly(5, 12);     // 0+5=5, then 5+7=12
    } finally {
      circuit.close();
    }
  }

  // ── Provider mismatch (SPEC §15.1) ────────────────────────────────────────

  @Test
  @DisplayName("emit with a non-provider pipe raises Fault on the caller thread")
  void emitForeignPipeRaisesFault() {
    Cortex cortex = Substrates.cortex();
    Circuit circuit = cortex.circuit(cortex.name("port.foreign"));
    try {
      Port<Integer> port = circuit.port(1);
      // Pipe isn't a functional interface (extends Substrate.subject()), so
      // an anonymous class is required to construct a non-FsPipe instance.
      Pipe<Integer> foreign = new Pipe<>() {
        @Override public void emit(Integer v) { /* not an FsPipe */ }
        @Override public Subject<Pipe<Integer>> subject() { return null; }
      };
      assertThatThrownBy(() -> port.emit(foreign)).isInstanceOf(Fault.class);
    } finally {
      circuit.close();
    }
  }

  // ── Argument validation ───────────────────────────────────────────────────

  @Test
  @DisplayName("replace(null) raises NullPointerException on the caller thread")
  void replaceNullThrowsNpe() {
    Cortex cortex = Substrates.cortex();
    Circuit circuit = cortex.circuit(cortex.name("port.replace.null"));
    try {
      Port<Integer> port = circuit.port(1);
      assertThatThrownBy(() -> port.replace(null)).isInstanceOf(NullPointerException.class);
    } finally {
      circuit.close();
    }
  }

  @Test
  @DisplayName("update(null fn) raises NullPointerException on the caller thread")
  void updateNullFnThrowsNpe() {
    Cortex cortex = Substrates.cortex();
    Circuit circuit = cortex.circuit(cortex.name("port.update.null"));
    try {
      Port<Integer> port = circuit.port(1);
      assertThatThrownBy(() -> port.update(null)).isInstanceOf(NullPointerException.class);
    } finally {
      circuit.close();
    }
  }

  @Test
  @DisplayName("emit(null) raises NullPointerException on the caller thread")
  void emitNullThrowsNpe() {
    Cortex cortex = Substrates.cortex();
    Circuit circuit = cortex.circuit(cortex.name("port.emit.null"));
    try {
      Port<Integer> port = circuit.port(1);
      assertThatThrownBy(() -> port.emit(null)).isInstanceOf(NullPointerException.class);
    } finally {
      circuit.close();
    }
  }

  // ── Name inheritance (SPEC §11.6 lines 1802-1805) ─────────────────────────

  @Test
  @DisplayName("port(initial) inherits the owning circuit's name")
  void unnamedPortInheritsCircuitName() {
    Cortex cortex = Substrates.cortex();
    Name circuitName = cortex.name("acme");
    Circuit circuit = cortex.circuit(circuitName);
    try {
      Port<Integer> port = circuit.port(1);
      assertThat(port.subject().name() == circuitName).isTrue();   // names are interned
    } finally {
      circuit.close();
    }
  }

  @Test
  @DisplayName("port(name, initial) binds the supplied name")
  void namedPortUsesSuppliedName() {
    Cortex cortex = Substrates.cortex();
    Circuit circuit = cortex.circuit(cortex.name("c"));
    try {
      Name explicit = cortex.name("counter");
      Port<Integer> port = circuit.port(explicit, 1);
      assertThat(port.subject().name() == explicit).isTrue();
    } finally {
      circuit.close();
    }
  }

  @Test
  @DisplayName("two named port calls with the same name return distinct identities")
  void distinctIdsForSameName() {
    Cortex cortex = Substrates.cortex();
    Circuit circuit = cortex.circuit(cortex.name("c"));
    try {
      Name n = cortex.name("dup");
      Port<Integer> a = circuit.port(n, 1);
      Port<Integer> b = circuit.port(n, 1);
      assertThat(a.subject().id()).isNotEqualTo(b.subject().id());
      assertThat(a).isNotSameAs(b);
    } finally {
      circuit.close();
    }
  }

  // ── Lifecycle (SPEC §11.6 lines 1807-1811) ────────────────────────────────

  @Test
  @DisplayName("Circuit.port(initial) raises Fault after the circuit is closed")
  void factoryAfterCloseRaisesFault() {
    Cortex cortex = Substrates.cortex();
    Circuit circuit = cortex.circuit(cortex.name("c"));
    circuit.close();
    assertThatThrownBy(() -> circuit.port(1)).isInstanceOf(Fault.class);
  }

  @Test
  @DisplayName("Port operations on a closed circuit do not throw and do not take effect")
  void postCloseOpsSilentlyDropped() {
    Cortex cortex = Substrates.cortex();
    Circuit circuit = cortex.circuit(cortex.name("port.postclose"));
    AtomicInteger observed = new AtomicInteger(-1);
    Port<Integer> port = circuit.port(1);
    Pipe<Integer> sink = circuit.pipe(observed::set);
    circuit.close();
    // Per §11.6 lines 1807-1811: must not throw synchronously, must not take effect.
    port.replace(2);
    port.update(x -> x + 1);
    port.emit(sink);
    assertThat(observed.get()).isEqualTo(-1);
  }

  @Test
  @DisplayName("Circuit.port(null) raises NullPointerException")
  void factoryNullSeedRejected() {
    Cortex cortex = Substrates.cortex();
    Circuit circuit = cortex.circuit(cortex.name("c"));
    try {
      assertThatThrownBy(() -> circuit.port(null)).isInstanceOf(NullPointerException.class);
    } finally {
      circuit.close();
    }
  }
}
