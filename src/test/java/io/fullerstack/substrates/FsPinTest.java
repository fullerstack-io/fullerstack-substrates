package io.fullerstack.substrates;

import io.humainary.substrates.api.Substrates;
import io.humainary.substrates.api.Substrates.Circuit;
import io.humainary.substrates.api.Substrates.Conduit;
import io.humainary.substrates.api.Substrates.Cortex;
import io.humainary.substrates.api.Substrates.Fault;
import io.humainary.substrates.api.Substrates.Name;
import io.humainary.substrates.api.Substrates.Pin;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Tests for the 2.9 Pin primitive (SPEC §11.7).
///
/// Pin grants immediate read/write access confined to the owning circuit's
/// worker thread. The owner-context guard rejects every other context with
/// an illegal-context-use error; see §11.7 lines 1834-1838.
class FsPinTest {

  // ── Owner-context access ──────────────────────────────────────────────────

  @Test
  @DisplayName("Pin.get returns the seed when called from the worker")
  void getReturnsSeedOnWorker() {
    Cortex cortex = Substrates.cortex();
    Circuit circuit = cortex.circuit(cortex.name("pin.seed"));
    try {
      Pin<Integer> pin = circuit.pin(42);
      Conduit<Integer> trigger = circuit.conduit(cortex.name("trigger"), Integer.class);
      AtomicInteger observed = new AtomicInteger();
      trigger.subscribe(circuit.subscriber(cortex.name("sub"),
        (subj, reg) -> reg.register(v -> observed.set(pin.get()))));
      trigger.get(cortex.name("x")).emit(0);
      circuit.await();
      assertThat(observed.get()).isEqualTo(42);
    } finally {
      circuit.close();
    }
  }

  @Test
  @DisplayName("Pin.set then Pin.get in the same callback observes the new value")
  void setThenGetSameCallback() {
    Cortex cortex = Substrates.cortex();
    Circuit circuit = cortex.circuit(cortex.name("pin.setget"));
    try {
      Pin<Integer> pin = circuit.pin(1);
      Conduit<Integer> trigger = circuit.conduit(cortex.name("trigger"), Integer.class);
      AtomicInteger observed = new AtomicInteger();
      trigger.subscribe(circuit.subscriber(cortex.name("sub"),
        (subj, reg) -> reg.register(v -> {
          pin.set(99);                  // immediate
          observed.set(pin.get());      // sees 99 on the same line — Pin's defining contract
        })));
      trigger.get(cortex.name("x")).emit(0);
      circuit.await();
      assertThat(observed.get()).isEqualTo(99);
    } finally {
      circuit.close();
    }
  }

  // ── Owner-context guard ───────────────────────────────────────────────────

  @Test
  @DisplayName("Pin.get from external thread raises IllegalStateException")
  void getFromExternalThreadThrows() {
    Cortex cortex = Substrates.cortex();
    Circuit circuit = cortex.circuit(cortex.name("pin.extget"));
    try {
      Pin<Integer> pin = circuit.pin(1);
      assertThatThrownBy(pin::get).isInstanceOf(IllegalStateException.class);
    } finally {
      circuit.close();
    }
  }

  @Test
  @DisplayName("Pin.set from external thread raises IllegalStateException")
  void setFromExternalThreadThrows() {
    Cortex cortex = Substrates.cortex();
    Circuit circuit = cortex.circuit(cortex.name("pin.extset"));
    try {
      Pin<Integer> pin = circuit.pin(1);
      assertThatThrownBy(() -> pin.set(2)).isInstanceOf(IllegalStateException.class);
    } finally {
      circuit.close();
    }
  }

  @Test
  @DisplayName("Pin.get/set from a different circuit's worker raises IllegalStateException")
  void crossCircuitWorkerRejected() {
    Cortex cortex = Substrates.cortex();
    Circuit owner = cortex.circuit(cortex.name("owner"));
    Circuit other = cortex.circuit(cortex.name("other"));
    try {
      Pin<Integer> pin = owner.pin(1);
      Conduit<Integer> trigger = other.conduit(cortex.name("trigger"), Integer.class);
      AtomicReference<Throwable> captured = new AtomicReference<>();
      trigger.subscribe(other.subscriber(cortex.name("sub"),
        (subj, reg) -> reg.register(v -> {
          try { pin.get(); } catch (Throwable t) { captured.set(t); }
        })));
      trigger.get(cortex.name("x")).emit(0);
      other.await();
      assertThat(captured.get()).isInstanceOf(IllegalStateException.class);
    } finally {
      other.close();
      owner.close();
    }
  }

  // ── Argument validation ───────────────────────────────────────────────────

  @Test
  @DisplayName("Pin.set(null) raises NullPointerException (on the owner thread)")
  void setNullThrowsNpe() {
    Cortex cortex = Substrates.cortex();
    Circuit circuit = cortex.circuit(cortex.name("pin.null"));
    try {
      Pin<Integer> pin = circuit.pin(1);
      Conduit<Integer> trigger = circuit.conduit(cortex.name("trigger"), Integer.class);
      AtomicReference<Throwable> captured = new AtomicReference<>();
      trigger.subscribe(circuit.subscriber(cortex.name("sub"),
        (subj, reg) -> reg.register(v -> {
          try { pin.set(null); } catch (Throwable t) { captured.set(t); }
        })));
      trigger.get(cortex.name("x")).emit(0);
      circuit.await();
      assertThat(captured.get()).isInstanceOf(NullPointerException.class);
    } finally {
      circuit.close();
    }
  }

  // ── Name inheritance (SPEC §11.7) ─────────────────────────────────────────

  @Test
  @DisplayName("pin(initial) inherits the owning circuit's name")
  void unnamedPinInheritsCircuitName() {
    Cortex cortex = Substrates.cortex();
    Name circuitName = cortex.name("acme");
    Circuit circuit = cortex.circuit(circuitName);
    try {
      Pin<Integer> pin = circuit.pin(1);
      assertThat(pin.subject().name() == circuitName).isTrue();   // names are interned
    } finally {
      circuit.close();
    }
  }

  @Test
  @DisplayName("pin(name, initial) binds the supplied name")
  void namedPinUsesSuppliedName() {
    Cortex cortex = Substrates.cortex();
    Circuit circuit = cortex.circuit(cortex.name("c"));
    try {
      Name explicit = cortex.name("temperature");
      Pin<Integer> pin = circuit.pin(explicit, 1);
      assertThat(pin.subject().name() == explicit).isTrue();
    } finally {
      circuit.close();
    }
  }

  @Test
  @DisplayName("two named pin calls with the same name return distinct identities")
  void distinctIdsForSameName() {
    Cortex cortex = Substrates.cortex();
    Circuit circuit = cortex.circuit(cortex.name("c"));
    try {
      Name n = cortex.name("dup");
      Pin<Integer> a = circuit.pin(n, 1);
      Pin<Integer> b = circuit.pin(n, 1);
      assertThat(a.subject().id()).isNotEqualTo(b.subject().id());
      assertThat(a).isNotSameAs(b);
    } finally {
      circuit.close();
    }
  }

  // ── Lifecycle (SPEC §11.7 + §9.1) ─────────────────────────────────────────

  @Test
  @DisplayName("Circuit.pin(initial) raises Fault after the circuit is closed")
  void factoryAfterCloseRaisesFault() {
    Cortex cortex = Substrates.cortex();
    Circuit circuit = cortex.circuit(cortex.name("c"));
    circuit.close();
    assertThatThrownBy(() -> circuit.pin(1)).isInstanceOf(Fault.class);
  }

  @Test
  @DisplayName("Circuit.pin(null) raises NullPointerException")
  void factoryNullSeedRejected() {
    Cortex cortex = Substrates.cortex();
    Circuit circuit = cortex.circuit(cortex.name("c"));
    try {
      assertThatThrownBy(() -> circuit.pin(null)).isInstanceOf(NullPointerException.class);
    } finally {
      circuit.close();
    }
  }

  @Test
  @DisplayName("Circuit.pin(name, null) raises NullPointerException")
  void factoryNamedNullSeedRejected() {
    Cortex cortex = Substrates.cortex();
    Circuit circuit = cortex.circuit(cortex.name("c"));
    try {
      Name n = cortex.name("p");
      assertThatThrownBy(() -> circuit.pin(n, null)).isInstanceOf(NullPointerException.class);
    } finally {
      circuit.close();
    }
  }
}
