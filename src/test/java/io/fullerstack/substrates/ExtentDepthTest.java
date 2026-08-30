package io.fullerstack.substrates;

import io.humainary.substrates.api.Substrates.Cortex;
import io.humainary.substrates.api.Substrates.Name;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.humainary.substrates.api.Substrates.cortex;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

/// §4.1 guarantees a name-depth floor of 16 segments and requires an implementation imposing a
/// maximum to document it; §4.2 (3.0.5) adds that a documented limit governs **every** operation
/// reached through the hierarchy — comparison and cascading close included — not traversal alone.
/// 3.0.5 rewrote `Extent.foldTo` iteratively so its depth is bounded by heap rather than stack.
///
/// This provider imposes no maximum, which is only true while every hierarchy walk is iterative.
/// Two overrides were not: `FsName.path(Function)` and `FsSubject.path()` each recursed to the
/// root, so they threw `StackOverflowError` on a 20 000-segment name where `foldTo` and `stream`
/// returned normally. Both now delegate to the API's defaults.
///
/// The depth here is far past anything realistic — the point is that it is bounded by memory, not
/// by one method's stack frame, so the "no maximum" claim in `docs/CONFORMANCE.md` stays honest.
@DisplayName ( "Extent depth" )
final class ExtentDepthTest {

  private static final int DEEP = 20_000;

  private static Name deepName ( Cortex cortex, int depth ) {
    Name name = cortex.name ( "s0" );
    for ( var i = 1; i < depth; i++ ) name = name.name ( "s" + i );
    return name;
  }

  @Test
  @DisplayName ( "the §4.1 floor of 16 segments is accepted, built either way" )
  void minimumDepthIsAccepted () {
    final Cortex cortex = cortex ();

    assertEquals ( 16, deepName ( cortex, 16 ).depth (), "by successive extension" );
    assertEquals ( 16, cortex.name ( "s0.s1.s2.s3.s4.s5.s6.s7.s8.s9.s10.s11.s12.s13.s14.s15" ).depth (),
      "in a single operation" );
  }

  @Test
  @DisplayName ( "every hierarchy walk survives a depth no single stack frame could" )
  void deepHierarchyWalksAreIterative () {
    final Name name = deepName ( cortex (), DEEP );

    assertEquals ( DEEP, name.depth () );
    final long streamed = assertDoesNotThrow ( () -> name.stream ().count (), "stream" );
    assertEquals ( DEEP, streamed );

    final int folded = assertDoesNotThrow (
      () -> name.foldTo ( x -> 1, ( acc, x ) -> acc + 1 ), "foldTo" );
    assertEquals ( DEEP, folded );
    assertDoesNotThrow ( () -> name.path ().length (),           "path()" );
    assertDoesNotThrow ( () -> name.path ( '/' ).length (),      "path(char)" );
    // the one that recursed
    assertDoesNotThrow ( () -> name.path ( segment -> segment ).length (), "path(Function)" );
  }

  @Test
  @DisplayName ( "a mapped path is the mapper applied once per segment, root first" )
  void mappedPathAppliesMapperOncePerSegment () {
    final Cortex cortex = cortex ();
    final Name name = cortex.name ( "a" ).name ( "b" ).name ( "c" );

    assertEquals ( "a.b.c", name.path ( segment -> segment ).toString () );
    assertEquals ( "A.B.C", name.path ( String::toUpperCase ).toString () );
  }

  @Test
  @DisplayName ( "a deep subject path walks iteratively too" )
  void deepSubjectPathIsIterative () {
    final Cortex cortex = cortex ();
    // A circuit's subject encloses the cortex subject; the name carries the depth.
    final var circuit = cortex.circuit ( deepName ( cortex, DEEP ) );
    try {
      assertDoesNotThrow ( () -> circuit.subject ().path ().length (), "subject path()" );
    } finally {
      circuit.close ();
    }
  }
}
