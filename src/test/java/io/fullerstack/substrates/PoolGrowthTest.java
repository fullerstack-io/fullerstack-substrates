package io.fullerstack.substrates;

import com.sun.management.ThreadMXBean;
import io.humainary.substrates.api.Substrates.Cortex;
import io.humainary.substrates.api.Substrates.Name;
import io.humainary.substrates.api.Substrates.Pool;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;

import static io.humainary.substrates.api.Substrates.cortex;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// A pool that resolves many distinct names — a conduit whose channels are price levels, instruments,
/// sessions — must cost O(1) per new name, not O(names so far). Each new name used to copy the whole map,
/// O(n²) over the pool's life: about n²/2 entries allocated, gigabytes at 20,000 names.
class PoolGrowthTest {

  @Test
  @DisplayName ( "resolving n new names allocates O(n), not O(n²)" )
  void manyNamesCostLinearAllocation () {
    Cortex cortex = cortex ();
    int n = 20_000;
    Name[] names = new Name[n];
    for ( int i = 0; i < n; i++ ) names[i] = cortex.name ( "pool.growth.p" + i );   // interned before measuring
    Pool < Name > pool = cortex.pool ( name -> name );
    ThreadMXBean mx = (ThreadMXBean) ManagementFactory.getThreadMXBean ();
    long before = mx.getCurrentThreadAllocatedBytes ();
    for ( Name name : names ) pool.get ( name );
    long allocated = mx.getCurrentThreadAllocatedBytes () - before;
    for ( Name name : names ) assertSame ( name, pool.get ( name ), "each name resolves to its own value" );
    assertTrue ( allocated < 64L << 20, "resolving " + n + " names allocated " + ( allocated >> 20 ) + " MB" );
  }

}
