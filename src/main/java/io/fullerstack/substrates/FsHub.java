package io.fullerstack.substrates;

import java.util.ArrayList;

/// Subscriber management hub — holds the subscription roster and version counter.
///
/// One hub per conduit. Tiny object — fits in one cache line.
/// The version counter is checked by channels on each emission (§7.6.2).
/// Channels that detect a mismatch rebuild their downstream receptor list.
///
/// The roster is keyed by **subscription**, not subscriber: §7.3 invokes the callback
/// "exactly once for that subscription/channel pair" and §7.5 makes each subscribe "an
/// independent subscription instance", so two subscriptions made from one subscriber
/// are two roster entries with two callbacks and two windows. Keyed by subscriber, a
/// close-then-resubscribe with no emission in between left the old pair's receptors in
/// place and never ran the new pair's callback.
///
/// All fields are circuit-thread-only — no synchronization needed.
final class FsHub < E > {

  /// Version counter — incremented on subscription add/remove.
  int subscriberVersion;

  /// Active subscriptions, in registration order.
  ArrayList < FsSubscription > subscribersList;

  /// Cached snapshot — rebuilt when version changes.
  FsSubscription[] subscribersSnapshot = EMPTY;

  private int snapshotVersion = -1;

  private static final FsSubscription[] EMPTY = new FsSubscription[0];

  void addSubscription ( FsSubscription subscription ) {
    if ( subscribersList == null ) {
      subscribersList = new ArrayList <> ();
    }
    subscribersList.add ( subscription );
    subscriberVersion++;
  }

  void removeSubscription ( FsSubscription subscription ) {
    // The version moves only when the roster does: a close job for a subscription whose
    // registration never ran removes nothing, and must not make every channel rebuild.
    if ( subscribersList != null && subscribersList.remove ( subscription ) ) {
      subscriberVersion++;
    }
  }

  /// Set by the conduit's close job, on the worker, and never cleared. It is the conduit's close
  /// **position** (§9.1): a registration job the worker reaches after it takes no effect. The
  /// conduit's own `closed` flag cannot serve — a caller writes that the moment `close()` is
  /// called, which is ahead of work the same caller queued before it.
  boolean closed;

  /// The conduit's close job: every window on this source ends here (§7.6.1).
  void close () {
    closed = true;
    if ( subscribersList != null ) {
      subscribersList.clear ();
      subscriberVersion++;
    }
  }

  boolean hasSubscribers () {
    return subscribersList != null && !subscribersList.isEmpty ();
  }

  FsSubscription[] ensureSnapshot () {
    if ( snapshotVersion != subscriberVersion ) {
      subscribersSnapshot = ( subscribersList == null || subscribersList.isEmpty () )
                            ? EMPTY
                            : subscribersList.toArray ( EMPTY );
      snapshotVersion = subscriberVersion;
    }
    return subscribersSnapshot;
  }
}
