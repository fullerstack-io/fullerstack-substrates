package io.fullerstack.substrates;

/// One unit of admitted work, and its own queue node — the alternative
/// granularity to [QChunk], which is a node holding 128 entries.
abstract class Job {

  /// Deliberately **not** `volatile`.
  ///
  /// Both queues get their cross-thread ordering from the access mode at the
  /// point of use, not from this declaration: [JobQueue] publishes with
  /// `NEXT.setRelease` and reads with `NEXT.getAcquire`, and [JobQueueSteal]
  /// writes this field before a CAS whose release ordering publishes both
  /// together. Everything else touching it — unlinking a consumed node,
  /// reversing a stolen chain — runs on a node the consumer privately owns.
  ///
  /// Declaring it `volatile` cost a StoreLoad fence on every plain-source
  /// store: `lock addl $0, (%rsp)` per item in JobQueue's unlink and three
  /// times per item on the steal path. perfasm measured atomics and fences at
  /// 40.89% of the steal profile against 8.22% for JobQueue, and that fence
  /// was a third of the gap.
  Job next;

  abstract void run ( FsCircuit circuit );
}
