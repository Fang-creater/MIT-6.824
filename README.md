# MIT-6.824

**Lab 1: MapReduce**
A fault-tolerant distributed MapReduce implementation in Go, based on the Google MapReduce paper. The system supports parallel task execution, worker crash recovery, atomic intermediate data handling, and correct task completion despite delayed or duplicate worker reports.

## Overview

The system consists of a centralized **Coordinator** and distributed **Workers**.

- **Coordinator**: Manages task scheduling, state machine (Map → Reduce → Done), timeout tracking, and fault recovery.
- **Workers**: Pull tasks via RPC over Unix sockets, execute dynamically loaded map/reduce plugins, and write intermediate/final output files.

The implementation passes all official tests:
TestWc, TestIndexer, TestMapParallel, TestReduceParallel, TestJobCount, TestEarlyExit, TestCrashWorker.

## Design

### Task Scheduling & State Machine

The Coordinator runs a three-stage state machine to enforce the barrier between map and reduce phases. Workers use a **pull-based model** to request tasks cyclically and back off when no work is available. A background goroutine detects timed-out tasks (10s) and reassigns them to ensure progress.

### Fault Tolerance & Exactly-Once Execution (Core Contribution)

The most critical challenge was ensuring **exactly-once execution semantics** under worker failures and delays.
In the initial implementation, slow or crashed workers could resume and submit stale results *after* a task had already been reassigned and completed, overwriting valid output and causing correctness failures (exposed in TestJobCount).

To solve this, I designed a **per-task versioning mechanism**:

- The Coordinator assigns a version number to each task on every allocation or timeout reset.
- Workers include the task version in their completion RPC.
- The Coordinator **only marks a task done if the returned version matches the current version**.

This lightweight approach **prevents stale writes from zombie workers** and achieves effective exactly-once execution without distributed locks or consensus protocols.

### Atomic File I/O

Intermediate files are written to temporary files and atomically renamed into place using Unix `rename()`. This ensures reduce workers never observe partial or corrupted files, eliminating the need for locks or checksums in the data path.

### Idempotency & Safety

Map and reduce functions are assumed idempotent, allowing safe task re-execution after timeouts or worker crashes. Combined with version checks and atomic writes, the system remains consistent under duplicate task runs.

## Key Challenges & Lessons

The primary difficulty was implementing **correct task completion under delay and failure**. The version mechanism provided a clean, efficient solution to enforce exactly-once semantics and reject stale worker reports. Performance debugging also revealed that I/O and filesystem behavior can dominate runtime in data-intensive systems.
