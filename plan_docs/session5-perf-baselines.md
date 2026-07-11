# Session 5 perf baselines: process vs thread-per-session vs pooled

Host: EC2 c7i.metal-24xl (96 vCPU, 1 NUMA node, 188 GB), AL2023.
Build: build-ec2 (production, no wait-completion diagnostic flag), libxtc v1.11.0,
pooled default flip + Session 3/4 affine marks in.
Workload: pgbench TPC-B-like, scale 50, -M prepared, 20 s, fsync=off,
synchronous_commit=off, shared_buffers=8GB, max_parallel_workers_per_gather=0
(isolates session scheduling from parallel-worker + disk cost).  Data on tmpfs.
Method: ONE fresh cluster per (mode, client-count) data point -- no cross-run
crash-recovery or thread-accumulation contamination (an earlier single-server
matrix gave misleading NA/collapse from exactly that).

## Throughput (tps), failed_tx=0 and 0 server errors unless noted

| clients | PROCESS  | THREAD_PER_SESSION | POOLED (8 carriers) |
|---------|----------|--------------------|---------------------|
| c=16    |  83,737  |  55,236            |  40,886             |
| c=32    | 134,889  |  82,491            |  41,156             |
| c=64    | 150,750  |  NA (see below)    |  42,222             |

## Reading the numbers

1. PROCESS scales with clients (84k -> 135k -> 151k) -- the baseline.

2. THREAD_PER_SESSION runs ~65 % of process throughput (55k, 82k) -- the cost
   of the threaded runtime's per-command scheduling / current-work indirection
   and per-session thread bringup+teardown.  At c=64 it FAILS:
   "could not fork new process for connection: Resource temporarily
   unavailable" (EAGAIN on thread creation) -- 0 server errors / no crash, a
   pure resource limit.  64 near-simultaneous new session OS threads (each with
   a large fiber stack) plus pgbench's own 64 threads exhaust the thread/nproc
   budget.  This is precisely the thread-per-session scaling wall pooling
   exists to remove.

3. POOLED is FLAT at ~41k across c=16/32/64 and, crucially, does NOT hit the
   c=64 thread-exhaustion wall (0 errors) because it uses only its 8 carrier
   OS threads regardless of client count.  The flatness is the pool ceiling:
   for this CPU-bound, in-RAM workload 8 carriers saturate at ~41k tps, so more
   clients do not add throughput.  Pooled trades raw throughput-scaling on a
   few CPU-bound clients for bounded thread/memory use and graceful behaviour at
   high connection counts -- which is its purpose.

## Takeaways / actions

- Correctness: all modes 0 failed_tx / 0 server errors in the ranges that ran;
  no crashes.  The two NA/failure cases were (a) a harness contamination bug
  (fixed by one-cluster-per-datapoint) and (b) a client/OS thread-count resource
  limit in thread-per-session at c=64, not a server defect.

- The pooled auto-carrier default is min(online CPUs, 8).  On a 96-core box the
  hard cap of 8 is what flattens pooled throughput here.  For CPU-bound
  workloads a larger pool would scale further; the cap is conservative to avoid
  a large idle carrier set on many-core hosts.  Candidate follow-up: scale the
  auto pool with cores (e.g. a fraction of nproc with a higher ceiling) rather
  than a flat 8, and/or make the ceiling a GUC-tunable.  Left as a tuning knob,
  not changed blind -- the right ceiling depends on workload mix (CPU-bound vs
  wait-bound) and wants its own sweep.

- Thread-per-session c=64 EAGAIN is a real high-concurrency ceiling for that
  model; pooled is the mitigation and already the default.  For thread-per-
  session deployments at high connection counts, document the nproc/stack ulimit
  requirement.
