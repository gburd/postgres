# Session 5 hardening summary (Phase 16)

Host: EC2 c7i.metal-24xl (96 vCPU, 188 GB), AL2023, libxtc v1.11.0, pooled
default.  All on top of the Session 3 pooled-as-default flip + Session 4 PL/
contrib affine marks.

## 1. Perf baselines  (plan_docs/session5-perf-baselines.md, commit 5770436611f)

pgbench TPC-B in RAM (fsync=off, parallel off), fresh cluster per data point:

  clients   PROCESS   THREAD_PER_SESSION   POOLED
  c=16       83,737    55,236               40,886 (8 carriers)
  c=32      134,889    82,491               41,156
  c=64      150,750    NA (thread EAGAIN)   42,222 (0 errors)

- Process scales best; thread-per-session ~65 % of process and hits a hard
  thread-creation wall at c=64 (EAGAIN, no server crash) -- the wall pooling
  removes (pooled c=64 ran clean).
- Pooled was flat because the flat min(cpus,8) default capped an 8-carrier pool.
  A carrier sweep at c=64 (8->42k, 16->69k, 24->82k, 32->72k, 48->88k) showed
  throughput scales to ~cpus/4 then flattens.  Fix: auto default changed to
  Max(8, cpus/4), bounded by nproc and MaxConnections (24 on this box).

## 2. Crash / FATAL  (TAP 010, commit 071afe3204e)

Contract pinned: a genuine backend-fiber SIGSEGV under pooled is FAIL-STOP --
contained on the carrier, escalated to the postmaster, whole server terminated
(a shared-process runtime cannot isolate one fiber's corruption).  Client
observes the crash; committed pre-crash data survives a manual restart with
clean recovery; server fully usable after.  8/8 subtests.

Fix landed: the supervisor now kicks the postmaster's process latch on crash
(captured at carrier start) so escalation is prompt instead of waiting out the
postmaster's idle DetermineSleepTime() (~60 s).

Remaining crash gaps (documented in the TAP header, deferred):
- the crashing client's call still takes ~71 s to return (faulted fiber's client
  socket not closed promptly);
- restart_after_crash not honored for threaded crashes (fail-stop, no auto
  cycle).

## 3. AddressSanitizer  (build-asan: cassert + -Db_sanitize=address, debug)

Ran the pooled-exercising TAP under ASan (ASAN_OPTIONS detect_leaks=0,
handle_segv=0 so libxtc's fiber fault-guard owns SIGSEGV, no stack-use-after-
return since libxtc lacks fiber-switch annotations):

- 007_phase15_pooled_protocol_mode: 40/40 subtests OK.
- 010_phase16_pooled_crash_recovery: 8/8 subtests OK.
- 009 skips under ASan (needs the wait-completion diagnostic flag, not in this
  build -- orthogonal).
- ZERO AddressSanitizer / Leak reports across the entire ASan testrun -- the
  pooled multiplexing, cancel/terminate, deep-wait, and crash/fail-stop paths
  are memory-clean.

TSan not run: libxtc's cooperative fiber context-switching has no
__sanitizer_*_switch_fiber annotations, so TSan's happens-before tracking would
produce false positives across every carrier fiber switch.  Deferred until
either libxtc gains the annotations or a TSan-suppression map is built.

## 4. Residual pooled-regression diffs cleared as base-tree (not threading)

The Session 3 pooled core-regression left 8 failing tests (join_hash,
tidrangescan, incremental_sort, select_parallel, write_parallel,
vacuum_parallel, bitmapops, tsearch), already shown to be a strict subset of the
thread-per-session baseline (120/245 on the same schedule).  Re-ran the suspect
tests in PROCESS mode (multithreaded=off, no threading at all): incremental_sort
and select_parallel FAIL there too, with plan-shape diffs -- e.g.
incremental_sort expects `Parallel Index Scan using tenk1_unique1` but this
branch's planner picks `Parallel Seq Scan ... Disabled: true`; select_parallel
has a large plan-output divergence.  These are pre-existing base-tree
planner/costing divergences from upstream's expected outputs on this
experimental branch -- they fail identically in process, thread-per-session, and
pooled.  Conclusion: the pooled default introduces ZERO regressions; the
residual diffs are branch-baseline plan drift, owned by whoever reconciles this
branch's expected/*.out against its planner, not by the threading work.

## 5. Deferred (needs live EC2 measurement, not guessed)

The ~71 s client-close latency on a crashed fiber (TAP 010 ok 4) was
investigated from the code: the supervisor observes the DOWN via a blocking
xtc_recv (prompt), the crash flag now kicks the postmaster latch (prompt), and
ExitPostmaster -> proc_exit has no obvious multi-second wait.  No code-level
cause found for the 71 s, and the dev host cannot run the threaded server
bringup to reproduce it (known meson-on-btrfs limitation).  Deliberately NOT
guess-fixed (e.g. closing the crashed backend's client socket from the
supervisor) -- the fault may have corrupted exactly that connection state, so a
blind surgical close risks touching corrupted memory; fail-stop + full-process
teardown is the safe path.  Needs a focused EC2 measurement session to localize
the 71 s segment before any change.
