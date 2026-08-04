# libxtc Foundational Fusion Roadmap (guiding thought, 2026-07-24)

## Directive

Lean fully into libxtc — its primitives AND its methodology — across ALL of
PostgreSQL, starting with the foundational, cross-cutting primitives (logging,
stats, locks, ...) and working outward. This elevates Phase 18 ("libxtc
Deduplication And Fusion") from a *performance lever that may proceed in
parallel* to the **leading guiding thought** for the branch's evolution.

This is NOT a big-bang cutover. It is aggressive, ordered, one-primitive-at-a-
time fusion, each increment A/B-measured neutral-or-better on
check-threaded-pooled, each keeping a fallback until proven, and each preserving:
- process mode as a permanently supported backend model (byte-for-byte);
- the process-lifetime exceptions (postmaster/control-plane, single-user,
  bootstrap, crash-escalation);
- gmake check + check-threaded + check-threaded-pooled green.

It supersedes the "libxtc is merely a pluggable substrate" framing. The governing
question is "where does fusing with libxtc make PostgreSQL faster AND simpler
than the fork model." The 2026-07-24 metal A/B proved the thread-explosion fix
took OLTP VU=192 to 98.6% of fork and beat fork on hot-update@192 — the fusion
hypothesis is paying off; this roadmap systematizes the rest.

## Methodology to adopt (not just the primitives)

libxtc's *methodology* is as much the target as its code:
1. **One-behaviour-at-a-time, A/B'd, fallback kept.** Never replace two things at
   once; never delete the hand-rolled version until the xtc version is proven
   neutral-or-better and green.
2. **DST-visibility as a design goal.** Every retained raw pthread is invisible
   to libxtc's Deterministic Simulation Testing and reintroduces real-hardware
   nondeterminism (the "lucky timing window" flake class — nearly every hard bug
   this project hit lives at the pthread<->fiber boundary). Moving a primitive
   onto an xtc loop makes it DST-replayable. Shrink the boundary in dependency
   order; do not convert a sync primitive to xtc before its users run on a loop.
3. **Observability-first.** libxtc instruments itself (counters/gauges/
   histograms, per-loop stats, structured log, xtc_dump). Surface these in-tree
   BEFORE the perf work, so every subsequent fusion increment is measurable via
   SQL/log instead of an external perf+EC2 run each time.
4. **Prove equivalence, do not assume it.** For any primitive with load-bearing
   PG semantics (lock fairness/ranking/holdoff, mctx reset-callback ordering,
   allocator alignment), build a per-primitive equivalence harness first.

## Layers today (what fusion consolidates)

- **carrier layer**: xtc scheduler on a pthread pool (one loop/thread); client
  backends are fibers; but the non-fiber fallback, aux worker families, and
  pooled-protocol carriers are still raw pg_thread_create pthreads.
- **synchronization**: in-backend work uses PG shmem LWLock/spinlock/latch/CV
  (correct for both models); threaded-runtime plumbing still has raw
  pthread_mutex/cond (pooled-protocol queue, PMChild/GUC locks, ps_status,
  pg_locale, reloptions, backend registry).
- **I/O**: io_method=xtc routes fiber-backend data-file reads through xtc_aio;
  everything else uses PG's own AIO/pread/WAL.
- **observability**: essentially NONE of libxtc's is surfaced (only
  xtc_exec_loop_stats -> xtc_pg_carrier_total_steals(), test-only).

## libxtc surface relevant to the foundation (verified in XTC_ROOT/src/inc, v1.31.0)

- **xtc_log.h** (9 fns): xtc_log_create/destroy, set_floor, set_default,
  log_default, log_write/vwrite, drain, drop_count. Levels + async drain +
  drop-count (backpressure-aware structured logging).
- **xtc_stats.h** (17 fns): counter/gauge/histogram create+ops+read,
  xtc_metrics_iterate(visit_fn), xtc_metrics_dump_prometheus(fd). Cache-line-
  local atomic counters; quantile histograms.
- **xtc_exec.h**: xtc_exec_loop_stats(exec, idx, {tasks_run, steals}) per loop;
  set/get_eager_rebalance; set/get_steal_backoff (v1.31.0).
- **xtc_lwlock.h** (11), **xtc_lrlock.h** (15, reader/writer), **xtc_lockmgr.h**
  (16, + xtc_lockmgr_stat), **xtc_sync.h** (43): the lock/CV family.
- **xtc_dump.h**: xtc_dump(fd) full runtime dump (crash diagnostics).
- **xtc_reg.h** (13), **xtc_svr.h**/**xtc_orc.h**/**xtc_pool.h**/**xtc_fsm.h**:
  the OTP behaviours for the later supervision/pool/registry fusion.

## Ordered increments (foundation first, outward)

Dependency-ordered; each is its own commit(s) + two-reviewer gate + A/B where it
touches a hot path. Perf-neutral infra increments (observability) need only the
green gate; hot-path replacements (locks, latch/CV) need the A/B.

### F0. Observability foundation (START HERE — perf-neutral, unlocks the rest)
- **F0a. xtc_log -> elog bridge (GUC-gated).** Install an xtc_log sink (via
  xtc_log_set_default) whose write callback routes into PG's ereport/elog at a
  mapped level, gated behind a developer GUC (e.g. `xtc_log_min_messages`),
  default off/high-floor. So libxtc-internal diagnostics (io-wq cap -ENOSYS on
  old kernels, wake-path warnings, DST hooks) become visible in the PG log.
  Threaded-only; process mode never installs it. Carrier-startup timing: install
  after PG logging is up, on the scheduler thread.
- **F0b. pg_stat_xtc_carriers view.** A SRF + system view exposing per-loop
  xtc_exec_loop_stats (tasks_run, steals), plus total steals, loop count,
  carrier count, idle/eager-rebalance/steal-backoff state. Promote the existing
  test-only total_steals into a real, documented, non-test SQL surface.
- **F0c. xtc_metrics passthrough (optional, same increment).** Expose
  xtc_metrics_iterate as a pg_stat_xtc_metrics SRF (name/type/value) so any
  counter/gauge/histogram libxtc or our carrier code registers is queryable;
  and an admin function wrapping xtc_metrics_dump_prometheus(fd) for scrape.
- **F0d. xtc_dump on threaded crash.** In the fault-guard path (pg_xtc_carrier.c
  fault handler), call xtc_dump to a log fd before fail-stop, so a threaded crash
  leaves a libxtc-runtime dump (loops, procs, mailboxes) alongside the backtrace.

### F1. Carrier/runtime counters (build ON F0)
Instrument our own carrier/scheduler hot paths with xtc_stats counters
(sessions leased, protocol parks, migrations, GUC-amutex contention, wake
deliveries) so the metrics view shows PG-side runtime health, not just libxtc's.
This makes F2+ measurable in-tree.

### F2. Lock/CV plumbing dedup (the "locks" foundation — hot path, A/B required)
Replace the RAW pthread_mutex/pthread_cond in the threaded-runtime plumbing with
xtc primitives, in dependency order (only where the users run on a loop):
- pooled-protocol queue mutex+cond -> xtc_chan / xtc mailbox (producers are the
  postmaster thread + carriers; consumers are carriers on loops).
- GUC amutex, PMChild, ps_status, pg_locale, reloptions guards -> xtc_lwlock /
  xtc_sync, case by case, each proven to preserve holdoff/fairness.
DO NOT touch in-backend shmem LWLock/CV yet (correct for both models; the deeper
Latch/LWLock/CV-onto-xtc fusion is F4, gated on more of the runtime being fibers).

### F3. Steal-backoff + parked-carrier polling (the queued perf work, now measured)
Wire xtc_exec_set_steal_backoff; tame the parked-carrier CFS newidle tax
(update_sg_lb_stats 41% on pgbench select@384). Measured via F0/F1 views + A/B.

### F4+ (later, per existing Phase 18): Latch/LWLock/CV/AIO onto xtc primitives;
xtc_pool for carrier/worker pools; xtc_svr/xtc_orc supervision; xtc_reg backend
registry; xtc_xproc watchdog. Each large, each gated, each A/B'd. MemoryContext
-> xtc_mctx stays defer-until-proven; shared-memory -> xtc is a NON-GOAL (only
xtc_slab INSIDE a PG-owned DSM region is sanctioned).

## Invariants for every increment (the guardrails)
- Process mode byte-for-byte; gate behind USE_XTC_CARRIER / multithreaded /
  xtc_in_backend_fiber as appropriate.
- Two independent adversarial reviews before landing.
- A/B on check-threaded-pooled for hot-path changes; green gate for infra.
- Keep the fallback until the replacement is proven; one behaviour at a time.
- Prefer surfacing DST-visibility gains explicitly (which pthread this removes).

## Status
- **F0b DONE + cleared** (2 reviews SHIP): pg_stat_xtc_carriers view — per-loop
  tasks_run/steals + eager_rebalance/steal_backoff, from xtc_exec_loop_stats.
- **F0a DEFERRED (no live emitter in v1.31.0).** Built the xtc_log->elog sink
  bridge, found it captures nothing: libxtc's only xtc_log_default emitter
  (__os_tuning_check) is neither auto-invoked NOR ABI-exported (the xtc_*-only
  symbol gate drops __os_*). Reverted rather than ship speculative infra with no
  producer. Outstanding libxtc question: /tmp/libxtc-log-emitter-gap-question.md
  (export+auto-run the tuning check, or route internal diagnostics through
  xtc_log). Revisit when libxtc answers / a future release adds an emitter.
- **F3 DONE (steal-backoff), pulled ahead of F1/F2** because it delivers
  immediate, now-MEASURABLE (pg_stat_xtc_carriers.steal_backoff) perf value
  against the benchmark's parked-carrier CFS-tax, and needs no new observability
  emitter.
- **F1 IMPLEMENTED (pending 2 reviews): pg_stat_xtc_runtime view** — a focused
  set of 7 libxtc xtc_stats counters over OUR OWN pooled-protocol carrier hot
  paths (which libxtc's per-loop xtc_exec_loop_stats do NOT see): sessions
  leased, sessions resumed, protocol parks, wakes delivered, carriers started,
  process fallbacks, queue waits.  Registered once at pooled-carrier bringup on
  the scheduler thread; each hot-path bump is one cache-line-local atomic add
  (xtc_counter_add).  Surfaced by pg_stat_get_xtc_runtime() -> pg_stat_xtc_runtime
  (counter,value rows), empty in process mode and any threaded run that never
  stood up a pooled carrier (counters never register there -> hot paths
  byte-for-byte).  Validated on EC2: process regress 245/245, backend-runtime
  suite 15 OK / 2 intentional skips, pooled load shows parks==resumes==wakes and
  carriers_started==pooled_protocol_carriers; process mode returns 0 rows.
  Next: F2 (lock dedup).
- Prior fusion wins already landed: eager-rebalance (v1.27), io-wq cap +
  right-sized executor (v1.31.0 + loop-count fix) — see the 2026-07-24 metal A/B.

### F1 LANDED (2026-08-03, origin/xtc 53463e8e30d)
pg_stat_xtc_runtime view: 7 libxtc xtc_stats counters on the pooled-protocol
carrier hot paths (sessions_leased/resumed, protocol_parks, wakes_delivered,
carriers_started, process_fallbacks, queue_waits). Two independent EC2 reviews
SHIP-WITH-CHANGES -> applied: rsinfo moved inside #ifdef (warning-clean process
build), and register() moved before pg_thread_create (publish-before-inc).
Reviewer opcode-proved the non-carrier hot path is byte-identical; 245/245
process regress; counters observable via SQL under load. Second fusion increment
(after F0b).

### walsender + bgworker threaded crash family FIXED (2026-08-03, origin/xtc 658dff9abc5)
The ~40 threaded-mode failures the EC2 A/B surfaced, root-caused + fixed on EC2:
- walsender: this branch NULLs xlogreader at StartLogicalReplication end (threaded
  session reuse), tripping upstream's Assert(xlogreader != NULL). Fix scopes the
  assert to REPLICATION_KIND_PHYSICAL (logical reader is owned/freed by the
  decoding context; physical reader stays live). Currently latent (logical repl
  blocked earlier by the process-only module guard) but correct + necessary.
- two real double-free SIGSEGVs (REPACK worker_dsm_segment; dsm_registry DSA/dshash)
  in the closed-state resets: re-detach after dsm_backend_shutdown already freed
  the segment at proc_exit. Coordinator review-fix converted the initial
  unconditional drops to `if (!PgBackendExitInProgress())` guards, matching the
  established launcher-DSA / LISTEN-NOTIFY-DSA precedent in the same file (detach
  only on a live session reset; skip on proc_exit where dsm_backend_shutdown owns
  it; dsm_registry_dsa is dsa_pin_mapping'd so skipping-on-exit leaks nothing).
- The other "segfaults" (pg_prewarm/test_aio/test_custom_stats/advice modules)
  were NOT crashes -- clean FATAL guard-rejections (model mismatch, Phase 16
  module-marking surface). Left as-is (clean reject, not a crash).
Validated on EC2: build 0/0, process regress 245/245, crash-repro suites clean
(no SIGSEGV with the guard), physical repl recovery/001_stream_rep OK. Reviewed
by coordinator source analysis (both background reviewers aborted mid-run).

STILL OPEN (documented, separate follow-ups -- pre-existing, proven on the clean
unmodified tree in PROCESS mode, NOT this family):
- ResetExtensionSiblingCache pfree-of-freed CacheMemoryContext entries at teardown
  (intarray, pg_stash_advice SIGSEGV at shutdown), teardown.c ~757.
- pgoutput_relation_sync_cache DynaHash assert (hashp->alloc == DynaHashAlloc)
  in PgSessionResetLogicalReplicationClosedState, teardown.c ~1070
  (subscription/recovery/pg_combinebackup) -- the "DynaHashAlloc" family.
- ~92 exit-status-29 = process-only module load-rejections (Phase 16 module
  marking). recovery/pg_basebackup shutdown-hang + fork-fallback ENOSYS = separate.

### F0a LANDED (2026-08-04, origin/xtc a2ff85921d0) -- unblocked by libxtc v1.32.0
The xtc_log->elog bridge, deferred at v1.31.0 (no reachable emitter), is now live:
libxtc v1.32.0 (commit a18c6d2) exports xtc_tuning_check() -- the public entry to
the host-tuning advisor -- exactly what /tmp/libxtc-log-emitter-gap-question.md
asked for. F0a installs an xtc_log default sink (xtc_pg_log_sink -> write() to
STDERR with an [xtc <LEVEL>] prefix, ereport-free/async-safe) at carrier bringup
behind the developer GUC xtc_log_to_server (default off, PGC_POSTMASTER,
threaded-only), calls xtc_tuning_check(), then xtc_log_drain() to flush.

REVIEW CAUGHT A REAL BUG (drain): the first F0a filled libxtc's log RING BUFFER
but never drained it (the sink fires only from xtc_log_drain, whose sole libxtc
caller is xtc_log_destroy, which we deliberately never call for the
process-lifetime log) -- so ZERO advisories reached the log despite a clean boot.
Fixed with one line: (void) xtc_log_drain(pglog) after xtc_tuning_check().
Re-validated on a mis-tuned EC2 box: the server log NOW shows
"[xtc INFO] [tuning] vm.swappiness is above 10 ..." + "sched_autogroup_enabled is 1 ..."
at carrier bringup. Build 0/0, process regress 245/245, threaded 18/0/2,
GUC-off + process-mode both silent+clean. Fourth fusion increment (F0b, F1, F2, F0a).

COORDINATOR PROCESS NOTE: first landed the pre-drain-fix commit by mistake
(ff-only aborted, push sent stale local HEAD); caught it by verifying
origin/xtc's xtc_log_drain count == 0, force-pushed the correct a2ff85921d0.
Standing rule reinforced: after every land, verify origin/xtc SHA == the exact
EC2-validated commit.

### F4+ AUDIT: primitive dedup is COMPLETE (2026-08-04) -- see MULTITHREADED_F4_FUSION_AUDIT.md
A rigorous source audit of ALL wait/block primitives reachable from a
client-backend fiber hot path found the "Latch/LWLock/CV/AIO onto xtc" fusion is
ALREADY DONE -- there is NO carrier-blocking gap.  Every primitive yields its
carrier OS thread via one of two seams:
  - epoll-fd park (xtc_pg_wait_fd): Latch/WaitEventSet, and everything that waits
    via WaitLatch -- ConditionVariable, ProcSleep/heavyweight locks,
    ProcWaitForSignal, XactLockTableWait, buffer-IO CV, SyncRep, ProcSignalBarrier.
  - eventfd-semaphore park (ProcWaitOnSemaphore): LWLock contended sleep,
    ProcArrayGroupClearXid, WAL flush/insertion (LWLockWaitForVar).
  - xtc_aio: fiber data-file read/write (io_method=xtc).
This is WHY the branch reaches 98.6% of fork -- the hot-path primitive fusion is
substantially complete.  The risky "convert Latch/LWLock to xtc" work does NOT
need redoing.  Intentional non-yields (spinlock pg_usleep, standby throttle) left
as-is.

xtc_pool for the carrier pool: REJECTED (no code).  xtc_pool is a bounded set of
caller-owned resources that FIBERS check out (checkout blocks a fiber) and
return; our carriers are raw pthreads spawned once and run forever (no
fiber-checkout, no checkin).  The carrier spawn-half is xtc_svr/xtc_orc
(supervisor) territory, and the session queue is already the F2-fused
xtc_amutex+xtc_notify producer/consumer -- neither is an xtc_pool borrow/return
slot pool.  Left the hand-rolled bookkeeping.

REMAINING genuine F4+ fusion (structural, not primitive; larger, lower
benefit-per-risk, design-first): xtc_svr/xtc_orc supervision (carrier/worker
spawn + restart), xtc_reg backend registry, xtc_xproc watchdog.  MemoryContext
-> xtc_mctx stays defer-until-proven; shared-memory -> xtc is a NON-GOAL.
