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

## 6. Crashed-fiber client-close latency FIXED + restart_after_crash resolved (2026-07-11)

Root-caused on EC2 (c7i.4xlarge, merged HEAD, libxtc v1.12.0).  The "~71s" (and
~20s on faster core storage) client hang after a fiber SIGSEGV was ENTIRELY the
kernel writing a whole-process multithreaded core dump (dozens of carrier
threads x 60MB fiber stacks); the crashing client's socket stayed open until the
process finished dumping.  A/B: with cores off, crash->psql-return dropped from
~20s to 0.0085s.

Fix (commit 43c4ac752e8): drop RLIMIT_CORE to 0 at threaded carrier start so the
re-raised uncontainable fault terminates the process instantly and closes all
client sockets at once.  PG_XTC_ALLOW_CORE=1 keeps cores for debugging.  Verified
on EC2: default -> ~8ms fail-stop, no core; env set -> core preserved; normal
threaded server still starts; TAP 010 dropped 83s -> 11.75s, 8/8.

restart_after_crash: resolved as a documented process-lifetime exception, not an
in-process fix.  The postmaster is a thread in the same (corruptible) address
space as the carriers, so it cannot survive a genuine memory-corrupting crash to
run in-process HandleChildCrash re-init -- fail-stop is the only correct
behavior.  Restart is external-supervision territory (systemd Restart=on-failure
acts on the death-by-signal exit); a future in-tree separate watchdog/control
process (candidate: libxtc xtc_xproc) could re-exec the server.  Documented in
MULTITHREADED_PLAN.md "Threaded crash policy and restart_after_crash".

## 7. Full-strictness ASan + TSan investigation (2026-07-12, libxtc v1.13.0)

libxtc v1.13.0 added the sanitizer fiber-switch annotations we requested, so
this closed the Session-5 sanitizer gap -- but only for ASan; TSan needs a
different API.

### ASan with detect_stack_use_after_return=1 -- DONE, found + fixed one real bug
Built an ASan-instrumented libxtc (configure CFLAGS=-fsanitize=address) + an
ASan PG, substituted the ASan libxtc at runtime (soname match), and ran the
carrier TAP with the FULL-strictness option the Session-5 pass had to disable.
It caught a genuine stack-use-after-return: the pooled input buffer was
stack-allocated and a query-string pointer into it escaped via
debug_query_string, read by EmitErrorReport() after the step frame unwound on an
error longjmp.  Fixed in 2a8dabf1ad0 (initStringInfo -> MessageContext, like
upstream).  After the fix: 007 (40/40) + 010 (6/6) run under
detect_stack_use_after_return=1 with 0 AddressSanitizer errors.

### TSan -- STILL BLOCKED, needs libxtc __tsan_*_fiber (clang-only)
The v1.13.0 annotations use the ASan "stack switch" API
(__sanitizer_start/finish_switch_fiber).  TSan does NOT implement that; it needs
the distinct fiber-IDENTITY API __tsan_create_fiber / __tsan_switch_to_fiber /
__tsan_destroy_fiber, and only clang's compiler-rt provides it (gcc's libtsan.so
exports neither).  A gcc -fsanitize=thread build fails at runtime with
"undefined symbol: __sanitizer_start_switch_fiber".  Requested the follow-up
libxtc change (clang-guarded __tsan_*_fiber calls around the coro switch) in
/tmp/libxtc-tsan-fiber-api-request.md.  TSan-on-the-carrier is deferred until
libxtc adds it AND we build the whole stack with clang; recorded as a concrete
unblock, not a vague "where feasible".

## 8. TSan (v1.16.0 has the fiber API) — code-unblocked, run blocked by nix glibc skew (2026-07-12)

libxtc v1.16.0 landed the __tsan_*_fiber annotations we requested (XTC_TSAN_FIBERS
guard in coro_fctx.c/coro_uctx.c, clang-only via __has_feature(thread_sanitizer)):
verified __tsan_create_fiber/__tsan_switch_to_fiber/__tsan_destroy_fiber wired
around every coro switch, with a captured scheduler fiber for switch-back.  So
the CODE gap is closed.

Built a clang-TSan libxtc via `nix develop .#clang` (CC=clang 21.1.8,
-fsanitize=thread).  It configures/compiles, but the TSan RUNTIME cannot execute
on this AL2023 box under nix: a trivial `clang -fsanitize=thread hi.c` binary
dies at load with
  symbol lookup error: .../glibc-2.40/libc.so.6: undefined symbol:
  __nptl_change_stack_perm, version GLIBC_PRIVATE
The symbol exists in nix glibc-2.40 but under a different GLIBC_PRIVATE version
tag than clang's compiler-rt (libclang_rt.tsan, built for a different glibc
private ABI) requests.  This is a nix-clang-rt <-> glibc version-tag skew
(toolchain-env), NOT a libxtc/PG code problem, and NOT gcc's missing-fiber-API
problem (that's fixed).

Deferred: the TSan RUN needs a glibc-consistent clang stdenv (match compiler-rt
to nix glibc, or run TSan outside the nix devshell against a system clang+glibc).
Recorded as an environment blocker with a concrete cause; the annotations are
proven present so the run will work once the toolchain is consistent.

## 9. Broad ASan regression attempt — one lead, needs clean re-run (2026-07-12)

Tried running the full core regression (parallel_schedule) under gcc-ASan
(detect_stack_use_after_return=1) + gcc-ASan libxtc in pooled mode, to hunt more
SUAR/UAF-class bugs like the one fixed in 2a8dabf1ad0.

The run was MISCONFIGURED: threaded_pooled.conf did not engage the pooled
scheduler under pg_regress ("pooled protocol scheduler enabled" never logged),
and the postmaster emitted 100x "could not fork new process for connection:
Function not implemented" -- i.e. threading was half-on (multithreaded=on but
connections hit the fork path, refused by the once-a-carrier-started guard).
Same pg_regress-vs-threaded-config friction seen earlier; the meson TAP harness
handles this correctly but a bare pg_regress --temp-config does not fully.

In that degraded state, ONE cassert assertion fired once:
  TRAP: failed Assert("dlist_is_empty(&session->plan_cache.saved_plan_list)")
  backend_runtime_teardown.c:1247, in PgSessionResetPlanCacheClosedState
  <- PgBackendExitCleanup <- PgBackendExit
i.e. a session was reset/closed with SAVED cached plans still on
saved_plan_list.  Saved plans (SaveCachedPlan, plancache.c) live in
CacheMemoryContext (backend-lifetime) and must be dropped before a POOLED
session resets its plan-cache state for reuse; a process backend never hits this
because the plans die with the process.

Status: a LEAD, not a confirmed pooled bug -- it fired under a half-threaded
misconfiguration, so it must be reproduced under a correctly-pooled run (via the
meson TAP harness or a fixed temp-config) before fixing.  Recorded for the next
hardening pass.  ACTION: add a TAP case that opens a pooled session, prepares +
executes a statement (populating saved_plan_list), then closes the session, and
assert clean teardown; if it reproduces, drain saved plans in the pooled
session-close path before PgSessionResetPlanCacheClosedState.
