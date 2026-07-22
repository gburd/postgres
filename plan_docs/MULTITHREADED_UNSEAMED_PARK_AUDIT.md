# Unseamed-Park Audit (READ-ONLY)

Systematic audit for the "unseamed libxtc park clobbers shared per-OS-thread
state" bug class in the threaded runtime.

- Repo: `/home/gburd/ws/postgres/xtc`, branch `xtc`, HEAD `0580a22bcc5`.
- libxtc: v1.27.0 (`/home/gburd/ws/xtc`, fd7a8f6).
- This is a find-them-all audit. NO code changes were made. Fixes are separate
  reviewed changes.

## The pattern

An unseamed libxtc park (a fiber yields/parks while a libxtc primitive blocks)
leaves SHARED PER-OS-THREAD state live; a sibling fiber time-sharing the same
carrier thread clobbers it; on resume the parked fiber reads corrupted/foreign
state.

Two shared-per-thread live-state families exist in this runtime:

1. **The 6-root bridge** (`PgRuntimeCurrentBridgeState`,
   backend_runtime.c:100) -- `PG_THREAD_LOCAL PG_GLOBAL_CARRIER`. The current
   `runtime/carrier/backend/session/connection/execution` roots that every
   PG accessor resolves through once roots are installed. Protected by the
   **seam** (`PgRuntimeSaveCurrentWork` / `PgRuntimeRestoreCurrentWork`,
   backend_runtime.c:595/607): snapshot the six roots on a stack-local that
   rides with the fiber across a steal, restore on resume.

2. **The early `*_fallback` thread-locals** (pre-install window). When roots
   are NULL, accessors resolve to `PG_THREAD_LOCAL` fallbacks:
   `early_backend_fallback` (backend_runtime_backend.c:69),
   `early_session_fallback` (backend_runtime_session.c:172),
   `early_execution_fallback` (backend_runtime_execution.c:27),
   `early_connection_fallback` (backend_runtime_connection.c:23). The seam
   does NOT cover these (it saves/restores the 6 roots, which are NULL here),
   so a park in this window is UNPROTECTED.

The general lesson: any libxtc parking primitive on a backend-fiber-reachable
path, entered while shared per-OS-thread state is the live source of truth, is
a corruption hazard.

## libxtc parking primitives (confirmed against v1.27.0 source)

Verified from `/home/gburd/ws/xtc/src/inc/xtc_proc.h` and `xtc_sync.h`:

| Primitive | Parks (yields fiber)? | Notes |
|---|---|---|
| `xtc_proc_wait_fd` | YES | fd/mailbox/timeout wait (xtc_proc.h:309) |
| `xtc_proc_sleep` | YES | timer park, "does not block the thread" (xtc_proc.h:312) |
| `xtc_amutex_lock` | YES on contention | uncontended = fast flag, no yield; contended = FIFO park + `xtc_yield` (xtc_sync.h:119) |
| `xtc_recv` / `xtc_recv_*` | YES (timed/blocking) | mailbox wait (xtc_proc.h:236) |
| `xtc_sem_acquire` / `xtc_notify_wait` | YES | not used on backend paths (see below) |
| `xtc_aio_preadv/pwritev/fsync/fdatasync` | YES | park issuing fiber for the IO duration |
| `xtc_yield` | YES | explicit yield; not called directly by PG |
| `xtc_amutex_unlock` | NO | hand-off + wake, never yields the caller |
| `xtc_proc_wake` | NO | cross-thread nudge; never yields the caller |

Migration semantics (xtc_proc.h:81-94): a proc is stolen ONLY at a yield/park
point, never mid-instruction, and only if `xtc_proc_opts_t.migratable == 1`.
So every corruption in this class requires (a) a park and (b) shared live
state at that park; cross-carrier resume additionally requires migratable=1,
but same-thread cooperative interleaving corrupts even while pinned.

## Enumeration of PG park sites (direct + funneled)

Every backend-fiber wait funnels into ONE of these choke points. Grep of
`src/backend` for all `xtc_amutex_lock | xtc_proc_wait_fd | xtc_proc_sleep |
xtc_sem_acquire | xtc_notify_wait | xtc_recv | xtc_yield` found NO direct park
call outside the sites below.

### TABLE A -- park sites

| # | Park site (file:line) | Reachable from backend fiber? | Seamed / safe / HAZARD | Live shared per-thread state | Verdict |
|---|---|---|---|---|---|
| A1 | `xtc_pg_wait_fd` park via `xtc_proc_wait_fd` (pg_xtc_carrier.c:1125) | YES (the universal wait choke point) | SEAMED (save@1124, restore@1136) + affine-depth==0 assert | 6-root bridge | **SAFE** -- seam repoints bridge on resume; rode fiber stack across a steal |
| A2 | `xtc_pg_wait_fd` no-fd timeout via `xtc_proc_sleep` (pg_xtc_carrier.c:1110) | YES | SEAMED (save@1109, restore@1111) | 6-root bridge | **SAFE** |
| A3 | GUC amutex via `xtc_amutex_lock` in `ThreadedGUCLock` (guc.c:183) | YES (command path: set/show/reserve GUC) | SEAMED (save@181, restore@184, verify@185) | 6-root bridge | **SAFE on command path** -- this is bug #1's fix; but see A3' |
| A3' | Same amutex park, reached in the **pre-install window** (guc.c:183 via `InitializeThreadedSessionGUCOptions` guc.c:2705 and `set_config_with_handle` guc.c:4929, called from launch_backend.c:1921-1922) | YES (every backend fiber startup) | **HAZARD** -- seam saves the 6 roots but here they are NULL; live state is the `early_*_fallback` thread-locals, which the seam does NOT cover | `early_execution_fallback` (TopMemoryContext), `early_session_fallback`, `early_backend_fallback` | **KNOWN #3** (concurrent-startup TopMemoryContext corruption) |
| A4 | AIO read/write via `xtc_aio_preadv/pwritev` (method_xtc.c:137/144) | YES (buffered read/write IO) | SEAMED (save@131, restore@150/154, verify@155) | 6-root bridge (incl. PrivateRefCountArray) | **SAFE** |
| A5 | fsync via `xtc_aio_fsync` (fd.c:544) | YES | SEAMED (save@543, restore@545, verify@546) | 6-root bridge | **SAFE** |
| A6 | fdatasync via `xtc_aio_fdatasync` (fd.c:604) | YES | SEAMED (save@603, restore@605, verify@606) | 6-root bridge | **SAFE** |
| A7 | PGPROC semaphore wait via `xtc_pg_wait_fd` in `ProcSemaphoreWaitFiber` (proc.c:2333) | YES (LWLock deep-wait, ProcSleep heavyweight lock, any PGSemaphoreLock) | SEAMED (routes through A1's seamed `xtc_pg_wait_fd`) | 6-root bridge | **SAFE** -- funnels to A1 |
| A8 | WaitEventSet wait via `xtc_pg_wait_fd` in `WaitEventSetWaitBlock` (waiteventset.c:1483) | YES (WaitLatch, ConditionVariableSleep, socket wait, latch wait) | SEAMED (routes through A1's seamed `xtc_pg_wait_fd`) | 6-root bridge | **SAFE** -- funnels to A1 |
| A9 | Pooled-protocol staging wait via `WaitEventSetWait` (postgres.c `PgSessionStagingWaitProtocolRead`) | YES (pooled protocol only) | Park runs with roots DELIBERATELY DETACHED (asserts `CurrentPgBackend==NULL`); routes to A8/A1 seam over a NULL bridge (harmless); resume re-attaches via `PgCarrierAttachBackend` | none (roots detached by design; backend held by the protocol scheduler) | **SAFE for the bridge**; the carrier-affinity *commit* is bug #2 -- see A10 |
| A10 | `PgCarrierCommitProtocolReadPark` carrier commit (postgres.c:7015 -> backend_runtime_backend.c:2433) | YES (pooled protocol only, migratable path) | asserts `carrier == CurrentPgCarrier`, `backend == CurrentPgBackend`, `carrier->current_backend == backend`; PANICs on mismatch | carrier identity (not a per-thread fallback; scheduler affinity) | **KNOWN #2** (protocol-read-park PANIC) -- see analysis below |
| A11 | Supervisor `xtc_recv` / `xtc_send` (pg_xtc_carrier.c:996 + supervisor loop) | NO (supervisor is a bare fiber, never a backend fiber; never brackets affine sections; never installs the bridge) | N/A -- not a backend fiber | none | **SAFE** (out of scope) |

### TABLE B -- shared per-OS-thread state

| Symbol (file:line) | Read across a park by | Written by sibling? | Protected? | Verdict |
|---|---|---|---|---|
| `PgRuntimeCurrentBridgeState` (backend_runtime.c:100) -- the 6 roots | every established backend fiber, at every park | YES (any sibling backend fiber sets its own roots on the same carrier) | YES -- seam (save/restore) at A1-A8; verify-is-self asserts | **SAFE** (bug #1 fix + pre-existing seams) |
| `early_execution_fallback` (backend_runtime_execution.c:27) -- incl. `.memory_contexts` (TopMemoryContext) | startup fiber pre-install (A3') | YES (a sibling startup fiber's `MemoryContextInit` writes the same per-thread fallback) | **NO** -- seam covers the 6 NULL roots, not the fallback | **KNOWN #3** |
| `early_session_fallback` (backend_runtime_session.c:172) | startup fiber pre-install (A3') | YES (sibling GUC/session init) | **NO** | **KNOWN #3** (same window) |
| `early_backend_fallback` (backend_runtime_backend.c:69) | startup fiber pre-install (A3') | YES | **NO** | **KNOWN #3** (same window) |
| `early_connection_fallback` (backend_runtime_connection.c:23) | startup fiber pre-install | possible | **NO** (same window) | subsumed by #3 -- same pre-install-window mechanism, same fix |
| `use_static_guc_defaults_for_initialization` (backend_runtime_session.c:170) | GUC init pre-install | possible during concurrent startup | **NO** | subsumed by #3 -- lives in and is cleared by the same startup window |
| `unpack_sql_state` static `buf[12]` (elog.c:3666) | any elog caller | YES (per-thread, shared by fibers) | safe-by-construction (fill+consume inline, NO yield between; documented invariant elog.c:3652; park-boundary assert would catch a future parking caller) | **SAFE** |
| `retained_top_memory_allocated` (ipc.c:65) -- `PG_GLOBAL_CARRIER` exit accounting | exit-time only | exit-time | carrier-local; touched only in teardown accounting, no park while live | **SAFE** (theoretical only) |
| `xtc_in_backend_fiber` (pg_xtc_carrier.c) `__thread` | wait-boundary decision | set only by the carrier thread; NOT migration-stable | already mitigated: the semaphore wake channel keys off `proc->sem_fiber_backed` (shmem, migration-stable) NOT this TLS (proc.c:2375 comment). No PG code reads this TLS *across* a park to decide correctness. | **SAFE** (already hardened) |
| `xtc_pg_affine_depth` (pg_xtc_carrier.c) `__thread`, assert-only | park-boundary tripwire | per-thread; reset at fiber entry/exit (`xtc_pg_affine_section_reset`) | assert-only; reset on fiber boundaries and FATAL unwinds | **SAFE** |
| macro-generated hot cells/fields `*ThreadRef/*ThreadCell/*ThreadOwner` (backend_runtime.c:108-130) | hot accessors | YES | flushed/reloaded by `PgRuntimeSetCurrentWork` inside the seam restore (they are derived from the 6 roots) | **SAFE** (ride the seam via the roots) |

## The pre-install startup window (the #3 class), step by step

Trace: `xtc_carrier_proc` (pg_xtc_carrier.c:683) -> tree's `backend_thread_entry`
(launch_backend.c:1837). At entry, `PgRuntimeResetThreadForNewBackend`
(launch_backend.c:1859 -> backend_runtime.c:812) sets all six roots to NULL and
`PgRuntimeHotCurrentCellModeState = PG_RUNTIME_HOT_CURRENT_CELLS_FALLBACK`, so
from here until `InstallPgThreadBackendRuntimeState` (launch_backend.c:1924)
every accessor resolves to the shared `early_*_fallback` thread-locals.

The window's operations (launch_backend.c:1917-1924) classified for
(a) can-park and (b) touches-shared-fallback:

| Step (file:line) | Can park? | Touches shared fallback? | Hazard? |
|---|---|---|---|
| `InitializeWaitEventSupport()` (waiteventset.c:274) | No -- pipe/epoll syscalls, no fiber park | carrier-local fds | No |
| `InitProcessLocalLatch()` (miscinit.c:238) | No -- assigns `&LocalLatchData`, `InitLatch` | no | No |
| `MemoryContextInit()` (mcxt.c:349) | No park itself; asserts `TopMemoryContext == NULL` then writes it | **YES** -- TopMemoryContext resolves to `early_execution_fallback.memory_contexts` | **the corruption target** |
| `InitializeTransactionState()` (xact.c:368) | No -- points at `TopTransactionStateData` | minimal | No |
| `InitializeThreadedSessionGUCOptions()` (guc.c:2693) | **YES** -- `ThreadedGUCLock` amutex park (guc.c:2705) | reads/writes fallback GUC/session state | **the parking trigger** |
| `read_nondefault_variables()` (guc.c:7398) | **YES** -- `set_config_with_handle` -> `ThreadedGUCLock` (guc.c:4929) | fallback GUC/session state | **the parking trigger** |
| `InitializeLatchWaitSet()` (latch.c:65) | No -- `CreateWaitEventSet`, no wait | no | No |

The interleaving that corrupts (bug #3): fiber X enters the window, reaches
`InitializeThreadedSessionGUCOptions`, contends the GUC amutex, PARKS. The
carrier loop runs sibling fiber Y, which also enters its window and runs
`MemoryContextInit` -- writing the SAME per-thread `early_execution_fallback`
top_context that X will still read. When X resumes it sees Y's TopMemoryContext
(or Y's later `MemoryContextInit` trips `Assert(TopMemoryContext == NULL)` on
already-populated shared state). Rate observed on pristine HEAD: N>=3 concurrent
connects corrupt; also crashes single-loop at N~6 (cooperative interleaving,
NOT stealing) -- so it bites the pinned runtime today, not only migratable.

No OTHER parking site exists in this window (only the two GUC calls park; the
rest are local setup).

## Bug #2 (protocol-read-park PANIC) -- is it distinct or subsumed by #1?

Bug #2 is a `PANIC` in `PgCarrierCommitProtocolReadPark` (backend_runtime_backend.c:2433)
with `carrier != backend->carrier`. This is NOT an unseamed-bridge staleness of
the *same* character as #1/#3:

- The pooled-protocol wait (A9) deliberately DETACHES the roots (asserts all
  four current-work pointers are NULL while parked) and hands the backend to
  the protocol scheduler; the seam over that park sees a NULL bridge and is
  harmless. So the 6-root bridge is NOT the corrupted state here.
- The failure is a **carrier-affinity mismatch at commit**: a migratable fiber
  that was work-stolen across a GUC-amutex park (A3, command path) resumes on a
  DIFFERENT carrier, then reaches the protocol-read boundary and commits the
  park; the scheduler's `carrier == CurrentPgCarrier` / `carrier->current_backend
  == backend` invariant no longer holds for the new carrier.
- It is release-visible (a real PANIC, not an assert-only tripwire) and was
  reproduced on the pristine pre-seam binary, so it is a pre-existing hazard on
  the migratable path, not a seam regression.

Verdict on #2: a DISTINCT hazard from #1 in mechanism (scheduler carrier
identity, not bridge staleness), but it ONLY bites with migratable=1 (it needs
a real cross-carrier steal). With the A3 GUC seam correctly repointing the
bridge, the *bridge* is fine; what remains is whether the protocol-read commit
path is correct when the resume carrier differs from the park carrier. It is
NOT subsumed by #1's seam -- the seam fixes the bridge, not the scheduler
affinity assertion. Whether it is a real remaining bug or an over-strict assert
requires a steal-capable substrate to decide (executor inert on this host); the
audit classifies it as KNOWN and migratable-gated.

## NEW hazards found beyond the 3 known

**ZERO new hazards.**

Every backend-fiber park funnels into a seamed choke point:
- `xtc_pg_wait_fd` (A1) -- seamed; and A7 (semaphore/LWLock/ProcSleep) and A8
  (WaitEventSet/WaitLatch/ConditionVariable/socket) both route through it.
- GUC amutex (A3) -- seamed on the command path; the pre-install reach (A3') is
  KNOWN #3.
- AIO (A4) and fsync/fdatasync (A5/A6) -- seamed.
- The pooled-protocol boundary (A9/A10) detaches by design; its residual is
  KNOWN #2.
- Supervisor `xtc_recv/xtc_send` (A11) is not a backend fiber.

Every shared per-OS-thread symbol (Table B) is either seamed (the 6 roots and
their derived hot cells/fields), safe-by-construction with no park while live
(`unpack_sql_state`, `retained_top_memory_allocated`), already hardened against
migration (`sem_fiber_backed` vs `xtc_in_backend_fiber`), or is the KNOWN #3
pre-install fallback family.

## Prioritized hazard list

1. **Bites the pinned runtime TODAY (highest):** KNOWN #3 -- concurrent-startup
   `early_*_fallback` corruption in the pre-install window (A3'). Reproduces at
   migratable=0 with >=3 concurrent connects (and single-loop at N~6). Latent
   production data-corruption bug in the runtime shipped today. Fix class:
   make the pre-install window PARK-FREE or PER-FIBER -- install a fiber-owned
   execution/memory-context root BEFORE `MemoryContextInit` so TopMemoryContext
   resolves to per-fiber state across the GUC-amutex park, OR make early-init
   GUC work not take the shared parking amutex. (This is the fix already
   in-progress separately.)

2. **Blocks migratable=1 (must fix before re-enable):** KNOWN #2 --
   protocol-read-park carrier-affinity PANIC (A10). Does not bite the pinned
   runtime (needs a real cross-carrier steal). Fix class: make the
   protocol-read commit resume-carrier-aware (re-derive/re-lease against the
   resume carrier), or prove the assertion over-strict. Needs a steal-capable
   substrate to reproduce/decide.

3. **Theoretical only (no action):** `retained_top_memory_allocated`
   (exit-time, no park while live); `unpack_sql_state` buf (documented
   fill+consume-inline invariant, park-boundary assert as backstop). Both would
   only become hazards if a future caller introduced a yield while the state is
   live -- the park-boundary assert (`xtc_pg_affine_section_depth() == 0`) is
   the standing backstop.

The already-seamed command-path parks (A1-A8) and the migration-stable
`sem_fiber_backed` wake channel are DONE (bug #1 fix + prior seams); no action.

## Is the enumeration EXHAUSTIVE?

**YES, for the two well-defined choke-point families and all `PG_THREAD_LOCAL`
runtime state**, bounded as follows:

- **Park sites:** `grep src/backend` for every libxtc parking primitive
  (`xtc_amutex_lock`, `xtc_proc_wait_fd`, `xtc_proc_sleep`, `xtc_sem_acquire`,
  `xtc_notify_wait`, `xtc_recv*`, `xtc_yield`, `xtc_aio_*`) found NO direct
  park call outside Table A. All indirect parks (WaitLatch, WaitEventSetWait,
  ConditionVariableSleep, LWLock deep-wait, ProcSleep, PGSemaphoreLock) were
  traced to their funnel and confirmed to route through the seamed
  `xtc_pg_wait_fd` (A7/A8) or the seamed GUC amutex (A3). The parking behavior
  of each primitive was confirmed against libxtc v1.27.0 source, not assumed.
- **Shared per-thread state:** `grep PG_THREAD_LOCAL` across `src/backend` (19
  sites) and `src/include` (7 macro-generating declarations) enumerates the
  complete set; each is classified in Table B.

**Bounds / caveats:**

- Bug #2's status ("real remaining bug vs over-strict assert") cannot be
  *decided* from source alone -- it needs a steal-capable substrate (the
  executor is inert on this host, and migratable=0). It is definitively
  CLASSIFIED (distinct mechanism, migratable-gated), just not resolved.
- The audit covers the parking-primitive class. It does not audit non-parking
  cross-fiber races (e.g. two fibers racing a shmem structure without a park),
  which are a different class outside this task's scope.
- Third-party extension code (contrib and out-of-tree) is out of scope
  (Phase 16 / Gate E2-Extensions owns that); a process-backend-only extension
  never runs on a backend fiber.

## Bottom line

The unseamed-park class is now **fully mapped** for in-tree core: every
backend-fiber park is either seamed (SAFE) or one of the 3 known hazards.
**No new hazards.** The known set is: #1 (GUC command-path bridge leak, FIXED),
#2 (protocol-read-park carrier-affinity PANIC, migratable-gated), #3
(pre-install `early_*_fallback` corruption, bites pinned today, fix in
progress). Fixing #3 unblocks the pinned runtime; fixing #2 (with a
steal-capable substrate to validate) unblocks migratable=1.
