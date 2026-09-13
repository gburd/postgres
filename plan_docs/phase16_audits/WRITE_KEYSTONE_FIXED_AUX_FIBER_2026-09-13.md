# Write keystone FIXED + validated: aux-process fiber-backed LWLock waits (commit ef06a6b07c)

Date: 2026-09-13. Fix by agent 0127e37b (aborted at turn limit but committed + validated + tore down
its resources first). Root cause diagnosed by agent 082077eb (see
LWLOCK_WEDGE_ROOTCAUSE_AUX_CARRIER_STARVATION.md). Two agents independently converged on this exact
fix (de68f552's out-of-scope tree had the same change), which is strong evidence it is right.

## The fix
`src/backend/storage/lmgr/proc.c`, `InitAuxiliaryProcess`, commit **ef06a6b07c**. Keys
`MyProc->sem_fiber_backed` off `xtc_in_backend_fiber` (mirroring `InitProcess`'s
`sem_wake_fd >= 0 && !PG_BACKEND_WAS_FORKEXECED` guard, with the fork+exec else-branch preserved)
instead of hardcoding `false`. So a fiber-backed aux process (WalWriter/BgWriter/Checkpointer)
blocking on a contended LWLock now PARKS via `ProcSemaphoreWaitFiber` instead of freezing the whole
carrier OS thread via raw `PGSemaphoreLock`. Process mode unaffected (guard is `USE_XTC_CARRIER`-only;
`xtc_in_backend_fiber` is false there).

## Validated (local, 8 vCPU, io_uring-backed libxtc 1.43.0)
NB: intended EC2 validation fell back to local because the AWS creds hit AuthFailure mid-run; local
was uring-backed (the code path that matters), so the repro is valid, but this has NOT been confirmed
on a large-core EC2 box. Re-confirm on Debian/EC2 when convenient.
- **Pre-fix (reproduced the exact wedge):** `pgbench -i -s 300` (c=8) -- COPY backend stuck on
  `IO/WalSync`, autovacuum worker stuck on `LWLock/WALWrite`, both 4-5+ minutes. Matches the
  diagnosed carrier-OS-thread-starvation mechanism.
- **Post-fix:** `pgbench -i -s 300` completed cleanly **3/3 runs**; `pgbench -c 8` write completed.
  The keystone wedge is CLEARED.
- **`gmake check`: 245/245** (process mode), built in an ISOLATED git worktree to dodge shared-tree
  contamination from the sibling's uncommitted WIP (the agent independently adopted the worktree
  mitigation).
- `check-runtime-lifecycles` / `check-global-lifetimes`: pre-existing failures, unaffected.

## What this fix does NOT clear (next write-path blockers)
- `pgbench -c 32` write still hits **finding #1: WalWriter livelock** in `ProcSemaphoreWaitFiber`'s
  GUC-rebind retry (100% CPU spin, never re-polls its ring) -- confirmed via gdb, cross-checked
  against fiber-write-wedge-diagnosis-2026-09-12.md mechanism 1. Separate bug, not caused/worsened by
  this fix. This is now the NEXT write-path fix: the GUC lazy-rebind
  (`PgRuntimeRestoreCurrentWorkLazy` exists at that site, unwired).
- Finding #3 (pooled sessions not routed through the fiber-aware wait) may be partly subsumed now
  that aux procs are fiber-backed -- re-measure the pooled write path after finding #1.

## Write-path status after this fix
- #2 lost-LWLock-wakeup keystone -> was actually aux-carrier-thread starvation -> **FIXED + validated
  (c=8 wedge gone)**.
- #1 WalWriter GUC-rebind livelock -> **OPEN, now the top write blocker** (c>=32 write).
- #3 pooled raw-semaphore carriers -> re-measure after #1.
The write path is materially unblocked at low concurrency; the remaining c>=32 write hang is a single
named, diagnosed CPU-spin bug (#1), not a mystery.
