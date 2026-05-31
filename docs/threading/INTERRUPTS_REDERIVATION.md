# Interrupt Re-derivation — replacing Latches with Interrupts on current master

Status: **DESIGN / NOT YET IMPLEMENTED.** This document plans the
re-derivation of Heikki Linnakangas's "Replace Latches with Interrupts"
refactor against current master (`7ea22d4c950`), as the first structural
piece of the PG-on-xtc work beyond Phase 0 tooling. It is the reference
design for the implementation commits that follow; it is intentionally
reviewable *before* any code lands, because none of it can be
build-verified in the current environment (unconfigured tree, no PG
build, no clang/LLVM).

This is a substrate change with **zero intended behaviour change** while
`multithreaded=off`: in the process model the new interrupt bitmask is a
drop-in re-expression of the latch + pending-flag machinery. Its payoff
is in the thread model, where a per-process atomic bitmask reached via a
`session_local` pointer is exactly what a shared-address-space backend
needs, and where the old `MyLatch`/per-PGPROC `procLatch` ownership dance
does not translate.

------------------------------------------------------------------------
## Why re-derive instead of cherry-pick

Heikki's refactor is commits `42946ffcc7c` ("Replace Latches with
Interrupts") + `6e11847ca79` ("Replace ProcSignals ... with interrupts"),
plus rename/annotation/bugfix scaffolding (`0e3ec5bc99c`, `628ab5fb814`,
`37ab35abf22`, `a9d9bc8c024`, `af18807a625`, `a8f5cada743`). It is ~1.5
years stale and was written against a `latch.c` that still contained the
WaitEventSet implementation. Master has since split WaitEventSet into its
own file (`393e0d23140`), changed signatures, and factored the wakeup
primitives out of the Latch struct. A verbatim replay collides on exactly
the layer we least want to take blind. So: take Heikki's design as the
spec, re-express it against master's current shape.

------------------------------------------------------------------------
## The abstraction (from Heikki, unchanged in intent)

The `Latch` (a per-process struct with `is_set`/`owner_pid`, owned and
disowned, pointed at by `MyLatch`) is replaced by a **per-process atomic
interrupt bitmask** reached through a `session_local` pointer:

```c
extern PGDLLIMPORT session_local pg_atomic_uint32 *MyPendingInterrupts;
```

`MyPendingInterrupts` points at one of two `pg_atomic_uint32` slots:

* `LocalPendingInterrupts` — a `session_local` slot, used before the
  process has a PGPROC and whenever the backend does not want other
  backends to reach it. Only `RaiseInterrupt()` (self) writes it.
* `MyProc->pendingInterrupts` — a new `pg_atomic_uint32` field in PGPROC
  (replacing `Latch procLatch`), reachable by other backends via
  `SendInterrupt(mask, ProcNumber)`.

`SwitchToLocalInterrupts()` / `SwitchToSharedInterrupts()` repoint
`MyPendingInterrupts` with a memory barrier and atomically OR-merge the
bits from the slot being left, so an interrupt sent concurrently across
the switch is never lost. This replaces `OwnLatch`/`DisownLatch`/
`SwitchToSharedLatch`/`SwitchBackToLocalLatch`.

### Interrupt types (32-bit bitmask)

`enum InterruptType`, one bit per condition. The set unifies three things
master keeps separate today:

1. latch wakeups → `INTERRUPT_GENERAL` (multiplexed general wakeup);
2. the `volatile sig_atomic_t` pending-flag globals (`ProcDiePending`,
   `QueryCancelPending`, the timeout flags, `ProcSignalBarrierPending`,
   `LogMemoryContextPending`, ...) → dedicated `INTERRUPT_*` bits;
3. ProcSignal reasons (`PROCSIG_*`) → dedicated `INTERRUPT_*` bits (this
   is the `6e11847ca79` half).

`SLEEPING_ON_INTERRUPTS = 1 << 31` is an internal bit set while the
backend is blocked in `WaitInterrupt`, so a setter knows whether a wakeup
is needed. Bits `1<<0 .. 1<<29` are real interrupts; the 32-bit ceiling
is a hard constraint (no portable 64-bit atomics).

Mask groups: `INTERRUPT_CFI_MASK` (bits `CHECK_FOR_INTERRUPTS()` may act
on), `INTERRUPT_STARTUP_PROC_MASK`, `INTERRUPT_MAIN_LOOP_MASK`.

### API surface (matches Heikki verbatim; stable target)

```c
/* inline, in interrupt.h */
bool InterruptPending(uint32 mask);     /* test (read barrier) */
void ClearInterrupt(uint32 mask);       /* clear (was ResetLatch) */
bool ConsumeInterrupt(uint32 mask);     /* test-and-clear */

/* out-of-line, in interrupt.c */
void RaiseInterrupt(uint32 mask);                 /* self (was SetLatch(MyLatch)) */
void SendInterrupt(uint32 mask, ProcNumber);      /* other proc (was SetLatch(&proc->procLatch)) */
int  WaitInterrupt(uint32 mask, int wakeEvents, long timeout, uint32 wei);   /* was WaitLatch */
int  WaitInterruptOrSocket(uint32 mask, int wakeEvents, pgsocket, long, uint32); /* was WaitLatchOrSocket */
void SwitchToLocalInterrupts(void);
void SwitchToSharedInterrupts(void);
void InitializeInterruptWaitSet(void);
void InitializeInterruptSupport(void);
```

------------------------------------------------------------------------
## Divergences from Heikki's tree that the re-derivation MUST reconcile

These are the concrete deltas found by diffing Heikki's `interrupt.c`/
`interrupt.h` against master's current `waiteventset.{c,h}`, `proc.h`,
`latch.c`:

1. **WaitEventSet event-source signature.** Master:
   `AddWaitEventToSet(set, events, fd, struct Latch *latch, user_data)`
   and `ModifyWaitEvent(set, pos, events, struct Latch *latch)`. Heikki:
   the `Latch *` slot becomes `uint32 interruptMask`, and `WL_LATCH_SET`
   becomes `WL_INTERRUPT`. **This is the core waiteventset.c rewrite** —
   the event source is "this process's pending-interrupt word" rather
   than "this Latch". `waiteventset.c` must learn to arm/check the
   interrupt word (via `MyPendingInterrupts`) where it currently arms/
   checks `latch->is_set`.

2. **`WL_LATCH_SET` (1<<0) → `WL_INTERRUPT`.** `waiteventset.h:34`. Every
   `WL_LATCH_SET` call site converts. Keep the bit value; rename only.

3. **`WakeupOtherProc` signature.** Master: `WakeupOtherProc(int pid)`
   (`waiteventset.c:2035`, keyed on pid via `kill(pid, SIGURG)`). Heikki's
   `SendInterrupt` calls `WakeupOtherProc(proc)` with a `PGPROC*`.
   **Decision: keep master's `int pid` signature and call
   `WakeupOtherProc(proc->pid)`** — less invasive, and the wakeup is
   already latch-independent on master (good: it factored cleanly).

4. **`WakeupMyProc`/`WakeupOtherProc` are `#ifndef WIN32`.** Windows uses
   the Latch's event HANDLE directly (`SetEvent`). The interrupt
   mechanism needs a Windows wakeup path that does not depend on a Latch
   struct. **Decision: defer Windows wakeup wiring to a follow-up**; the
   first core commit targets the POSIX self-pipe/SIGURG path (matches our
   F5/spike platform and the dev environment). Tracked as an open item.

5. **PGPROC field.** Master `proc.h:262` `Latch procLatch;` →
   `pg_atomic_uint32 pendingInterrupts;`. **Decision: in the first core
   commit, ADD `pendingInterrupts` ALONGSIDE `procLatch` rather than
   replacing it**, so `latch.c` keeps compiling and there is no
   call-site avalanche in one commit. The replacement (and `procLatch`
   removal) happens in the later conversion commits. This keeps the core
   commit small and reviewable, at the cost of a transient extra word in
   PGPROC under `multithreaded=off` (harmless; removed by end of series).

6. **Initialization order.** `InitializeInterruptSupport()` (sets
   `MyPendingInterrupts = &LocalPendingInterrupts`) must run early in
   every process's init, before any `RaiseInterrupt`. We wire it from the
   tail of `InitializeWaitEventSupport()` (waiteventset.c), which all three
   master call sites already invoke before `InitializeLatchWaitSet()`
   (`InitPostmasterChild`, `InitStandaloneProcess`, postmaster). This is a
   single seam that covers every process that can wait, and runs before
   any latch/interrupt use. `InitializeInterruptWaitSet()` must run after
   WaitEventSet support is up; it is deferred to step 2 (it needs the
   interrupt-word event source), and will slot in next to master's
   `InitializeLatchWaitSet()` call sites.

------------------------------------------------------------------------
## Commit plan (each independently reviewable; build-gated as noted)

The series is ordered so the tree keeps building after each step. Steps
1–2 are the "core" this session targets; 3–7 are the conversion sweep.

1. **Add the interrupt core, coexisting with latches.** New
   `src/include/storage/interrupt.h` (enum, masks, inline accessors, API
   decls) and `src/backend/storage/ipc/interrupt.c` (`MyPendingInterrupts`,
   `LocalPendingInterrupts`, `Switch*Interrupts`, `Raise/SendInterrupt`,
   `Initialize*`). `WaitInterrupt`/`WaitInterruptOrSocket` are included but
   depend on step 2 for `WL_INTERRUPT`; until then they can be compiled
   against a temporary `WL_INTERRUPT := WL_LATCH_SET` alias OR step 2 is
   folded in. Add `pendingInterrupts` to PGPROC alongside `procLatch`.
   Add `interrupt.c` to the backend build (meson `meson.build` +
   `Makefile` in `storage/ipc`). **No call sites converted.** latch.c
   untouched. Tree builds; behaviour unchanged.

2. **Teach WaitEventSet about the interrupt word.** Add `WL_INTERRUPT`,
   give `AddWaitEventToSet`/`ModifyWaitEvent` the ability to register the
   interrupt word as an event source (parallel to the latch source,
   initially — both can coexist). This is the minimal waiteventset.c
   change that makes `WaitInterrupt` real.

3. **Convert the pending-flag globals** (`ProcDiePending`,
   `QueryCancelPending`, timeout flags, `ProcSignalBarrierPending`,
   `LogMemoryContextPending`, ...) to `INTERRUPT_*` bits; rewrite
   `ProcessInterrupts()` (`postgres.c:3361`) as a chain of
   `ConsumeInterrupt(INTERRUPT_X)` branches gated by `CheckForInterrupts
   Mask`; update `CHECK_FOR_INTERRUPTS()`/`HOLD_INTERRUPTS()` in
   `miscadmin.h`. Update signal handlers `die()`/`StatementCancelHandler()`
   to `RaiseInterrupt(...)`.

4. **Absorb ProcSignal** (`6e11847ca79`): `PROCSIG_*` reasons → interrupt
   bits; `procsignal.c` consumers and senders move to `SendInterrupt`.

5. **Convert latch wait/wake call sites**: `WaitLatch`→`WaitInterrupt`,
   `SetLatch(MyLatch)`→`RaiseInterrupt(INTERRUPT_GENERAL)`,
   `SetLatch(&proc->procLatch)`→`SendInterrupt(INTERRUPT_GENERAL, procno)`,
   `ResetLatch`→`ClearInterrupt`, across `postmaster/*` aux loops
   (`checkpointer.c`, `bgwriter.c`, `walwriter.c`, `autovacuum.c`),
   `replication/logical/launcher.c`, `applyparallelworker.c`,
   `commands/async.c`, `replication/slot.c`, `storage/lmgr/proc.c`.

6. **Remove the Latch.** Delete `procLatch` from PGPROC, delete `MyLatch`,
   delete `latch.c`/`latch.h`, fold any remaining latch-only wakeup
   plumbing into `interrupt.c`/`waiteventset.c`.

7. **Annotations + Windows.** Ensure every new file-scope/global the
   mechanism introduces carries a `session_local`/`pg_global` annotation
   (validated by the `pg_static_vars` tool from Phase 0); wire the Windows
   wakeup path (item 4 in Divergences).

------------------------------------------------------------------------
## What this session implements

Step **1 only** (the bitmask core), as one reviewable commit. Step 2 (the
WaitEventSet wait-source rewrite) is deferred to a follow-up because the
latch source is woven through `WaitEventSetWaitBlock` for all four
backends (epoll/kqueue/poll/win32), making it a large, separately
reviewable change:

* `src/include/storage/interrupt.h` — re-derived from Heikki, comments
  preserved, copyright year updated, includes adjusted for master. The
  wait entry points (`WaitInterrupt`, `WaitInterruptOrSocket`,
  `InitializeInterruptWaitSet`) are declared as the stable target ABI but
  noted as implemented in step 2.
* `src/backend/storage/ipc/interrupt.c` — bitmask core only:
  `MyPendingInterrupts`, `LocalPendingInterrupts`,
  `InitializeInterruptSupport` (with explicit `pg_atomic_init_u32`),
  `Switch{Local,Shared}Interrupts`, `RaiseInterrupt`, `SendInterrupt`
  (adapted to `WakeupOtherProc(proc->pid)`). The three wait-dependent
  functions are deferred to step 2.
* `src/include/storage/proc.h` — add `pg_atomic_uint32 pendingInterrupts`
  to PGPROC, alongside `procLatch` (no removal yet).
* `src/backend/storage/lmgr/proc.c` — `pg_atomic_init_u32` the new field
  next to `InitSharedLatch(&proc->procLatch)`.
* `src/backend/storage/ipc/waiteventset.c` — call
  `InitializeInterruptSupport()` from the tail of
  `InitializeWaitEventSupport()` (and `#include "storage/interrupt.h"`),
  so `MyPendingInterrupts` is non-NULL in every waiting process. No
  `WL_INTERRUPT` and no wait-source changes yet (step 2).
* build wiring: add `interrupt.c` to `storage/ipc/meson.build` and
  `Makefile`.

**No call sites are converted; `latch.c` is untouched; `multithreaded`
stays off.** The tree should build with zero behaviour change. Because no
PG build is available here, verification is limited to: header/syntax
review, confirming signatures against master, and confirming the enum
fits 32 bits. A full `meson`/`make` build on a configured host is a
required follow-up before step 2+.

------------------------------------------------------------------------
## xtc relationship (forward-looking, not implemented here)

The interrupt mechanism is the PG-side substrate that a later phase backs
with xtc. Cooperative cancellation of a runaway in-thread backend will
ride `xtc_abort_source`/`xtc_abort_token` (one token per backend, fired
from `statement_timeout` and the cancel path, polled at
`CHECK_FOR_INTERRUPTS()` sites) — see the PG-on-xtc/xtc-dev
correspondence. `WaitInterrupt` is also the natural seam to later map
onto `xtc_notify`/`xtc_proc_wait_fd` (vision Phase 2). Nothing in this
re-derivation bakes in "one thread per connection"; it is correct for
both the process and thread models, with thread-correctness delegated to
the `session_local` annotation layer rather than `IsMultiThreaded`
branching.
