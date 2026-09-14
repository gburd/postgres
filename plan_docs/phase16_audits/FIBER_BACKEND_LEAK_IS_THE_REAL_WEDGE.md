# The "write wedge" is a FIBER BACKEND LEAK on client disconnect, not a lost lock wakeup

Date: 2026-09-14. Found by running the wedge down on a clean server with a DEBUG libxtc build.

## What I chased vs what it is
I attributed the c=64 write wedge first to the GUC-rebind livelock, then to a lost buffer-lock wakeup
(waiters on `Buffer/BufferShared`/`BufferExclusive` with an EMPTY `pg_blocking_pids()`). Those waiters
are real, but they are a SYMPTOM. The cause is upstream of them:

**Client backends are not reaped when their client disconnects.** Measured directly: 68 client backends
before a clean `pgbench -S -c 16 -T 8` run, **69 after** it finished and 4s elapsed. Each aborted or
completed run leaves backends behind; some sit `idle in transaction` holding row/buffer locks forever.
Everything else then piles up behind them, and the server degrades until even
`pgbench`'s initial `VACUUM pgbench_branches` hangs -- which is exactly the "starting vacuum..." hang
that looked like a write wedge.

Decisive evidence that the locks themselves are healthy: on the SAME poisoned server, an isolated
`VACUUM pgbench_branches` from a fresh connection returns **instantly (`VACUUM`, exit 0)**. Nothing is
structurally stuck; the server is simply full of leaked lock-holding backends.

## Why my earlier readings were misleading
- `pg_blocking_pids()` empty on a `Buffer/*` wait looks like a lost LWLock wake, but a leaked
  idle-in-transaction backend does not appear as a *blocker* for a buffer content lock, so the chain
  root looked ownerless.
- Thread backtraces cannot see PARKED fibers (their state lives on their own stacks), so `gdb thread
  apply all bt` showed "0 fibers in ProcSemaphoreWaitFiber" while 75 fibers were in fact parked.
- Leftover sessions from timeout-killed runs silently poisoned every subsequent measurement. Any
  wedge measurement MUST start from a freshly-initdb'd server; otherwise results are meaningless.
  (This invalidated one of my read-only runs: it returned no tps on a poisoned server, then 34,545 tps
  on a clean one.)

## Tooling note (worth keeping)
libxtc is pinned as a RELEASE build, so the xtc-gdb helpers report `no loops registered` and are
useless for this class of bug. Building libxtc with `-g3 -O1 -fno-omit-frame-pointer` +
`dontStrip = true` (local `nix develop --override-input libxtc path:/tmp/libxtc-head`) makes
`xtc-loops` / `xtc-stranded` / `xtc-rings` work. With symbols, `xtc-stranded` correctly enumerated
**75 parked fibers, 1 suspect, 7 idle service fibers excluded** -- and it self-documents that a
`local_id 0` proc parked in `xtc_recv(...,-1)` is normal. That is the right tool for lost-wake claims,
and it also told me the runtime was NOT stranded, steering me off the lost-wake theory.

## Next
Find why a fiber backend survives client disconnect. Candidates, in order:
1. The fiber's exit path on `ClientRead` EOF / `pq` connection loss -- does `PgBackendExit()` run and
   does the supervisor observe DOWN? (The log does show `backend fiber exiting ... code=0` for some,
   so the path works sometimes -- find what makes it not run.)
2. Whether a backend parked in a LOCK wait (not a client-read park) ever notices its client is gone;
   upstream relies on a socket-readable/EOF event that a lock-parked fiber may not be watching.
3. `pmchild` slot release vs `pg_stat_activity` visibility -- confirm these leaked entries are live
   fibers, not stale stat entries (they hold locks, so they are live).

This is now the top fiber-path blocker, ahead of the two-model expunge: a model that leaks a backend
per disconnect cannot be the default.
