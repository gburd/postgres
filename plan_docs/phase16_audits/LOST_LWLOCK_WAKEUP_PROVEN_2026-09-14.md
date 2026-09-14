# PROVEN: a lost LWLock/buffer-content-lock wakeup strands fiber backends forever

Date: 2026-09-14. Established with a DEBUG libxtc build + direct PGPROC/eventfd inspection.
This supersedes the "fiber backend leak" framing: the leak is real but it is a CONSEQUENCE of this.

## The proof (four independent facts, same conclusion)
On a wedged server with **0 ESTAB client sockets** (every client gone) and fibers **11+ minutes** old:

1. **The waiters are correctly armed and parked.** For every stuck PGPROC:
   `lwWaiting=1 (LW_WS_WAITING)  sem_fiber_backed=1  sem_fiber_armed=1  sem_fiber_wake_pending=0`
   So the fiber wait path did its job: it armed, published the owner handle, and parked.
2. **No wake was ever delivered.** `/proc/<pid>/fdinfo/<sem_wake_fd>` shows **`eventfd-count: 0`** on
   every one of them. `ProcWakeSemaphore()` writes 1 to that eventfd as its load-bearing wake, so a
   count of 0 means the wake was **never written** -- it is not a "wake delivered but missed" race.
3. **`lwWaiting` is still `LW_WS_WAITING`, not `LW_WS_PENDING_WAKEUP`.** `LWLockWakeup()` sets
   `PENDING_WAKEUP` on every waiter it dequeues *before* posting. Still being `WAITING` proves
   `LWLockWakeup` never walked these waiters at all.
4. **libxtc itself is healthy.** `xtc-stranded` (debug build) reports `79 parked, 1 suspect, 7 idle
   service fibers excluded`, park census `fd=70, none=8, timer=1`. The 70 fd-parks ARE these waiters
   on their `sem_wake_fd`. libxtc is holding them exactly as asked; nothing is stranded on its side.

Waits involved: `Buffer/BufferExclusive` (20), `Buffer/BufferShared` (11), `LWLock/WALInsert` (6).
Buffer content locks ARE LWLocks (in the buffer descriptor), so all of these are one bug.

## Conclusion
The lost wakeup is **upstream of the fiber plumbing, in the LWLock release/wake path**: a releaser
either did not observe `LW_FLAG_HAS_WAITERS` or did not call `LWLockWakeup()`, so the queued waiters
were never dequeued and never posted. The fiber park/arm/wake machinery
(`ProcSemaphoreWaitFiber` / `ProcWakeSemaphore`) is provably correct here -- armed=1 with an
eventfd-count of 0 exonerates it.

This also explains the apparent "backend leak" (68 -> 69 client backends after a clean run): a backend
stranded in a buffer-lock wait cannot notice its client disconnected, so it never exits, and it keeps
holding row/buffer locks. The leak and the wedge are the same bug.

## Why the earlier fixes helped but did not fix it
The three wait-path fixes (lazy GUC restore, logical-timeout clamp spin, ProcSleep's missing
WL_TIMEOUT) were all real defects and improved write tps 198 -> 499 -- but none of them creates the
missed `LWLockWakeup` call, so the wedge survives them.

## Next step (and the lock directive)
The standing directive is to use libxtc's async primitives for locks rather than hand-rolled ones.
That is the right structural answer here: our LWLock wait is PG's own queue + spinlock + a hand-rolled
eventfd semaphore, and it is that queue/release interaction that is dropping the wake. libxtc offers
`xtc_sem` (async counting semaphore, fiber-parking), `xtc_amutex` / `xtc_arwlock` (async mutex /
rwlock, already used for our GUC and dfmgr critical sections), and `xtc_notify`.
Two candidate directions, to be decided by measurement:
 A. Keep PG's LWLock semantics but replace the per-PGPROC wake channel with `xtc_sem`, so arm/post is
    libxtc's problem rather than our eventfd + spinlock + pending-flag hand-off.
 B. For buffer content locks specifically (the dominant waiter here), evaluate `xtc_arwlock` --
    shared/exclusive is exactly its shape.
Before either, reproduce with a targeted probe on `LWLockRelease`: log when a releaser sees
`LW_FLAG_HAS_WAITERS` clear while waiters are queued. That single counter distinguishes "flag lost"
from "wakeup skipped" and tells us whether the bug is in our annotated release path or in the
queue/flag update ordering.

## Measurement hygiene learned (do not skip)
- Wedge runs MUST start from a freshly-initdb'd server; leftover sessions from timeout-killed runs
  poison every later result (one read-only run returned nothing on a poisoned server and 34,545 tps on
  a clean one).
- `gdb thread apply all bt` CANNOT see parked fibers (state is on their own stacks). Use
  `xtc-stranded` with a DEBUG libxtc (`-g3 -O1 -fno-omit-frame-pointer`, `dontStrip = true`, via
  `nix develop --override-input libxtc path:...`); the release pin reports "no loops registered".
- `eventfd-count` in `/proc/<pid>/fdinfo/<fd>` is the decisive test for "was the wake delivered".
