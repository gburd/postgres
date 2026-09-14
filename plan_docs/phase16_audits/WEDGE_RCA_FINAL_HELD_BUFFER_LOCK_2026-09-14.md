# Write-wedge RCA (final): a buffer content lock is HELD and never released -- not a lost wakeup

Date: 2026-09-14. Debug libxtc build + direct buffer-descriptor decode. This CORRECTS my two earlier
write-ups in this series (the "backend leak" framing and the "lost LWLock wakeup" framing).

## The decisive evidence
This tree has NO `content_lock` LWLock in `BufferDesc`; buffer content locking is a custom mechanism
using `proclist_head lock_waiters` + lock bits inside the 64-bit `BufferDesc.state`
(`BUF_FLAG_SHIFT = BUF_REFCOUNT_BITS(18) + BUF_USAGECOUNT_BITS(4) = 22`, so
`BM_LOCK_HAS_WAITERS` = bit 32, `BM_LOCK_WAKE_IN_PROGRESS` = bit 33, lock value bits >= 34).

Scanning all 8000 buffers on a wedged server, exactly **3** have waiters, and all three read:

```
buf[2254] state=0x200001c3940016  refcount=22 usagecount=5 HAS_WAITERS=1 WAKE_IN_PROGRESS=0  bits>=34=0x80000
buf[3357] state=0x20000183940005  refcount=5  usagecount=5 HAS_WAITERS=1 WAKE_IN_PROGRESS=0  bits>=34=0x80000
buf[5833] state=0x20000183940004  refcount=4  usagecount=5 HAS_WAITERS=1 WAKE_IN_PROGRESS=0  bits>=34=0x80000
```

Three facts follow:
1. **`WAKE_IN_PROGRESS = 0`** -- the wake gate is OPEN. `BufferLockRelease`'s `check_waiters` test
   (`HAS_WAITERS && !WAKE_IN_PROGRESS`) would fire and call `BufferLockWakeup()` on the next release.
   So this is NOT the self-sustaining stuck-flag deadlock I hypothesised.
2. **The lock value bits (>= 34) are NONZERO (0x80000)** -- the content lock is **still HELD**.
3. The buffers are still **pinned** (refcount 22 / 5 / 4).

So the waiters are queued correctly, the wake path is armed and unblocked, and they are simply waiting
for a holder that **never releases**. No wake is "lost": no release ever happens, which is also exactly
consistent with the earlier observation that every waiter's `sem_wake_fd` had `eventfd-count: 0` and
every waiter still had `lwWaiting = LW_WS_WAITING`.

## Corrected causal chain
A fiber acquires a buffer content lock, then **exits or parks forever without releasing it** ->
`HAS_WAITERS` accumulates on that buffer -> every later waiter on the same buffer blocks permanently ->
those blocked backends cannot notice their client disconnected, so they never exit and keep holding
row/heavyweight locks -> the "backend leak" (68 -> 69 client backends per run) and, eventually, even
`pgbench`'s initial `VACUUM pgbench_branches` hangs. One root cause, three symptoms.

## What to look for next (specific)
The question is now narrow: **which path leaves a buffer content lock held?** Candidates in order:
1. An error/longjmp out of a fiber while holding a content lock -- does the fiber's abort path run the
   buffer-lock release that process mode gets via `AbortBufferIO`/resource-owner cleanup? A fiber that
   FATALs or is cancelled mid-`heap_update` is the prime suspect (the stuck buffers hold pins and the
   waiters are all `heap_update`/`UPDATE` paths).
2. `BufferLockAcquire`'s retry loop: if the fiber is woken, sets `LW_WS_PENDING_WAKEUP`, and then dies
   before re-acquiring, the lock bits it already took are never dropped.
3. A fiber migrated between carriers mid-critical-section, so the release ran against different
   per-thread state (the buffer lock is tracked per backend, so verify the tracking array is the
   session's, not the carrier thread's).
Concrete probe: record the holder in the descriptor under `#ifdef USE_ASSERT_CHECKING` (backend id +
a short acquire site tag) so the wedged state names the leaker directly, instead of inferring it.

## Bearing on the lock directive
The standing directive (use libxtc async primitives for locks) still applies, but this RCA changes what
it would fix: swapping the wake channel to `xtc_sem`/`xtc_arwlock` would NOT fix a lock that is never
released. The release-path leak must be fixed first; adopting `xtc_arwlock` for buffer content locks
remains attractive afterwards (shared/exclusive is exactly its shape, and it would move the queue +
flag + wake bookkeeping into libxtc where a missed release is far harder to write), and should be
evaluated once the wedge is closed.
