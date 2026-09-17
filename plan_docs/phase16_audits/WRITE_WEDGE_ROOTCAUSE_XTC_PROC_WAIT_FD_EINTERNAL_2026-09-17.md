# Write wedge ROOT CAUSE: xtc_proc_wait_fd returns XTC_E_INTERNAL (-6) in a spin

Date: 2026-09-17. Found on the 32-core EC2 SUT by direct instrumentation. This is the terminal root
cause of the residual write wedge -- the thing every 8-core trace kept eliminating from the wrong end.

## The chain, fully resolved
1. write c=32 wedges in ~12 s at 32 cores (WALWrite waiters pile to ~25, 0 tps). Server stays alive
   (0 PANIC). It is a wedge, not a crash.
2. One carrier thread spins at 100 % CPU. Its stack:
   `XLogFlush -> LWLockAcquireOrWait(WALWriteLock) -> ProcWaitOnSemaphore -> read() (EAGAIN)`.
   31 of 32 exec loops sit idle in xtc_io_poll.
3. NOT a stranded LWLock queue: the missing LWLockAcquireOrWait WAKE_IN_PROGRESS clear (fixed in
   a141499c86, kept because it is a genuine latent inconsistency) did NOT change the wedge.
4. NOT a wake storm: PG_XTC_WAKE_STORM counted ZERO writes to the spinner's eventfd during the wedge.
   The eventfd (fd 61 / fd 59) has kernel `eventfd-count: 0` -- idle, valid, level-triggered.
5. THE cause: PG_XTC_WAITFD_TRACE at the xtc_proc_wait_fd return shows it returning, 35,000,000+ times,
   in a tight loop:
   ```
   WAITFD fd=61 interest=0xd rc=-6 revents=0x0 timeout_ns=-1
   ```
   rc=-6 = XTC_E_INTERNAL ("invariant violation; bug", xtc.h:63). interest=0xd = READABLE|HUP|ERR.
   timeout_ns=-1 = infinite. The documented return set for xtc_proc_wait_fd is ONLY
   {XTC_OK, XTC_E_AGAIN, XTC_E_INVAL} (xtc_proc.h:396-403). XTC_E_INTERNAL is undocumented here.

## Why it turns into a 100% spin (the embedder mapping)
pg_xtc_carrier.c xtc_pg_wait_fd() maps the libxtc return:
```c
if (rc != XTC_OK && rc != XTC_E_AGAIN)
    return WL_LATCH_SET;   /* treat as a wakeup; caller re-checks */
```
So XTC_E_INTERNAL -> WL_LATCH_SET -> LWLockAcquireOrWait's wait loop re-checks lwWaiting (still WAITING,
because no real wake happened) -> re-parks -> xtc_proc_wait_fd returns XTC_E_INTERNAL again -> unbounded
busy loop. The carrier never returns to xtc_io_poll, so every fiber whose completion lives on this loop
starves -> the whole write path wedges.

## Why the 8-core traces never caught it
At 8 loops the same workload never drives xtc_proc_wait_fd into the XTC_E_INTERNAL state (far less
cross-loop fd-park churn). The wedge only appears with enough loops + enough concurrent per-waiter
eventfd parks. Every read-path/transport elimination on 8 cores was correct -- the defect was never
there; it is the fd-park call itself failing its own invariant at scale. This is exactly why the north
star required 32-core validation: the bug does not exist below the loop count where it bites.

## Ownership: libxtc bug, filed; embedder mapping flagged
- PRIMARY: libxtc. xtc_proc_wait_fd must PARK on a valid idle fd with an infinite timeout, not return
  XTC_E_INTERNAL. Report filed: /tmp/libxtc-proc-wait-fd-returns-XTC_E_INTERNAL-in-a-spin-2026-09-17.md
  (full trace, fdinfo, backtrace, wake-count=0 control, loop-count sensitivity). No workaround per the
  contract-first policy: we are NOT swallowing -6.
- SECONDARY (ours, to consider once libxtc is fixed): mapping an UNDOCUMENTED libxtc return to
  WL_LATCH_SET (spurious wake) is what converts a single error into a 35M-iteration spin. WL_LATCH_SET is
  the right mapping for OK/benign wakes, but for an internal-error return it would be safer to escalate
  (FATAL the fiber / fail-stop) than to busy-spin forever silently. We are NOT changing this yet: it
  would mask the libxtc bug and the correct fix is upstream in libxtc. If libxtc says XTC_E_INTERNAL can
  be a legitimate transient we will revisit; a -6 repeating 35M times with revents=0 is not transient.

## Status against the north star
This is THE gate between the read parity we measured (fiber ~= fork to core count) and a real write
comparison. It is now a single, located, upstream-libxtc invariant violation with a filed report,
reproducible in ~12 s at c=32 on 32 cores. Blocked on the libxtc fix; the diagnosis is complete.
