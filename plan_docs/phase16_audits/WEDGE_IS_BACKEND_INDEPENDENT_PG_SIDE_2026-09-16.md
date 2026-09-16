# The wedge is BACKEND-INDEPENDENT: epoll wedges too (sooner). The ring overflow was a symptom.

Date: 2026-09-16. The control I should have run before filing the io_uring report.

## The control
Rebuilt libxtc v1.47.0 with `--with-io-backend=epoll` (verified: **0 io_uring symbols** in the resulting
`libxtc.so.1`, so the backend really switched), same box, same workload, fresh initdb:

| backend | throughput trace | wedges at |
|---|---|---|
| io_uring | 1377 -> 1476 -> 1379 -> 124 -> **0.0 forever** | ~90 s |
| **epoll** | 597 -> 385 -> **0.0 forever** | **~45 s** |
| process mode (fork) | 2823 / 3288 / 3038 sustained | never |

Same signature on epoll: **3 of 65** unix sockets hold unread client data, fibers parked on fds,
`xtc-stranded` reports `0 suspect`, sessions stuck `idle in transaction` on `ClientRead` holding locks.

## Conclusion
`unreaped=512 ovf=1` on loop 7 was a **consequence** of the wedge, not its cause: once fibers stop being
resumed, that loop stops reaping, so its CQ fills and overflows. I reported the most *visible* anomaly
rather than the *earliest* one -- the sixth misattribution in this investigation, and the same failure
mode as the previous five: I acted on a striking signal without first asking "what does this look like
with the suspected component removed?"

Corrected the libxtc report in place with a DEPRIORITISE banner
(`/tmp/libxtc-io-uring-ring-wedges-cq-full-overflow-2026-09-16.md`) so their team does not chase it, while
keeping the one part that stands on its own: `xtc-stranded` reports `0 suspect` while fibers are provably
doomed on BOTH backends, so the strand-detection request is independent of this bug's cause.

## Where this leaves the wedge
It reproduces across two independent libxtc IO backends and never in process mode, so it is
**PostgreSQL-side, in code that is common to both fiber paths** -- i.e. our own park/resume seam, not
libxtc's IO. That narrows it usefully:
- ruled out: io_uring specifics, ring overflow, the supervisor mailbox (v1.47.0), a lost LWLock wake
  (waiters were armed with `eventfd-count: 0`, so no wake was ever *attempted*), row contention (fork
  sustains 3k tps on the identical workload), and the deadlock detector (a deliberate 2-session deadlock
  IS caught on the fiber path).
- still in scope: `WaitEventSetWaitBlock`'s fiber branch (we park on the epoll fd and harvest
  non-blocking -- an epoll-inside-libxtc nesting that exists on *both* backends), the
  `ProcSemaphoreWaitFiber` arm/disarm hand-off, and `PgRuntimeRestoreCurrentWork*` on resume.

The strongest remaining clue is the one constant across every capture: **client sockets with unread data
whose backend never reads them**, while a fiber sits parked on the epoll fd that watches those very
sockets, with the registration verified correct (`events: 0x19 = EPOLLIN|ERR|HUP`). That says the epoll
fd was ready and the fiber's park did not observe it -- which is now a claim about *our* seam, since it
holds with libxtc's IO backend swapped out from under it.

## Next probe (concrete)
Instrument `WaitEventSetWaitBlock`'s fiber branch to record, per park: `cur_timeout`, the return of
`xtc_pg_wait_fd`, and the `rc` of the following non-blocking `epoll_wait`. The hypothesis to kill or
confirm is that we take the "woke but nothing ready" path (my earlier `rc == 0` -> retry fix) and then
re-park **without re-arming** something that must be re-armed each time -- which would strand exactly the
sessions whose data arrived during the gap, and would be backend-independent.
