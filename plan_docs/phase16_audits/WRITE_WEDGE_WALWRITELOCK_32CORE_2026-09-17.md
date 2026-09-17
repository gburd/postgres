# Write wedge at 32 cores: a WALWriteLock acquire never completes; one fiber spins, 31 loops idle

Date: 2026-09-17. Captured live on the 32-core EC2 SUT (libxtc v1.48.1, uring) at the exact concurrency
where it bites (write c=32), which the 8-core box could not produce.

## The captured state
Driving `pgbench -c 32` write from the separate driver wedges within ~12 s (WALWrite waiters 22 -> 25,
0 tps). At the wedge:

**The chain head** -- two client backends, `active`, executing `END;` (commit), with EMPTY
`pg_blocking_pids()`:
```
pid 9  active  LWLock/WALWrite  END;
pid 12 active  LWLock/WALWrite  END;
```

**The one busy thread** (tid 72749, ~100% of a core, stable across samples) is a commit fiber stuck
acquiring WALWriteLock:
```
#3 read()                         <- draining its sem_wake_fd
#4 ProcWaitOnSemaphore
#5 LWLockAcquireOrWait            <- WALWriteLock (lwlock.c)
#6 XLogFlush
#7 CommitTransaction
#8 CommitTransactionCommand
#9 exec_simple_query
#10 PgSessionRun                  <- the fiber path (my P-A1 routing)
```

**Everything else is idle**: of 32 exec loops, 31 are parked in `xtc_io_poll` /
`__io_uring_get_cqe` with nothing to run. So this is NOT heavy CPU contention on the lock -- it is ONE
fiber hot-looping in `LWLockAcquireOrWait`'s retry (`for(;;){ ProcWaitOnSemaphore; if lwWaiting ==
NOT_WAITING break; }`) while 25 committers queue behind it and every carrier thread sits idle.

**The wake channel shows no pending wake**: the spinner's eventfds all read `eventfd-count: 0`. So it is
not "a wake arrived and was missed"; the fiber parks, `xtc_pg_wait_fd` returns, `lwWaiting` is still
`LW_WS_WAITING`, and it re-parks -- burning a core -- because the release that should clear `lwWaiting`
and post the fd never happens (or happened against a different waiter).

## Why this is the write wedge, and why it eluded the 8-core traces
- On 8 cores the same bug presented as `tuple` / `BufferExclusive` waiters; here it is `WALWrite`. Same
  root shape (a queued LWLock waiter never released), different most-contended lock because the WAL
  flush path is the bottleneck at 32-core write concurrency. This is why every read-path elimination on
  8 cores (wait layer, secure_read, epoll) correctly cleared -- the wedge was never on the read side; it
  is the WALWriteLock release/wake on the fiber path, and it only concentrates on WALWriteLock at scale.
- libxtc v1.48.1 reports itself clean (its wrong-proc strand fixes helped -- the wedge moved from ~45s to
  ~105s locally and the read matrix is now at parity), so the residual is PG-side: `LWLockRelease` /
  `LWLockWakeup` for WALWriteLock, or the `LWLockAcquireOrWait` fast-path interaction with the
  fiber-backed `ProcWaitOnSemaphore`.

## Prime suspect (to instrument next, at 32 cores)
`LWLockAcquireOrWait` is the odd one: it acquires OR waits-until-free without holding, and its waiter
uses `LW_WAIT_UNTIL_FREE` semantics. The releaser's `LWLockWakeup` must wake a `LW_WAIT_UNTIL_FREE`
waiter AND clear its `lwWaiting`. If a concurrent releaser sees `LW_FLAG_WAKE_IN_PROGRESS` still set (the
flag only a woken waiter clears), it skips the wakeup -- and a fiber that was "woken" but never actually
resumed to clear the flag would strand the queue. That is the classic self-sustaining shape, and it fits
"one fiber spins, everyone else queues, no loop does work". Instrument `LWLockRelease`/`LWLockWakeup` for
WALWriteLock to log when a releaser sees waiters queued but skips the wakeup, or when a waiter's
`lwWaiting` is not cleared across a `ProcWaitOnSemaphore` return.

## Status against the north star
This is the single gate between the read parity we now have and a real write comparison. It is a
concrete, located, PG-side LWLock-wake defect on the WAL commit path, reproducible in ~12 s at c=32 on
32 cores. Not fixed yet; the repro and the exact stack are now on record so the fix can be built and
validated where it actually occurs.
