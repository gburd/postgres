# Write wedge at 32 cores: WALWrite wait observations, not a root cause

> **SUPERSEDED DIAGNOSIS -- corrected 2026-09-22.** The September 17 claim of a
> located PG-side LWLock-wake defect was not established. The observations below
> are retained as historical evidence, not a current validation result. The
> later sampled `xtc_proc_wait_fd` errors narrow the failing path, not its root
> cause: see [the corrected return-code report](WRITE_WEDGE_ROOTCAUSE_XTC_PROC_WAIT_FD_EINTERNAL_2026-09-17.md).

## Reported capture (2026-09-17)

SUT: 32-vCPU EC2, libxtc v1.48.1, uring. A separate driver running write
`pgbench -c 32` reportedly stopped making progress within about 12 seconds;
WALWrite waiters rose from 22 to 25 and throughput fell to 0 tps.

Two client backends were reported as active on `END;`, with empty
`pg_blocking_pids()` results:

```
pid 9  active  LWLock/WALWrite  END;
pid 12 active  LWLock/WALWrite  END;
```

**These are not identified chain heads.** `pg_blocking_pids()` describes
heavyweight-lock blockers; an empty result does not identify an LWLock holder,
queue head, or missing wake.

The reported busy thread (tid 72749, roughly one CPU across samples) had this
stack:

```
#3 read()                         <- draining sem_wake_fd
#4 ProcWaitOnSemaphore
#5 LWLockAcquireOrWait            <- WALWriteLock
#6 XLogFlush
#7 CommitTransaction
#8 CommitTransactionCommand
#9 exec_simple_query
#10 PgSessionRun
```

The other 31 exec loops were reported in `xtc_io_poll` /
`__io_uring_get_cqe`. These samples locate the busy path and the sampled wait
states. They do not establish why progress stopped or that every other fiber
was unrunnable. An eventfd count of zero is an instantaneous observation, not
proof that no wake was delivered: the wait path drains the fd on return.
The fd 59 snapshot must not be treated as kernel-state evidence for fd 61 in
the later WAITFD trace without matching run, process, fd, and time identity.

## Withdrawn LWLock explanation

Commit `a141499c86` added a clear of `LW_FLAG_WAKE_IN_PROGRESS` after the wait
in `LWLockAcquireOrWait`. That change is reverted in the bounded integrity
repair; it was not a demonstrated fix and reportedly did not change the wedge.

The flag is upstream PostgreSQL logic, not a branch addition. In
`LWLockWakeup`, `new_wake_in_progress` is set only for a waiter whose mode is
**not** `LW_WAIT_UNTIL_FREE`. `LWLockAcquireOrWait` queues with
`LW_WAIT_UNTIL_FREE` and returns without acquiring when the wait completes.
Unlike `LWLockAcquire`, it does not automatically retry acquisition there.
It must not clear a flag potentially representing another acquiring waiter.
The separate clear in `LWLockWaitForVar` does not justify adding one here.

## Evidence limits and next gate

- The different `tuple` / `BufferExclusive` shape in earlier eight-loop runs
  does not prove a common cause, nor that this failure is impossible at eight
  loops. Preserve the 32-loop workload as a reproducer, not a minimum threshold.
- A clean libxtc strand report is not proof that all libxtc paths are correct.
  Neither PG-side nor libxtc-only fault ownership follows from these stacks.
- Lack of PANIC output does not exclude a fault. Reported responsiveness during
  sampled intervals supports a live stall then, not a complete crash history.
- The write wedge is an open issue, not the only remaining correctness gate.
  See `THREADED_TEST_BASELINE_2026-09.md` for other recorded failures.

No new instrumentation, retry workaround, or error-policy change is part of
this repair. Controlled old/new dependency runs and process/threaded gates
remain pending. EC2 is blocked by lava `InvalidClientTokenId`; no new runtime
validation is claimed here.
