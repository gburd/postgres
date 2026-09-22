# Write wedge: sampled xtc_proc_wait_fd registration errors; root cause open

> **SUPERSEDED DIAGNOSIS -- corrected 2026-09-22.** The September 17 claim of a
> complete, libxtc-only root cause is withdrawn. The sampled return values are
> useful evidence, but the probes did not establish zero wakes, an exact error
> count, matching fd state, or a minimum loop count. This report is historical
> evidence, not a current test result or a request to wait for an unverified fix.

## Observations retained from September 17

On the reported 32-vCPU EC2 SUT with libxtc v1.48.1 / uring, write c=32 stopped
making progress after about 12 seconds, with roughly 25 WALWrite waiters.
One carrier was busy in
`XLogFlush -> LWLockAcquireOrWait -> ProcWaitOnSemaphore -> read()`;
31 exec loops were sampled in `xtc_io_poll`. The WAITFD probe recorded:

```
WAITFD fd=61 interest=0xd rc=-6 revents=0x0 timeout_ns=-1 n=35120000
WAITFD fd=61 interest=0xd rc=-6 revents=0x0 timeout_ns=-1 n=35140000
```

`0xd` is READABLE|HUP|ERR; `-1` is an infinite timeout; `-6` is
`XTC_E_INTERNAL`. These are sampled failed calls, not a count of all failures.

## Corrections to the earlier evidence claims

| Earlier claim | Correct interpretation |
|---|---|
| ZERO writes / not a wake storm | WAKESTORM logged only every 5000 counted fd writes. Missing log lines cannot establish zero writes; the probe did not count pending-flag wakes either. Its shared static enable flag and unlocked PGPROC counter were unsafe under concurrency. |
| 35,000,000+ returns of -6 on this fd | `wf_n` was an unsynchronized process-wide counter across fds and return codes, sampled every 20000 calls. Its value is neither an exact total nor a per-fd/per-error count. |
| fd 61 / fd 59 were proven idle and valid throughout the spin | The fd 59 kernel snapshot is not a matched fd 61 snapshot. Even a matched zero count is instantaneous and cannot exclude earlier drained wakes. |
| Empty `pg_blocking_pids()` identifies the LWLock chain head | That function reports heavyweight-lock blockers, not LWLock ownership or queue order. |
| 0 PANIC proves a wedge and no crash | Responsiveness supports a live stall during observations; absent PANIC is not a complete fault/supervision record. |
| The eight-loop case cannot enter this state | No controlled loop-count sweep establishes impossibility or a threshold. |
| The extra LWLock flag clear is a genuine latent fix | `LW_FLAG_WAKE_IN_PROGRESS` is upstream logic; `LW_WAIT_UNTIL_FREE` is excluded from setting it in `LWLockWakeup`. The unjustified clear from `a141499c86` is reverted. |

The probes from `cffd4b0a57` and `4a7af72bd1`, including the added PGPROC field,
are removed rather than used to support further quantitative claims.

## What the source actually narrows

In libxtc v1.48.1 (`560a5bc5e12e4b677b5c4a21c633d5592d5b9b4c`),
`src/ptc/proc.c:xtc_proc_wait_fd` maps a non-OK return from
`xtc_io_reg_fd(wl->io, fd, interest, self->task)` to `XTC_E_INTERNAL`.
The other explicit INTERNAL returns in that function are timer setup failures,
which are not on the `timeout_ns=-1` path. Thus the sample locates a
**registration failure**, but discards the underlying registration return code.
It does not distinguish duplicate registration, allocation/submission failure,
or the state that led to the failure. A valid fd alone is not enough to prove
registration must succeed, and the error name is not proof of fault ownership.

The public `xtc_proc.h` return list omits INTERNAL. That is a source/contract
mismatch, not evidence that the embedder has been exonerated.

## Existing PostgreSQL mapping (unchanged)

```c
if (rc != XTC_OK && rc != XTC_E_AGAIN)
    return WL_LATCH_SET;   /* treat as a wakeup; caller re-checks */
```

`ProcSemaphoreWaitFiber` ignores the returned event mask, disarms and drains
the fd, then returns to the caller's predicate check. Repeated registration
failures while `lwWaiting` remains waiting can therefore drive repeated calls
without a successful park. This explains a possible spin mechanism; the
samples do not prove the complete causal chain or why registration failed.
No error-policy change or retry workaround is made in this repair.

## New dependency source, not yet a runtime verdict

The clean v1.49.2 checkout at
`542a67d9ea37a425a2ab7d6b11dff8993bd42871` was read on September 22:

- `d18481b` changes uring registration lifetime: `xtc_io_mod_fd` adds a fresh
  node and retires the old one rather than reusing a node across poll
  generations. `xtc_io_reg_fd` still rejects duplicate registration.
- `9f196cb` changes cancellation/park handling. The current wait-fd path couples
  mailbox observation and waker arming, disarms the waker on registration
  failure, and still maps that failure to INTERNAL. The header still omits it.

These changes touch relevant paths. This source inspection does **not** show
whether v1.49.2 fixes the September 17 failure; no pin is changed here.

## Remaining validation

Use matched old/new dependency builds, fresh servers, recorded effective
configuration and the original 32-loop write workload. Capture matching task,
registration and kernel fd identity before cleanup, and keep process-mode,
threaded, worker and teardown gates separate from the historical throughput
matrix. Other recorded threaded failures remain open; this is not the sole
gate. EC2 validation is pending lava credential repair (`InvalidClientTokenId`);
no AWS launch or new runtime result is part of this bounded repair.
