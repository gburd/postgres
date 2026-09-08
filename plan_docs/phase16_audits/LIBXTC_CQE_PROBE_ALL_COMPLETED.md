# Probe result: MISSING_COUNT=0 -- every CQE arrived. The loss is downstream of the reap.

Date: 2026-09-07
libxtc: your HEAD **27d327b**, plus your tracing patch applied verbatim (helper +
`xtc_io_aio_submit` submit site + the `a->done = 1` reap site), built
`-Dbuildtype=debugoptimized` (`libxtc.so.1.41.0`: `not stripped`).
Re: `/tmp/libxtc-lost-wake-cqe-probe-reply-2026-09-07.md`

--------------------------------------------------------------------------------
## THE ANSWER: `comm -23` is EMPTY

```
submits(unique tags)     = 65
completions(unique tags) = 65
submitted-but-NEVER-completed (comm -23):   <nothing>
=> MISSING_COUNT=0
```

**Every AIO submission completed.** So by your own split, this is **not** the submit side --
the kernel completed everything we submitted, for XFS on that NVMe device. The loss is
**downstream of the reap**: we got the CQE and lost the wake between reaping it and the fiber.
Per your framing, that puts it in `__xtc_loop_dispatch_event` -> `xtc_waker_wake` for our
topology (22-32 loops, fiber-per-session), not in submission.

Your `xtc-stranded` local_id-0 fix works exactly as intended, by the way -- the output is now
readable at a glance:

```
proc states:  PARKED 68   RUNNING 1   (SCHEDULED 0)
park kinds:   park=- 33   park=fd 34   park=timer 1

0x64e38c0    1.1.1    -   clear   <-- SUSPECT
0x66b0ef0   27.1.1    -   clear   <-- SUSPECT
(68 parked, 2 suspect, 31 idle service fiber(s) excluded)
```

## Your two smaller asks -- and the answers DIVERGE between the two suspects

This is the part I would not have guessed, and I think it matters:

| suspect | `wake_revents` | `state` | `park_fd` | `park_timer` | `park_requested` | home_loop |
|---|---|---|---|---|---|---|
| `1.1.1`  (0x64e38c0) | **65536 = 0x10000** | 2 | -1 | nil | 0 | 0x649d020 |
| `27.1.1` (0x66b0ef0) | **0** | 2 | -1 | nil | 0 | 0x64db200 |

### (i) `wake_revents` is `XTC_WAIT_MAILBOX`, not `XTC_IO_AIO`

You asked specifically whether bit `0x20` (`XTC_IO_AIO`) was set, on the theory that dispatch
had run for that task. On suspect `1.1.1` the value is **`0x10000`**, which per your headers is
**`XTC_WAIT_MAILBOX`** (`src/inc/xtc_proc.h:327`), *not* `XTC_IO_AIO` (`0x20`,
`src/inc/xtc_io.h:36`).

So for `1.1.1`, what last touched `wake_revents` was a **mailbox** wake (a cross-thread
`xtc_proc_wake` / send), **not** an AIO-completion dispatch. And suspect `27.1.1` has
`wake_revents == 0` -- nothing ever or'd anything into it.

I want to be careful not to over-read this, since `wake_revents` is cleared/re-armed across
parks, so `0x10000` may be residue from an earlier mailbox wake rather than the wake for the
park it is stuck in. But the plain reading of the pair is:

* `27.1.1`: dispatch never ran for this task (`wake_revents == 0`) -- yet its CQE demonstrably
  arrived (MISSING_COUNT=0). Reaped, never dispatched to this task.
* `1.1.1`: the last thing recorded was a MAILBOX wake, not the AIO completion.

Neither of them has `XTC_IO_AIO` set. If your expectation was that an AIO-completion dispatch
would leave `0x20` there, that expectation is not met on either suspect.

### (ii) Different home loops

`1.1.1` -> `home_loop=0x649d020`; `27.1.1` -> `home_loop=0x64db200`. **Different loops**, so by
your own criterion this is a **global race**, not one loop's ring or worker.

### (iii) Both are cleanly parked with NO armed source

`park_fd=-1`, `park_timer=nil`, `park_requested=0`, `state=2` on both -- and `state=2` is
`XTC_TS_PARKED` (verified in `src/inc/loop_int.h:36`: SCHEDULED=0, RUNNING=1, PARKED=2,
DONE=3), consistent with `xtc-stranded`'s classification.  So: parked, no fd, no timer, no
latched wake, and the completion they were waiting
for did arrive. That is a lost wake with no remaining re-delivery path, and it is now localised
to the reap -> dispatch -> waker chain.

## Reproduction / environment (unchanged)
PG "xtc" HEAD, libxtc 27d327b + your tracing patch, `multithreaded=on`,
`pooled_protocol_carriers=0` (fiber-per-session), `fsync=on`, `synchronous_commit=on`,
c6id.8xlarge (32 vCPU), PGDATA on **XFS / EC2 local NVMe**, scale=50, `-c 32 -T 20`.
Hang caught on run 1 of the probe sweep (still ~50-60%). Trace files
`/tmp/xtc-aio.{sub,cmp}` were reset per run so the counts above are for the hung run only.

## What I can run next, cheaply
* Any additional gdb expression on `0x64e38c0` / `0x66b0ef0` -- name it and I will capture it
  on the next hang (~2 min/attempt).
* If you want the AIO tag <-> task correlation made explicit (so we can say *which* of the 65
  completions belonged to a stranded task), give me the field to print (`a->tag` is the task,
  so a `printf` of `a` and `a->tag` in the reap would let me join the trace to the suspects) --
  I did not add that because your patch traced by `a` only, and I did not want to alter your
  instrumentation and muddy the comparison.
* A dispatch-side trace (log every `__xtc_loop_dispatch_event` for `XTC_IO_AIO` with the task
  pointer and the CAS outcome) is the obvious next probe now that submission is exonerated --
  happy to apply one if you send it, or I can write it.

Thanks for the `local_id 0` fix and for the honesty about the `n_alive` predicate revert. The
`comm -23` design was exactly right: one hang, one number, and it eliminated an entire half of
the search space.
