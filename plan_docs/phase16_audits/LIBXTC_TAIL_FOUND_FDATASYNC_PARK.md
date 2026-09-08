# xtc_tail FOUND IT: pid 27.1.1 parked on XTC_AIO_FDATASYNC (op=3) and never resumed -- 41 clean cycles then one dead park

Date: 2026-09-07
libxtc: **v1.41.1** (tag `cbaff0a`) + one probe of my own in `aio_offload` (see below).
`xtc_tail_enable(SCHED|MSG)` wired into our carrier bringup behind `PG_XTC_TAIL`.
Re: `/tmp/libxtc-xtc-tail-for-lost-wake-2026-09-07.md`

--------------------------------------------------------------------------------
## The instrument worked. Here is the stranded fiber, on the timeline.

`27.1.1`, last eight events:

```
 6480797307 ns  SCHED RUN   pid=27.1.1   wake->run ns=125704
 6486235704 ns  SCHED PARK  pid=27.1.1   detail=3
 6486279589 ns  SCHED RUN   pid=27.1.1   wake->run ns=43758
 6490442763 ns  SCHED PARK  pid=27.1.1   detail=3
 6490548983 ns  SCHED RUN   pid=27.1.1   wake->run ns=105847
 6493435998 ns  SCHED PARK  pid=27.1.1   detail=3
 6493499720 ns  SCHED RUN   pid=27.1.1   wake->run ns=63585
 6496553098 ns  SCHED PARK  pid=27.1.1   detail=3      <-- NO MATCHING RUN. EVER.
```

`detail=3` is `XTC_AIO_FDATASYNC` (`xtc_io.h:58`). So: **42 aio fdatasync parks, 41 resumed in
43-126 microseconds, and the 42nd never resumed.** That is your PARK-without-RUN criterion,
met exactly once, on the op we predicted.

The PARK/RUN deficit analysis isolates it unambiguously:

```
pids with aio parks: 35
pids with PARK > RUN: 34
  ... but 33 of those have aio_park=0  (the local_id 0 / 1.1.1 mailbox service fibers
                                        resting in xtc_recv -- the known false-positive class)

STRANDED CANDIDATE (deficit>0 AND aio_park>0):
    27.1.1   aio_parks=42   deficit=1   last_park_ts=6496553098
```

**One fiber. One dead park. On `fdatasync`.** This is the first time any instrument has named
the stranded fiber and its wake source together.

## Your query 2, answered: the loop STOPS -- it does not keep working and skip it

You said this distinction needs different fixes, so it is the important one:

```
27.1.1's final PARK is event 3522 of 3533   -> only 11 events follow, across 2 pids
events after it on loop 27 (pid=27.*):      0
SCHED RUN events anywhere after it:          2
```

**Nothing further happens on loop 27 at all**, and the whole trace effectively ends ~11 events
later. So this is your "**everything on that loop stops at the same instant**" case, not the
"loop alive and servicing completions but skipped this fiber" case.

## Two of my own hypotheses eliminated (I instrumented both rather than guess)

In my previous note I gave two readings for the zero-aio-park dump and said I would not choose.
I have now closed both:

1. **"the fsyncs go via `aio_offload`, invisible to the tail"** -- **ELIMINATED.** I added a
   counter at the top of `aio_offload` (every 64th entry appended to a file). Result:
   **0 offload entries** in a healthy control run *and* **0 in the hang**. Our fsyncs never take
   the offload path; they are all native io_uring. (Confirmed independently: `io_method = xtc`,
   and `pg_fdatasync()` gates on `xtc_in_backend_fiber` -> `xtc_aio_fdatasync`.)
2. **"the backends never reach `aio_do`"** -- **ELIMINATED.** This run's tail carries **1546
   PARKs with the op detail** (vs 140 bare), so backends reach `aio_do` constantly.

Which also explains my earlier "zero aio parks": that dump had **2501** events and this one has
**3533** -- the earlier capture simply caught a window still dominated by pre-load mailbox
traffic. My "the absence is real, the ring did not wrap" claim was wrong; the ring bound was not
the issue, the *window* was. Shortening the load to `-T 8` and dumping promptly fixed it.

## Corrected status of my earlier claims (so you are not carrying stale conclusions)

| my earlier claim | status |
|---|---|
| `comm -23` empty => all completed, loss downstream | withdrawn (tag reuse) |
| one submit never completed => submit-side | withdrawn (the `cmp` line only fires on reap) |
| `cq_unreaped > 0` => libxtc never drained the ring | **withdrawn** -- your three-sample form shows **unreaped=0 on all 32 rings in all 3 samples** |
| zero aio parks during the hang | withdrawn (capture window, not ring wrap) |
| **27.1.1 parked on op=3 fdatasync with no RUN, loop 27 then silent** | **stands, and is the finding** |

Your caveat about single samples was right and cost me three headlines; the timeline is the
first instrument here that has not produced a retraction.

## What I think this now points at (stated as inference, not fact)

41 consecutive fdatasync park/resumes at ~50-125 us, then one park that never resumes, with
**all activity on that loop ceasing at the same instant** and **no unreaped CQEs on any ring**.
That combination reads to me as: the loop that owned `27.1.1` stopped polling/dispatching
entirely at that moment, rather than a single completion going missing. But I cannot see loop
27's worker state from the timeline, and I have been wrong three times by inferring past the
data, so I will stop there.

The obvious next instrument, if you want it, is whatever would show **loop 27's worker** at that
instant -- a tail event on loop entry/exit of `xtc_io_poll` per loop, or a `SCHED` event when a
worker blocks/unblocks. Then "the loop stopped polling" becomes visible rather than inferred.
Say the word and I will run it; the repro is ~2 minutes and hits ~50-60%.

## Environment
PG "xtc" HEAD (`139793dfe8`), libxtc v1.41.1 + my `aio_offload` counter, debugoptimized,
`multithreaded=on`, `pooled_protocol_carriers=0`, `io_method=xtc`, `fsync=on`,
`synchronous_commit=on`, c6id.8xlarge (32 vCPU, 32 loops/rings), PGDATA on XFS / EC2 local
NVMe, scale=50, `-c 32 -T 8`.

Artifacts available: `sa.xtcl` + rendered timeline (3533 events), the three `xtc-rings` samples,
`xtc-stranded` output, and the offload counter files.
