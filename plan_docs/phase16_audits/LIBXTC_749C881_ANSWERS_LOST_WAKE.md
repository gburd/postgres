# Answers to your asks (a)-(e) on rev 749c881: park='-' with wake_pending CLEAR => LOST WAKE, and 749c881 does not change our rate

Date: 2026-09-07
libxtc: pinned to your untagged HEAD **749c881** (built `-Dbuildtype=debugoptimized` so
`libxtc.so.1.41.0` carries `debug_info, not stripped` -- required for `xtc-stranded`).
Tool: your `tools/gdb/xtc-gdb.py` @ b0a019f.  It worked first try, no build flag needed.
Thank you for shipping it -- and for the correction on surface #7.  Both were the right call.

--------------------------------------------------------------------------------
## (a) The discriminator: `park='-'` + `wake_pending` **CLEAR**

Per your table, that means **LOST WAKE** -- "a wake was never delivered, or was consumed and
then lost", i.e. **NOT** the consume-path bug that `749c881` addresses.  So your instinct in
your own reply was right: 749c881 is not our root cause.

`xtc-stranded`, three samples ~1s apart at a caught hang, **byte-identical every sample**:

```
proc states:
    PARKED       69
    RUNNING      1                <-- (the gdb-attached thread)
park kinds (PARKED procs only):
    park=-       33
    park=fd      35
    park=timer    1
```

## IMPORTANT: 31 of those 33 `park='-'` "suspects" are FALSE POSITIVES (ours, not yours)

This is the thing most worth you knowing, because it will save you chasing noise if other
embedders run the tool.  The `park='-'` list is dominated by pids with **local_id == 0**:

```
1.0.1  2.0.1  3.0.1  4.0.1  5.0.1 ... 22.0.1   (one per loop)
```

Those are **our own per-loop supervisor fibers** (`xtc_carrier_supervisor_proc`), which sit in
`xtc_recv(..., -1)` forever waiting for spawn messages.  `park='-'` with no armed source and
no latched wake is their **normal, correct idle state** -- they are not stranded.  The tool
cannot know that, so it flags all 22+ of them.

Filtering to `local_id >= 1` leaves **exactly two real suspects**, and they are stable across
all three samples:

```
sample 1 local_id>=1 park=- :  1.1.1   12.1.1
sample 2 local_id>=1 park=- :  1.1.1   12.1.1
sample 3 local_id>=1 park=- :  1.1.1   12.1.1
```

Both `wake_pending` **clear**.  `iou-wrk` threads present: **19** (so submissions were
accepted and serviced by the kernel, per your caution about a still-pending syscall).

Suggestion for the tool, if you want it: de-emphasise (or footnote) `park='-'` procs whose
`local_id == 0`, or let the caller pass a "these pids are expected idle receivers" filter.
As shipped, an embedder that parks long-lived service fibers in `xtc_recv(-1)` gets a wall of
suspects. The `local_id >= 1` filter was enough for us, but it is a foot-gun.

## (b) proc states

`PARKED 69 / RUNNING 1 / SCHEDULED 0` -- identical in all three samples.

**Zero SCHEDULED.** By your own split in ask (b), that means we have the **lost-wake** shape,
not the lost-reschedule/starvation shape your repro shows.  Combined with (a) this is a fairly
strong statement that **our two hangs are different bugs**, as you suspected.  Ours: everything
PARKED, two non-supervisor fibers with `park='-'` and no latched wake, io serviced.  Yours:
zero `park='-'`, lock FREE, a fiber yield-spinning, millions of `tasks_run`.

## (c) Does 749c881 change our hang rate?

**No -- if anything worse, and certainly not better.** Same harness, fresh server per run,
scale=50, c=32, fsync=on:

```
v1.41.0 (9c5c5ff):  4 HANG / 6 completed runs
749c881          :  run1 HANG, run2 HANG, run3 HANG, run4 HANG, run5 HANG, run6 HANG,
                    run7 PASS (tps=2549), run8 HANG, run9 INIT-HANG
                    => 7 HANG + 1 INIT-HANG / 9
```

Take that as "no improvement" rather than "regression" -- run-to-run variance at this hang rate
is large and I have not run enough samples to claim 749c881 made it worse.  The useful signal
is only: **it did not help**, which with (a) means the two windows are independent.

New datapoint in that sweep: **run 9 hung during `pgbench -i`** (the single-connection
COPY/index/vacuum load), which is the second time I have seen the single-connection phase hang.
Note this does *not* contradict my earlier 9/9 single-connection PASS: a *fresh* server doing
only `-i` is reliably fine; `-i` hangs only when it follows earlier concurrent load on the same
box (loops already churning).

## (e) Filesystem, explicitly

You were right to ask; here it is measured, not remembered:

```
/dev/nvme1n1  xfs  rw,relatime,seclabel,attr2,inode64,logbufs=8,logbsize=32k,noquota
nvme1n1   ROTA=0   Amazon EC2 NVMe Instance Storage
```

**XFS on EC2 local NVMe instance storage** (c6id.8xlarge, 32 vCPU), PGDATA on that mount,
`fsync=on`, `synchronous_commit=on`, `shared_buffers` = 40% RAM.  Not btrfs, not ext4, not EBS.

And to close out your fsync-floor concern: our failure is **not** slow-but-progressing.  At a
hang, server-side `sum(xact_commit)` moves by **2-3 over a 4 second window** (vs ~1500-1900/s
when healthy), and it never recovers over multi-minute observation.  A 6-10ms p99 fsync floor
cannot produce that.  It is a freeze, not a tail-latency artifact.

## (d) --enable-diagnostic

Not run yet -- it is next on my list, and I will send the result separately.  I wanted you to
have (a)-(c) immediately since (a) is the one you said unblocks everything.

--------------------------------------------------------------------------------
## Summary of what this says

* `park='-'` + `wake_pending` **clear** + io demonstrably serviced + frozen across 3 samples
  + **zero SCHEDULED** => our hang is a **LOST WAKE on the aio-completion path**, and it is
  distinct from the lost-reschedule behaviour your standalone repro exhibits.
* `749c881` does not change it (as you predicted).  We are carrying it anyway since it closes a
  real hole.
* The two stranded non-supervisor fibers are `1.1.1` and `12.1.1` -- if you want anything else
  read out of those two procs specifically (park_fd, task fields, the owning loop's ring
  state), name the gdb expression and I will capture it on the next hang; the repro takes
  ~2 minutes.

## Environment
PG "xtc" HEAD (carries our hot-cell thread-safety fix + the ProcSleep bounded-wait guard),
libxtc 749c881 built debugoptimized, c6id.8xlarge (32 vCPU), PGDATA on XFS/NVMe,
multithreaded=on, pooled_protocol_carriers=0 (fiber-per-session), fsync=on.
