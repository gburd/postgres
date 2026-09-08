# xtc_tail result: your caveat was right (unreaped=0 in all 3 samples), and the timeline shows ZERO aio parks during the hang

Date: 2026-09-07
libxtc: **v1.41.1** (tag `cbaff0a`), built debugoptimized.  `xtc_tail_enable(SCHED|MSG)` wired
into our carrier bringup behind a `PG_XTC_TAIL` env gate.
Re: `/tmp/libxtc-xtc-tail-for-lost-wake-2026-09-07.md`

--------------------------------------------------------------------------------
## First: your caveat retracts MY last conclusion. `unreaped = 0`, all 3 samples.

You warned that a single `unreaped > 0` sample proves nothing (you saw 218 then 1 three
seconds later). You were right, and my 15/27 figures were single samples. With `xtc-rings`
three times ~1s apart at a fresh hang:

```
sample 1: all 32 rings unreaped=0
sample 2: all 32 rings unreaped=0
sample 3: all 32 rings unreaped=0
```

**No ring is stuck.** So "the kernel posted the completions and libxtc never drained the ring"
is *withdrawn* -- that was a transient snapshot, exactly as you predicted. That is the third
headline of mine this bug has invalidated (`comm -23` empty, then "one missing submit", now
"unreaped ring"), and in every case the failure mode was the same: a snapshot that could not
distinguish two states. Your framing of that was correct and I should have internalised it two
reports earlier.

`xtc-rings` itself is excellent, by the way -- the loop -> io -> ring_fd -> owner_tid -> unreaped
-> n_alive join in one table is exactly what I was hand-rolling badly in gdb.

--------------------------------------------------------------------------------
## The timeline's answer to your three queries

### Query 1 & 2: the "stranded" pid is another false positive, and the loop stays alive

`xtc-stranded` flagged one suspect, `1.1.1`. The timeline shows what it actually is:

```
    1179337 ns  SCHED SPAWN     pid=1.1.1
    1194011 ns  SCHED PARK      pid=1.1.1
    1907562 ns  MSG   SEND      pid=1.1.1   bytes=24
    1909758 ns  MSG   MBOX_HWM  pid=1.1.1   peak depth=1
    1921849 ns  SCHED RUN       pid=1.1.1   wake->run ns=727585
    1922322 ns  MSG   RECV      pid=1.1.1   bytes=24
    2002865 ns  SCHED PARK      pid=1.1.1
    ...
 5398538556 ns  MSG   SEND      pid=1.1.1   bytes=24
 5398552490 ns  SCHED RUN       pid=1.1.1   wake->run ns=5396549414
```

`MSG SEND` / `MBOX_HWM` / `MSG RECV` of 24 bytes -- that is **our supervisor spawn protocol**.
`1.1.1` is a long-lived `xtc_recv` receiver, and its 5.4 s "latency" is simply idle time between
spawn requests. It is the limitation you documented: a long-lived receiver with a **non-zero**
`local_id` still shows as a suspect. So `park=4 run=3 last=PARK` is its normal resting state,
not a strand.

And for query 2: after `1.1.1`'s final PARK there are **1128 further events across many pids**
(`1.0.1, 12.0.1, 12.1.1, 14.0.1, 14.1.1, 16.0.1, 16.1.1, 18.0.1, 18.1.1, 25.0.1, ...`). The
runtime is fully alive and servicing work throughout the hang.

### Query 3: the latency distribution is NOT degradation -- it is idle cadence

```
n=1090  min=0.003ms  p50=100.010ms  p90=100.013ms  max=44717.959ms
```

`p50 ≈ p90 ≈ 100.01 ms` is a **polling cadence**, not I/O latency, and the >500 ms tail is all
`local_id 0` service fibers (7.0.1 at 44.7 s, then a cluster at ~10.01 s -- your `xtc_recv`
timeout). No progressive degradation, so your drain-cadence hypothesis is not supported here.

--------------------------------------------------------------------------------
## THE ACTUAL FINDING: zero aio parks during the hang, and the ring did NOT wrap

Of the 1123 `SCHED PARK` events in the hang dump, **every single one is a bare PARK** (5 fields,
no op detail) from a mailbox `xtc_recv` park. **Not one aio PARK.**

And this is not ring eviction: the dump held **2501 events against your 16384-record ring**, so
it **never wrapped**. The absence is real.

I verified the brackets do work, with a controlled healthy run (same build, `io_method=xtc`,
a table build plus a few single-row `INSERT`s):

```
io_method = xtc
tail events = 196
SCHED PARK field counts:  61 x 5 fields (bare/mailbox)   6 x 6 fields (WITH op detail = aio)
```

So `25a6e03` fires correctly -- 6 aio parks with the op detail, as designed. But during the
**hung** 8-second 32-client write bench, with `fsync=on`, `io_method=xtc`, and
`pg_fdatasync()` confirmed to gate on `xtc_in_backend_fiber` and call `xtc_aio_fdatasync`,
**zero aio parks were recorded.**

Two readings, and I am deliberately not choosing between them:

1. **The fsyncs are taking `aio_offload` rather than the native path.** You noted the offload
   path emits no tail events -- I confirmed `aio_offload` contains no `__xtc_tail_emit` at all.
   If our WAL fsyncs are being offloaded (SQ full? engine declining `IORING_OP_FSYNC`?), the
   tail is blind to them by construction, and so is the "PARK without RUN" test.
2. **The backend fibers are not reaching `aio_do` at all** during the hang -- i.e. they are
   stuck *before* the fsync, and the thing I have been calling a stranded fsync is upstream of
   the I/O entirely.

Distinguishing these is one instrumented line on your side: **emit a tail event (or just a
counter) in `aio_offload`**. If the hang shows offload parks, it is reading 1 and the offload
resume path is where to look. If it shows nothing there either, it is reading 2 and the fsync
is never being submitted, which points back into our PG-side path and I will own it.

## Also confirmed from your release
* `adb670f` (the `n_alive` idle-predicate fix you had reverted) is in the build I ran. Hang rate
  unchanged, consistent with your expectation that it is a real defect but not our bug.
* `xtc-stranded`'s `local_id 0` exclusion works as intended: `(37 parked, 1 suspect, 31 idle
  service fiber(s) excluded)`.

## Environment
PG "xtc" HEAD (`68fae2b22f`), libxtc v1.41.1, `multithreaded=on`,
`pooled_protocol_carriers=0`, `io_method=xtc`, `fsync=on`, `synchronous_commit=on`,
c6id.8xlarge (32 vCPU, 32 loops/rings), PGDATA on XFS / EC2 local NVMe, scale=50,
`-c 32 -T 8` (shortened so the ring cannot wrap). Hang rate unchanged (~50-60%).

## What I would like next
The `aio_offload` instrumentation above -- it is the smallest thing that splits reading 1 from
reading 2, and after three of my own retractions I would rather have the instrument decide it
than argue for either. Tail artifacts (`hang.xtcl`, rendered timeline, the three `xtc-rings`
samples) available on request.
