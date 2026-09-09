# LOOP_POLL answers query 2: BRANCH 1 (lost wake) is proven. Your instrument also needs two fixes, one of which invalidated my first verdict.

Date: 2026-09-07
libxtc: **v1.42.0** (tag `3f6a3f6`), debugoptimized.
Re: `/tmp/libxtc-loop-poll-liveness-reply-2026-09-07.md`

--------------------------------------------------------------------------------
## Headline: branch 1. The loop kept polling for 53.8 s while its own fiber stayed parked.

Hang 3 of 3, with your dump-later change (12 s of freeze recorded before dumping):

```
stranded = 17.1.1   aio_parks=5   deficit=1   park_ts = 0.819853 s
its final PARK:   819853139 ns  SCHED PARK  pid=17.1.1  detail=3     (XTC_AIO_FDATASYNC)

loop 17 LOOP_POLL events: 643 total, 27 AFTER that park
   first after-park polls (s): 0.8199, 0.8199, 0.8199, 0.8200, 0.8965, 0.8966
   last  after-park polls (s): 54.5721, 54.5721, 54.5722
   => loop 17 kept polling 53.8 SECONDS after its own fiber parked

any further events for 17.1.1 after its final PARK: 0
```

**Loop 17 was alive and polling its own ring 27 times, across 53.8 seconds, while `17.1.1` sat
in an unresumed `XTC_AIO_FDATASYNC` park.** That is your branch 1: *the loop kept turning and
that one task was skipped.* Per your note, the bug is in **dispatch/waker for that one task**.

This conclusion rests **only on positive evidence** -- events that are present. That matters,
because of the two instrument problems below, both of which can only ever *remove* events.

--------------------------------------------------------------------------------
## Instrument bug 1: `tools/xtc-tail.py` in v1.42.0 does not know kind 8

`KINDS` stops at `7: MBOX_HWM`, so **every LOOP_POLL renders as `?8`**:

```python
KINDS = {
    0: "SPAWN", 1: "EXIT", 2: "WAKE", 3: "RUN", 4: "PARK",
    5: "SEND", 6: "RECV", 7: "MBOX_HWM",
}
```

My first pass grepped for `LOOP_POLL`, got **zero**, and I nearly reported "the whole runtime
quiesced". The events were all there as `?8` -- 14,317 of them. One-line fix:

```python
    5: "SEND", 6: "RECV", 7: "MBOX_HWM", 8: "LOOP_POLL",
```
and in `DETAIL`: `"LOOP_POLL": "events dispatched"`.

## Instrument bug 2 (the important one): LOOP_POLL is 87-93 % of the ring, so ABSENCE proves nothing

This one invalidated a verdict of mine, so I want to be precise about it.

```
hang 1: total=16385  LOOP_POLL=14379  (87 %)   window 57.9 s   RING WRAPPED
hang 2: total=16385  LOOP_POLL=15319  (93 %)   window 52.4 s   RING WRAPPED
hang 3: total=16385  LOOP_POLL=14956  (91 %)   window 54.6 s   RING WRAPPED
```

Your dump-later advice was correct and necessary -- but combined with a poll event per loop per
poll, 32 loops now **saturate the 16384-slot ring**. Consequence, measured in hang 1:

```
loops that emitted LOOP_POLL in the retained window:  10 of 33
home loops that emitted NONE:                          23  (including loop 20)
```

Loop 20 is the stranded fiber's home in hang 1, and it shows **0 polls** -- yet `20.1.1` demonstrably
resumed 22 times, so loops *were* polling for it. **"0 polls" was eviction, not death.**

So my scripted verdicts on hangs 1 and 2 -- "LOOP STOPPED (branch 2)" -- are **WITHDRAWN.** They
were built on `count == 0`, and in a wrapped ring dominated by the very event I was counting, a
zero count is unfalsifiable. Only hang 3, which rests on *present* events, is admissible. My
naive script also mis-called hang 1 "alive" from a single post-park poll 4 us later (the tail of
the pre-park burst), which is the same error in the opposite direction.

**Suggested fixes, either alone is enough:**
1. **A separate class/mask bit and its own ring for LOOP_POLL**, or a much larger ring when
   `XTC_TAIL_LOOP_POLL` is enabled -- it is a fundamentally higher-frequency event than PARK/RUN.
2. **Sample it**: emit every Nth poll per loop, or only when `detail == 0` (an *idle* poll, which
   is the liveness-relevant case), or coalesce into a per-loop counter that the dump renders once.
   An idle-poll-only variant would have answered query 2 with ~2 orders of magnitude fewer events.
3. Have `xtc_tail_dump` report **`dropped`/`wrapped`** counts, so a consumer can tell a real zero
   from an evicted one. I would have caught my own error immediately with that field.

## Corrected candidate set

Your two branches, against the evidence I can defend:

| branch | mechanism | status |
|---|---|---|
| 1 | CQE reaped, wake lost -> dispatch/waker skips that one task | **CONFIRMED (hang 3, positive evidence)** |
| 2 | no CQE posted, loop blocked in `io_uring_wait_cqe(-1)` | not established; my two "confirmations" were eviction artifacts |

Note branch 1 is also the branch **consistent with `unreaped = 0`** on all rings, which was the
constraint you flagged from the three-sample `xtc-rings`. The CQE *was* reaped; the wake went
missing after that.

### One loose end I am NOT presenting as a finding
`xtc-rings` in these three hangs reported a nonzero `unreaped` on a ring (hang 1: fd 560 = 2,
fd 599 = 23; hang 2: fd 560 = 40; hang 3: fd 560 = 31) -- but these are again **single samples**,
which you have already taught me prove nothing, and they sit alongside a `31-37`-row table where
I am not certain I am parsing your columns correctly. I am reporting them only so you have them,
not as support for anything. Note the recurrence of `fd 560` across all three.

## Also
* Your io-wq starvation test (32 fibers, one ring, 8x the cap of 4, all 640 ops completed) is
  noted and I will not revisit it. Thank you for testing your own suspicion to destruction.
* The `n_alive` false-idle fix is in this build. Hang rate unchanged (3 hangs in 3 loaded runs
  here), consistent with your latency-not-strand framing.

## What would close this
For branch 1, the question is now narrow: **which wake path did that one task miss?** The fiber
had completed 4 identical fdatasync park/resume cycles (5 parks, 4 resumes) before the one that
stranded, on a loop that kept polling. If you have a tail event for *the waker side* -- "loop L
dispatched a completion to task T" and/or "task T marked runnable" -- then the missing step
between "CQE reaped" and "task never ran" becomes visible directly. That, plus a LOOP_POLL that
does not evict everything else, and I think this is done.

Repro is ~4 min/hang at ~50-100 % on a 32-vCPU box; I will run whatever you ship.

## Environment
PG "xtc" `2789a4e670`, libxtc v1.42.0, `multithreaded=on`, `pooled_protocol_carriers=0`,
`io_method=xtc`, `fsync=on`, `synchronous_commit=on`, `full_page_writes=on`, c6id.8xlarge
(32 vCPU), XFS on local NVMe, scale=50, `-c 32 -T 8`, dump 12 s after freeze detection.
3 hangs captured (`ld1/ld2/ld3.xtcl` + rendered timelines + `xtc-rings` output available).
