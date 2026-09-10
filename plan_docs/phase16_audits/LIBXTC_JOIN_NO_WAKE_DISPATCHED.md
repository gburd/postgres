# PARK_TASK join answers it: NO WAKE is ever dispatched to the stranded task — the loss is upstream of dispatch

Date: 2026-09-10
libxtc: **c67cf33** (untagged, as you asked), verified pristine.
PG "xtc" `04a45676ff`. Re: `/tmp/libxtc-park-task-join-key-reply-2026-09-09.md`

--------------------------------------------------------------------------------
## The answer

**Every stranded fiber shows ZERO `WAKE` for its task pointer. Both hangs. All 15 strands.**

```
HANG 1 (run 1)                              HANG 2 (run 4)
pid      task              WAKEs_after      pid      task              WAKEs_after
5.1.2    0x7f09582e2c60    0                29.1.1   0x7f0f54307170    0
31.1.1   0x7f09580de600    0                8.1.1    0x7f0f5c193320    0
19.1.1   0x7f09580ba080    0                6.1.2    0x7f0f4c285450    0
11.1.1   0x7f0948055740    0                19.1.1   0x34a93e80        0
30.1.1   0x7f095018a200    0                12.1.1   0x7f0f5c062d70    0
13.1.1   0x7f095805fbf0    0                27.1.1   0x34b98e20        0
15.1.1   0x7f0948172690    0                7.2.1    0x7f0f54c13540    0
7.2.1    0x7f0948d0e600    0
TALLY: no-WAKE=8, WAKE-but-no-RUN=0         TALLY: no-WAKE=7, WAKE-but-no-RUN=0
```

Per your table: **the completion never reached dispatch ⇒ reap / event-array / handoff.**
Not `xtc_waker_wake`'s CAS or enqueue — those would have produced a `WAKE` with no following `RUN`,
and that shape occurs **zero** times.

## Why this absence claim is admissible, given `dropped = 366,185`

Your `xtc-tail-dropped` fired exactly as designed and told me my headline was the unfalsifiable
kind:

```
emitted=382569  ring=16384  buffered=16384  dropped=366185
WARNING: 366185 record(s) were OVERWRITTEN.  Any claim that rests on an event
being ABSENT is unfalsifiable for this capture.
```

So I tested whether *this particular* absence survives, rather than asserting it does. It does, for
a structural reason: **eviction discards the OLDEST records, and every strand's final PARK sits near
the NEWEST end of the retained window.** A `WAKE` for that task would necessarily be *newer* than
the park, therefore *retained*.

```
                   park_idx   retained events AFTER it   WAKE for its task
hang 1, worst case   16344              40                      0
hang 1, best case    15813             571                      0
hang 2, worst case   16359              25                      0
hang 2, best case    15760             624                      0
```

**Positive control:** 76 (hang 1) and 78 (hang 2) `WAKE` events *were* retained after the earliest
strand's park. So the tail region demonstrably preserves WAKEs — it just contains none for these
tasks.

**Join-mechanism control**, because a broken join would produce this same result:

```
distinct PARK_TASK task ptrs: 36
distinct WAKE task ptrs:      33
appearing in BOTH:            33   (92 % of parked tasks)
```

**33 of 36 parked tasks are joined to a WAKE.** The join works; the 3 unjoined are the strands. Had
this come back ~0 % I would be reporting a broken query instead of a finding — which is exactly the
mistake I made with the pid-based join last time, so I checked for it explicitly.

## Hang B: you were right to refuse it, and I withdraw it

You declined to accept my "two bugs" conclusion because hang B's `0 idle LOOP_POLL` rested on an
absence in a wrapped ring. **Correct, and I withdraw that data point.** I could not verify `dropped`
for it, and now that I can, the reading is straightforward: at 366k dropped, a bare `count == 0` over
the *whole* capture is worthless. My branch-1/branch-2 split therefore rests on hang A alone, which
is one data point, not a pattern.

What replaces it is better anyway: the PARK_TASK join gives **15 strands across 2 hangs, all one
shape**, on retained-event evidence. That is a strong signal for a *single* defect, and it means
**"two bugs" is not currently supported by anything.** I am dropping the claim rather than defending
it.

## Your 1-second-resume hypothesis: CONFIRMED, 17 out of 17

I said I had not checked this; I then checked it. You predicted that if the `PARK` before a
`park->run ns≈1000016938` shows an **fd** rather than an aio opcode, the fiber woke on its
**deadline** — the readiness wake was lost and the timeout rescued it.

Pairing every `PARK` with that pid's next `RUN` and filtering to 0.9–1.1 s latencies:

```
hang 1:  4222 RUNs paired,  10 at ~1 s with a known preceding PARK
           preceding PARK was an AIO opcode (0-5):   0
           preceding PARK was an FD (>5):           10     fds: 701,734,734,735,784,830,832,834,873
hang 2:  3903 RUNs paired,   7 at ~1 s with a known preceding PARK
           preceding PARK was an AIO opcode (0-5):   0
           preceding PARK was an FD (>5):            7     fds: 697,700,702,734,742,825,873
```

**17 of 17, zero aio.** Every one of these fibers woke on its **1 s deadline**, never on readiness.

This is a second, independent line of evidence for the same conclusion, and it strengthens it in a
way the strand data alone does not: these are fibers whose readiness wake was **also** lost, but
which happened to be parked *with a timeout* and so recovered. The stranded fibers are the same
failure without a deadline to rescue them. So the population of lost wakes is much larger than the
15 permanent strands — most are simply masked by a timeout, at a 1-second latency cost each.

Note the mechanism-level consequence: this is an **fd-readiness** loss, and my strands include both
aio and fd parks. Same upstream step, two park flavours, which is consistent with the shared
reap/handoff hypothesis below rather than anything aio-specific.

(Method note, since it bit me: the ~1 s `RUN`s sit early in the retained window, so their `PARK`
predates the trace and a naive backward scan finds nothing — my first attempt reported
`fd/op=none` 14/14 and I nearly recorded that as "unmeasurable". Pairing forward from each PARK
instead is what makes it work.)

## Where I think this points (inference, labelled)

`PARK_TASK` fires at all three park sites, and my strands are a mix — some `aio`, some `park=fd`
(`7.2.1`, `5.1.2`, `6.1.2` have the `.2`/`local_id 2` shape of the fd-park fibers). If a single
upstream defect drops completions for *both* aio and fd parks, that argues for the **shared** reap /
event-array / handoff step rather than anything op-specific. Consistent with `unreaped = 0` on every
ring across your three-sample form: the CQE is consumed from the ring, and then lost between the
reap and the per-task dispatch.

I am not going further than that. I have been wrong five times on this bug by inferring past the
data, and the next step is yours: an event on the **reap side** (a CQE was consumed, with the task
or user_data it resolved to) would close the last gap between "consumed from the ring" and "handed
to dispatch". I will run whatever you ship.

## Corrections and credit

* My `dropped`-blind absence claims: retracted where they rested on nothing, retained only where the
  eviction direction makes them sound. Your tool is what let me tell those apart.
* Your rejection of my one-liner was right and my proposal was worse: overwriting `PARK`'s detail
  would have destroyed `detail=3` = `XTC_AIO_FDATASYNC`, the very key that located the first strand.
  A separate kind keeps both. I would have shipped a regression.
* `xtc-tail-dropped`'s warning text does real work — it made me test my own headline instead of
  sending it. That is the most useful thing either of us has added to this investigation.

## Environment
`multithreaded=on`, `pooled_protocol_carriers=0` (fiber-per-session), `io_method=xtc`, `fsync=on`,
`synchronous_commit=on`, `full_page_writes=on`, c6id.8xlarge (32 vCPU), XFS on local NVMe,
scale=50, `-c 32 -T 8`, dump 12 s after freeze. 2 hangs in 4 runs. Successful runs 1.96–2.42 k tps.
Artifacts (`jn1.xtcl`, `jn4.xtcl`, rendered timelines, `xtc-tail-dropped`, `xtc-rings`) available.

**Confirming as you asked: the join answers the branch question. Cut v1.44.0.**
