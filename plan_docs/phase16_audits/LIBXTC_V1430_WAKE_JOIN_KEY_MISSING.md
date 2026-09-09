# v1.43.0: lost wake CONFIRMED, and the WAKE↔PARK join key is missing (so the branch question is still open)

Date: 2026-09-09
libxtc: **v1.43.0** (tag `b977d21`), debugoptimized, clean tree (no local probes).
PG "xtc" `1a314376d3`. Re: `/tmp/libxtc-loop-poll-liveness-reply-2026-09-07.md`

--------------------------------------------------------------------------------
## 1. Thank you — all three asks shipped, and two of them worked immediately

* `XTC_TAIL_WAKE` from dispatch (c27300d) — the waker side.
* `LOOP_POLL` made **idle-only** + `xtc_tail_dropped()` (4bdaca9) — this directly fixes the
  unfalsifiability that forced me to withdraw my branch-2 verdicts. LOOP_POLL is now **1424/13817
  (10 %)** and **1786/16385 (11 %)** of the ring instead of 87–93 %. Absence is interpretable again.
* `tools/xtc-tail.py` taught kind 8 — my `?8` bug, fixed.

Also fixed on my side: I had started to OR `XTC_TAIL_LOOP_POLL` into `xtc_tail_enable()`. It is an
event **kind** (=8), not a mask bit (only SCHED/MSG/IO/OS are bits) — both new kinds ride
`XTC_TAIL_SCHED`. My error, corrected before it shipped.

--------------------------------------------------------------------------------
## 2. The hang still reproduces on v1.43.0

2 hangs in 7 runs (~29 %), same shape as v1.41.1/v1.42.0. Successful runs are **2.0–3.6 k tps**
(one at 24 tps — degraded but completing), versus stackless-pooled ~37.9 k and fork ~42–46 k.

```
hang A (run 6): 13817 events  PARK 4545  RUN 4480  WAKE 3173  LOOP_POLL 1424  SPAWN 72  EXIT 4
hang B (run 7): 16385 events  PARK 5145  RUN 5137  WAKE 4310  LOOP_POLL 1786  SPAWN  1  EXIT 1
```

**32 and 33 pids respectively end on an unmatched PARK** — far more than the single stranded
fiber I saw on v1.41.1, and it is now a broad stall rather than one victim.

A representative strand, showing the healthy-then-dead rhythm:

```
1051533167 ns  SCHED RUN   pid=25.1.1  park->run ns=1000016938
1051784705 ns  SCHED PARK  pid=25.1.1  detail=839
1051826403 ns  SCHED RUN   pid=25.1.1  park->run ns=41498
1051997074 ns  SCHED PARK  pid=25.1.1  detail=839
1052015742 ns  SCHED RUN   pid=25.1.1  park->run ns=18482
1052055222 ns  SCHED PARK  pid=25.1.1  detail=45      <-- never resumed
```

Note the `park->run ns=1000016938` — a **1.000000-second** resume. That is suspiciously exactly
1 s, i.e. a timeout/retry cadence rather than an I/O completion, and it recurs. Flagging it as an
observation, not a claim.

--------------------------------------------------------------------------------
## 3. THE BLOCKER: `XTC_TAIL_WAKE` and the aio `XTC_TAIL_PARK` do not share a join key

`xtc_tail.h` says the join key is the task pointer: WAKE carries it in `detail` because "dispatch
has a task, not a proc", and instructs matching it "against the task pointer an aio PARK records".

**The aio PARK does not record a task pointer.** In `src/ptc/aio.c:254`:

```c
__xtc_tail_emit(XTC_TAIL_SCHED, XTC_TAIL_PARK, self_pid, (uint64_t)op);
```

`detail = op`. Measured in my captures, the two fields are plainly different key spaces:

```
WAKE detail: min=1014954624  max=139933326905504     <- real pointers
PARK detail: 2, 3, 11, 13, 15, 17, 19, 21, 23, 25, 29, 31, ..., 45, 708, 793, 839
```

So:
* WAKE gives me `(loop_id, task*)` with **no pid** (`memset` zeroes it; only `loop_id` is set).
* PARK gives me `(pid, op)` with **no task***.
* There is no task→proc back pointer in the trace.

**Therefore the question your event was built to answer — "was a WAKE dispatched to the stranded
task?" — is still not answerable from the trace alone.**

I want to be explicit that my first pass got this wrong: I joined WAKE to PARK **by pid** and got
"0 WAKE events for the stranded pid", which reads like a decisive "the completion never reached
dispatch". That result was **true by construction** — WAKE has no pid — and I am withdrawing it
before it misleads either of us. Joining on `detail` instead fails too, for the reason above. That
would have been my fifth retraction on this bug; catching it before sending is the only reason it
is not.

### The one-line fix that closes this
Make the aio PARK carry the task pointer, and put the op somewhere non-conflicting:

```c
/* src/ptc/aio.c ~254 */
__xtc_tail_emit(XTC_TAIL_SCHED, XTC_TAIL_PARK, self_pid, (uint64_t)(uintptr_t)t);
```
…or add a distinct kind (e.g. `XTC_TAIL_PARK_AIO`) whose detail is the task pointer, or emit both
(one PARK with op, one with the task). Any of those makes the WAKE↔PARK join real, and then a
single hang capture answers it definitively.

If you would rather not change the payload: expose the task pointer for a parked proc in
`xtc-procs`/`xtc-stranded` output (a `task` column, which `xtc_tail.h` already implies exists),
and I can join the gdb snapshot to the WAKE stream instead.

--------------------------------------------------------------------------------
## 4. What the liveness data now says (this part IS trustworthy)

With idle-only LOOP_POLL, absence is meaningful again, and the two hangs differ:

| hang | stranded pid | idle LOOP_POLL on its loop AFTER the park |
|---|---|---|
| A (run 6) | `3.1.1` (loop 3) | **164** |
| B (run 7) | `25.1.1` (loop 25) | **0** |

Hang A is your **branch 1** (loop demonstrably alive, kept idle-polling 164 times, that fiber
skipped). Hang B looks like **branch 2** (loop silent) — but I am NOT asserting that, because I
have not yet confirmed `xtc_tail_dropped() == 0` for that capture: my probe printed the dropped
count from `xtc-rings`/gdb output and came back empty, so I could not verify the ring did not lose
those records. Hang B's ring was exactly full (16385 = wrapped), hang A's was not (13817).

**If both branches genuinely occur, this is two bugs, not one** — which would explain why eight
successive cross-loop fixes each helped and none closed it.

To make `dropped` usable from a dump, is there a gdb-side accessor (an `xtc-tail-dropped`, or
having `xtc-tail-dump` print it in a header line)? Calling `xtc_tail_dropped()` from a hung
process under gdb is awkward, and it is the field that makes every absence claim in this report
falsifiable.

--------------------------------------------------------------------------------
## 5. Corrected status of my claims across this whole investigation

| claim | status |
|---|---|
| `comm -23` empty ⇒ all completed | withdrawn (tag reuse) |
| one submit never completed | withdrawn (`cmp` only fires on reap) |
| `cq_unreaped>0` ⇒ ring never drained | withdrawn (your 3-sample form: unreaped=0 ×3) |
| zero aio parks during the hang | withdrawn (capture window, not ring wrap) |
| loop stopped (hangs 1–2 on v1.42.0) | withdrawn (LOOP_POLL evicted; unfalsifiable) |
| **"no WAKE dispatched to the stranded task"** | **withdrawn before sending — pid join is invalid** |
| pid 17.1.1 parked on fdatasync, loop polled 53.8 s after (v1.42.0) | stands (positive evidence) |
| hang reproduces on v1.43.0, 32–33 pids end on unmatched PARK | stands |
| WAKE and aio PARK share no join key | stands (source + measured key ranges) |

## Environment
`multithreaded=on`, `pooled_protocol_carriers=0`, `io_method=xtc`, `fsync=on`,
`synchronous_commit=on`, `full_page_writes=on`, c6id.8xlarge (32 vCPU), XFS on local NVMe,
scale=50, `-c 32 -T 8`, dump 12 s after freeze detection. Artifacts (`w6.xtcl`, `w7.xtcl`,
rendered timelines, `xtc-rings`, `xtc-stranded`) available on request.

## Ask, in priority order
1. **The join key** (aio PARK carries the task pointer) — one line, and it settles branch 1 vs 2.
2. **`dropped` visible in a dump** — makes every absence claim falsifiable.
3. Then I re-run immediately: repro is ~4 min/hang at ~29–60 %.
