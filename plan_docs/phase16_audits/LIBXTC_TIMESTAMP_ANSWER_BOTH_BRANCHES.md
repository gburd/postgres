# The timestamp answer: BOTH branches, split by loop — loop 0 is drain-side, the worker loops stopped being entered

Date: 2026-09-11
libxtc: **c1a7bda** traces (postdate `55c9d5f`, so they carry SUBMIT), analysed with `57bde2c` tools.
PG "xtc" `a3b654a12b`.
Re: `/tmp/libxtc-cqes-join-and-contradiction-reply-2026-09-11.md`

--------------------------------------------------------------------------------
## Your question, answered from the traces I still had

> *"For a stuck ring, is there an idle `LOOP_POLL` for that loop_id whose timestamp is LATER than
> the `SUBMIT` of a CQE now sitting unreaped?"*

**Both, and it splits by loop — the same way in both hangs.**

```
HANG 1 (ov2)   stuck rings: fd 560 (loop 0), fd 569 (loop 3)
  loop 0:  idle polls 17   real unreaped submits 1   with a LATER idle poll: 1   -> DRAIN-SIDE
             task=0x7fa6e01729f0  submit@12.413ms -> first idle poll after @12.416ms (+0.002ms), 2 total
  loop 3:  idle polls 22   real unreaped submits 6   with a LATER idle poll: 0   -> ring stopped being entered

HANG 2 (ov4)   stuck rings: fd 560 (loop 0), fd 581 (loop 7)
  loop 0:  idle polls 21   real unreaped submits 4   with a LATER idle poll: 4   -> DRAIN-SIDE
             task=0x1e281910        submit@13.908ms -> first idle poll after @14.333ms (+0.425ms), 4 total
             task=0x1e281910        submit@14.170ms -> +0.163ms, 4 total
             task=0x7fa11c22aca0    submit@14.819ms -> +0.062ms, 3 total
             task=0x7fa11c18bc20    submit@14.919ms -> +0.002ms, 2 total
  loop 7:  idle polls 18   real unreaped submits 4   with a LATER idle poll: 0   -> ring stopped being entered
```

So your two facts do not actually contradict — **they describe different loops.**

* **Loop 0** has idle polls *after* a submit that was never reaped. Per your table that is a
  **drain-side** bug in `xtc_io_poll`: a poll ran with the completion already submitted and took
  nothing.
* **Loops 3 and 7** (the worker loops carrying the fdatasync strands) have **zero** idle polls
  after their unreaped submits — every idle poll predates them. That is your second branch:
  **the ring simply stopped being entered**, which is scheduler-side.

The pattern reproduced identically across two independent hangs, which is what makes me willing
to report it as a split rather than noise.

## Two corrections to my own analysis, both of which I nearly shipped

**1. `fd 560` is not a mystery. It is loop 0.**
You said you could not explain `fd 560` recurring across five hangs. The mapping is arithmetic:

```
loop_id:   0    1    2    3    4    5    6    7    8
ring_fd: 560  563  566  569  572  575  578  581  584
```

`ring_fd = 560 + 3 * loop_id`, exactly. So "`fd 560` keeps appearing" simply means **loop 0 keeps
appearing** — and loop 0 is your supervisor/service loop, which is a different animal from the
worker loops. The five-hang recurrence is real but it is not a property of an fd; please drop the
mystery framing (I supplied it, so this is my correction to make).

That also reframes the split above: it is not "two random loops behave differently", it is
**loop 0 versus the worker loops**, which is a structural distinction.

**2. My first pass was wrong because `task=0x0` submits are never reaped by design.**
My initial join reported "DRAIN-SIDE" for *all four* loops, with every single hit showing
`task=0x0`. Those are the discarded `poll_remove` cancels your reply describes — they never
produce a REAP, so a naive "submitted but never reaped" filter selects **exactly the wrong
population**: 526 and 465 of them per trace, swamping the handful of real ones.

Excluding `task=0x0` leaves 1/6 and 4/4 real unreaped submits per loop, and *then* the split
appears. Had I not noticed the uniform `0x0`, I would have sent you "drain-side, unanimous,
four loops of four" — a clean-looking and completely wrong answer that would have pointed you at
one subsystem when two are involved. Recording it because the near-miss is the useful part.

## What I am claiming, and what I am not

* **Claimed:** on both hangs, loop 0 has an idle poll strictly after a real (non-cancel) submit
  that was never reaped; loops 3 and 7 do not. Timestamps above, method below.
* **Claimed:** `ring_fd = 560 + 3*loop_id` on this build, so fd↔loop is arithmetic.
* **Not claimed:** that the CQEs I identify as "unreaped" are the *same* CQEs `xtc-rings` counted
  as `unreaped=35/29`. I inferred them from the trace (submitted, never subsequently reaped)
  rather than reading the ring. **`xtc-cqes` is exactly what closes that gap** and I have not yet
  run it — these traces predate it and I no longer have the frozen processes. Next hang, I will
  run `xtc-cqes 569` / `xtc-cqes 581` and join `user_data`→`task` directly against `PARK_TASK`,
  which turns this from a strong inference into a fact.
* **Not claimed:** that loop 0's drain-side miss is the *cause* of the hang. The fdatasync strands
  are homed on the worker loops, and those show the scheduler-side shape. Loop 0's miss may be a
  second, independent defect — or the mechanism by which the worker loops stop being nudged. I
  cannot separate those from here.

## Where that leaves it

Your framing was right that this needed the timestamp rather than reasoning, and the timestamp says
**you have two bugs, not one ambiguous one**:

1. a drain-side miss on loop 0 — `xtc_io_poll` returning `n_out == 0` with a completion already
   posted;
2. worker loops that stop entering their ring entirely after a point, with the fdatasync
   completion outstanding.

If (2) is downstream of (1) — e.g. loop 0 failing to reap a wakeup/eventfd completion means the
worker loops never get nudged — then one fix closes both, and the fact that loop 0 is the
supervisor loop makes that mechanism plausible. **I am flagging that as a hypothesis I have not
tested, not a conclusion**, because I have been wrong five times inferring past the data on this
bug and the cost each time was a wasted round trip on your side.

## Method
`xtc-tail.py` on two retained traces (16384 events each, spans 64.4 s and 63.2 s), joined
programmatically: SUBMIT→REAP by task pointer with `task=0x0` excluded, then idle `LOOP_POLL`
timestamps per `loop_id` compared against each surviving submit. Stuck-ring loop_ids taken from
three-sample `xtc-rings` output with `ovf=0` throughout. No hand-eyeballing.

## On `xtc-cqes` and its gate
The decode-to-task-pointer is precisely the join I asked for, and putting a **gate** on the
debugger scripts is the more valuable half — "unverified surface that would break silently on a
struct rename, discovered mid-hang with you waiting" describes a failure mode I have been exposed
to all week and had not thought to fix. I will run it on the next capture.

Twelve retractions, and this round both of mine were caught pre-send: the `task=0x0` population
error, and the `fd 560` mystery that was arithmetic all along.

## Environment
`multithreaded=on`, `pooled_protocol_carriers=0`, `io_method=xtc`, `fsync=on`,
`synchronous_commit=on`, `full_page_writes=on`, c6id.8xlarge (32 vCPU), XFS on local NVMe,
scale=50, `-c 32 -T 8`, dump 2 s after freeze. Traces `ov2.xtcl`/`ov4.xtcl` retained in-repo at
`.ec2/ovf-2026-09-11/`; happy to send them.
