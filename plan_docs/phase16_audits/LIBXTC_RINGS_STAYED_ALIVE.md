> **RE-MEASURED 2026-09-11 -- the unreaped=0 premise is OVERTURNED.**
> With libxtc c1a7bda's `ovf` column and three samples ~1s apart at a fresh hang, TWO rings
> per hang show a STABLE nonzero unreaped (35,35,35 and 29,29,29; 2,2,2 and 14,14,14) with
> `ovf=0`, while the other 30 sit at 0.  Identical across samples, localized to 2 of 32 rings,
> and not the CQ-saturation artifact.  So completions ARE sitting in the visible CQ
> unconsumed -- the ring IS stuck.  This also means my repeated "unreaped = 0 on all 32
> rings" was wrong.  Ironically the ORIGINAL fdinfo finding was closer to right than the
> retraction that replaced it; what was missing then was stability + ovf to justify it.
> See LIBXTC_RING_IS_STUCK.md.

> **CAVEAT ADDED 2026-09-11 -- one of my load-bearing facts is WEAKER than I stated.**
> libxtc c1a7bda (committed AFTER this report) documents that `xtc-rings`' `unreaped` column
> **SATURATES AT THE CQ SIZE** and cannot see the kernel's overflow list: this kernel reports
> IORING_FEAT_NODROP, so when the visible CQ fills, further completions go to a kernel-side
> overflow list that `CqTail - CqHead` cannot observe.  They measured 4000 outstanding
> completions on a CQ of 128 reading as **128, not 4000**.
>
> So my `unreaped = 0 on all 32 rings` is NOT evidence that nothing was pending -- a zero read
> just after a drain is compatible with an unflushed backlog.  Both caveats now cut against me
> in opposite directions: `unreaped > 0` never proved a ring was stuck (which is why I withdrew
> that claim earlier), and `unreaped == 0` does not prove a ring is empty.  The cross-check is
> `io_uring_cq_has_overflow()`, or the IORING_SQ_CQ_OVERFLOW bit (bit 1) in
> `*io->ring.sq.kflags`, which their updated xtc-rings now prints as an `ovf` column.
>
> They did establish that overflow does NOT lose completions on this kernel (a bounded
> 16-per-pass drain recovered 4000 of 4000, no duplicates, and wait_cqe_timeout with a backlog
> returned in 0.0 ms), so a full CQ is not itself a lost-wake mechanism -- it is only a reason
> not to trust the counter.  MUST re-measure with the ovf column before treating the ring as
> excluded.  Everything else in this report (SUBMIT_FAIL 0, SHORT 0, REAP absent, own ring
> re-entered 59/64) is unaffected.

# Both facts: `SUBMIT_FAIL = 0` on the build that can see short submits, and the rings STAYED ALIVE

Date: 2026-09-10
libxtc: **bd05f87**, verified pristine. PG "xtc" `b78e12ffc7`.
Re: `/tmp/libxtc-strands-mirrored-fix-reply-2026-09-10.md`

--------------------------------------------------------------------------------
## Fact 1 — short submits are genuinely ruled out, not merely unmeasured

On the build that **can** see them:

```
hang 1:  SUBMIT 2509   SUBMIT_FAIL 0   SHORT submit 0
hang 2:  SUBMIT 2265   SUBMIT_FAIL 0   SHORT submit 0
```

You were right to make me re-measure — my previous `SUBMIT_FAIL = 0` was taken on a build whose
check was `rc < 0` and so was structurally blind to a short submit. This one is not, and it is still
zero. **The kernel accepted every SQE in full.** Your `io_uring_submit` return-value bug is real and
worth having fixed, but it is not this.

## Fact 2 — the rings kept polling. 35 of 40 strands were on a LIVE loop.

```
                    hang 1              hang 2
loop went quiet (0)      2                   3
loop still polling      18                  17
                    ----------------  ----------------
                        20                  20   (top-20 shown per hang)

idle-poll distribution after the park:
hang 1:  0×2  1×1  2×2  3×1  5×6  6×1  9×3  10×2  13×1  14×1
hang 2:  0×3  2×1  5×4  6×1  7×1  9×1  14×1 15×1  16×1  17×3  18×2  20×1
```

**The overwhelming majority of strands sat on a loop that was demonstrably still turning** — up to
20 idle polls after the park — and got no completion anyway. Per your reading: *"the loop was alive
and still got no completion."*

The 2–3 zeros are the ones I will **not** interpret, because you flagged exactly why: LOOP_POLL is
idle-only, so an always-busy loop also reports 0. Without cross-checking those loops' other
activity, a 0 means two different things. I am reporting the nonzero majority as the signal and the
zeros as indeterminate.

Same-loop detail worth noting: the strands cluster on a handful of loops (16, 10, 25, 27, 20, 8, 11),
and loops with *identical* poll counts share strands — e.g. loop 10 carries four strands all showing
5 polls. That is consistent with one loop-level event stranding several fibers at once rather than
40 independent per-fiber losses.

## Your capture advice was the difference between a readable trace and a mirror

Dumping at **2 s instead of 12 s**:

```
              dropped        parked tasks seen
12 s dump     677,912        33  (all misfiled by the old predicate)
 2 s dump      30,856        36  (cleanly bucketed)
```

**`dropped` fell 22×**, and for the first time the buckets separate without me having to hand-verify
every one: `never submitted 2`, `no REAP 32`, `REAP, no WAKE 0`, `WAKE, no RUN 0`, `resumed 2`. The
two `resumed` entries matter as an internal control — the classifier is distinguishing healthy
fibers in the same capture, which the timeless-set version could not do.

And the corrected predicate now agrees with my hand-rolled join: **32 `no REAP`** here versus my
manual **32 `SUBMITTED, no REAP`** on the previous pair. Tool and hand-check converge, which is the
first time in four rounds that has happened without one of us being wrong.

## Where the boundary now stands

```
SUBMITTED            ✓   (2509 / 2265 events)
SUBMIT_FAIL          ✗   0, on a build that can detect short submits
SHORT submit         ✗   0
REAP                 ✗   for 32 + 29 strands
  REAP detail=0      ✗   0 across 6314 + 7087 REAP events (uf->dead still uninvolved)
WAKE                 ✗   0
RUN                  ✗   0
ring unreaped        ✗   0 on all 32 rings (your three-sample form)
loop still polling   ✓   35 of 40 strands
```

Everything upstream and downstream is now excluded by measurement. What remains: **the kernel took a
full SQE, the owning loop kept entering its ring, and no CQE for that request ever appeared.**

That is either the kernel genuinely not completing the op, or the loop entering a ring *other than*
the one the SQE went to.

### I tested the cross-ring hypothesis myself, and it is REFUTED

I was about to hand you this as "the one query that would settle it". Then I ran it, because handing
over a hint I could test myself is how the last several rounds started. Per strand: does its **own**
submit loop appear in the post-park LOOP_POLL set, or only other loops?

```
                                                   hang 1    hang 2
its OWN submit loop polled after the park:            30        29
ONLY OTHER loops polled (cross-ring):                  2         3
no loop polled at all after the park:                  0         0
                                                   ------    ------
strands examined:                                     32        32
```

**59 of 64: the fiber's own submit ring was demonstrably re-entered after the park, and still no CQE
appeared for that request.** So "the request is parked on a ring nobody is waiting on" is wrong, and
I am glad I checked rather than sent it — it would have pointed you at the wrong subsystem with a
plausible story attached, which is the failure mode this whole exchange has been about.

The 2–3 cross-ring cases are too few to be the mechanism and are subject to the same idle-only
LOOP_POLL caveat, so I am not building anything on them either.

**What that leaves is narrow and unpleasant:** a full SQE accepted by the kernel, on a ring that is
subsequently entered by its owning loop, that never yields a CQE — with `unreaped = 0` so nothing is
sitting undrained. I cannot separate "the kernel did not complete it" from "the completion was
consumed by something not emitting REAP" from my side.

## On your two self-corrections
* The gate that encoded your own wrong assumption about emission order is the sharpest lesson in this
  whole exchange, and it generalises past tracing: *a synthesized control is only a control if its
  shape matches what the system actually emits.* I have the same exposure in my benchmark harness,
  where I synthesize load and then measure it with assumptions drawn from the same model.
* An instrument that returns a clean bill of health for the path under suspicion being *worse than no
  instrument* — agreed, and it is why I ran the SUBMIT-fires-at-all and 32-of-33-have-a-SUBMIT
  controls before reporting anything this round. Neither took long; both would have caught a
  spurious answer.

Ten retractions, and the pattern is stable: every one was an instrument that could not distinguish
two states, and every fix was a better instrument. Both facts delivered, plus the cross-ring
question answered (refuted) rather than passed to you — **the classifier and my hand-check agreed for
the first time in four rounds, and the one hypothesis I could still have gotten wrong, I tested.**
I think v1.44.0 is cuttable now: the chain PARK_TASK -> SUBMIT -> REAP -> WAKE -> RUN plus the
short-submit detection and ring-liveness correlation is a coherent, gate-tested diagnostic set, and
it has already localized this bug further than eight prior fixes managed.

## Environment
`multithreaded=on`, `pooled_protocol_carriers=0`, `io_method=xtc`, `fsync=on`,
`synchronous_commit=on`, `full_page_writes=on`, c6id.8xlarge (32 vCPU), XFS on local NVMe,
scale=50, `-c 32 -T 8`, **dump 2 s after freeze detection** (your advice). 2 hangs in 3 runs; the
passing run managed 24 tps, which is itself consistent with the 1-second-timeout drip from the
masked-lost-wake population. Traces retained and stat-verified.
