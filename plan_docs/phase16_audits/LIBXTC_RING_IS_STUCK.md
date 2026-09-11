# The ring IS stuck: a STABLE nonzero `unreaped` across three samples, `ovf = 0`

Date: 2026-09-11
libxtc: **c1a7bda** (the `ovf` column), verified pristine. PG "xtc" `6f8093fc62`.
Re: `/tmp/libxtc-strands-mirrored-fix-reply-2026-09-10.md` and your `xtc-rings` caveat commit.

--------------------------------------------------------------------------------
## Headline: I was wrong, and so was the "it is just a transient" reading

Your `c1a7bda` warned me that `unreaped == 0` does not mean empty. I re-measured to check that —
and found something else entirely. **`unreaped` is not zero. It is stably nonzero on exactly the
rings that matter.**

Three `xtc-rings` samples ~1 s apart, at a fresh hang:

```
HANG 1   32 rings parsed, 2 interesting
  ring_fd 569   unr=35,ovf=0,alive=2  ->  unr=35,ovf=0,alive=2  ->  unr=35,ovf=0,alive=2   STABLE
  ring_fd 560   unr=2, ovf=0,alive=4  ->  unr=2, ovf=0,alive=4  ->  unr=2, ovf=0,alive=4   STABLE

HANG 2   32 rings parsed, 2 interesting
  ring_fd 581   unr=29,ovf=0,alive=2  ->  unr=29,ovf=0,alive=2  ->  unr=29,ovf=0,alive=2   STABLE
  ring_fd 560   unr=14,ovf=0,alive=4  ->  unr=14,ovf=0,alive=4  ->  unr=14,ovf=0,alive=4   STABLE

ANY ring with ovf=1:  False  (both hangs, all samples)
```

**Identical values across all three samples.** The other 30 rings are 0/0 in every sample.

## Why this is not the artifact you correctly diagnosed before

You taught me — rightly — that a single `unreaped > 0` sample proves nothing, because a busy ring
normally has completions in flight; you measured one ring at 218 then 1 three seconds later. That
is exactly why I withdrew my earlier `cq_unreaped` claim, and the withdrawal was correct **for
that evidence**.

This is a different shape:

* **Stability.** 35 → 35 → 35 and 29 → 29 → 29. A busy ring's backlog *moves*. These do not.
* **Locality.** 2 of 32 rings, and only ever those 2. Not a global property of load.
* **`ovf = 0`.** So this is not the saturation case your commit warns about — the visible CQ is
  not full, there is no kernel overflow list hiding behind it, and the count is therefore
  trustworthy in the direction it is being read.
* **`alive` differs.** The stuck rings show `alive=2` (569, 581) while `fd 560` shows `alive=4`;
  the healthy rings show `alive=2` or `3`. I do not know what to make of that and am not building
  on it, but `fd 560` recurring in both hangs — as it did in three earlier hangs I reported — is
  the third independent time that specific fd has come up.

**So: completions ARE sitting in the visible CQ, unconsumed, indefinitely.** That is the state my
`no REAP` classification implied and that I then talked myself out of.

## This reconciles the whole chain

Everything else I measured now fits without contradiction:

| step | result | consistent with a stuck CQ? |
|---|---|---|
| `SUBMIT` | 2509 / 2265 events | yes — we asked |
| `SUBMIT_FAIL` / short submit | 0, on a build that can see both | yes — the kernel took it |
| `REAP` for the strands | absent (61/65 `no REAP`) | **yes — this is the same fact** |
| `REAP detail=0` | 0 across 13,401 events | yes — nothing discarded |
| `WAKE` for the strand's task | absent | yes — nothing to dispatch |
| own ring re-entered | 59/64 | **the tension** — see below |
| `unreaped` | **stably 35 / 29** | **the completion is sitting there** |

The one thing that does *not* trivially fit is that 59 of 64 strands had their own submit loop
emit an idle `LOOP_POLL` after the park. If the loop re-enters the ring and a CQE is sitting in
the visible CQ with no overflow, the drain should find it.

Which narrows it to something quite specific: **either the poll is entering the ring but not
draining it (a drain-side bound, a filter, or a wrong CQ head), or the "idle poll" is on the
loop's own ring while the SQE went to a different `io` owned by the same loop.** Note the table
prints `io` and `ring_fd` as separate columns, and I have been treating "loop" as equivalent to
"ring" throughout. If a loop can own more than one `io`, my cross-ring test was too coarse — it
compared loop ids, not `io`/`ring_fd`. **I flag that as a real possible flaw in my own prior
refutation rather than defending it.**

## What I am claiming, precisely

* **Established:** on 2 rings per hang, a nonzero `unreaped` that is *identical* across three
  samples ~1 s apart, with `ovf=0`, while 30 sibling rings sit at 0.
* **Established:** this is not the saturation artifact, because `ovf=0` and the count is far
  below any CQ size.
* **Not established:** that these specific CQEs belong to the stranded fibers. I have not joined
  the CQE contents to the strand task pointers — `xtc-rings` gives me a count, not the
  `user_data`. **That join is the missing link**, and it is the one thing that would make this
  airtight rather than strongly suggestive.
* **Withdrawn:** my "`unreaped = 0` on all 32 rings" statement, twice repeated. It was wrong on
  this build — and I should note it was *also* measured before your caveat existed, so I cannot
  cleanly separate "the earlier builds really were 0" from "I mis-read a saturating counter".
  Either way the current, better-instrumented measurement is the one to act on.

## The ask

**Can `xtc-rings` (or a new helper) dump the pending CQEs' `user_data` for a ring with
`unreaped > 0`?** If those `user_data` values are the task pointers of my stranded fibers, this
bug is closed: the kernel completed the op, the CQE is in the visible CQ, and the drain is not
taking it. That is one gdb walk of `ring.cq` from `CqHead` to `CqTail`.

Second, smaller: **can one loop own multiple `io`/rings?** If yes, my cross-ring refutation
compared the wrong granularity and needs redoing at `io`/`ring_fd` level.

## Method note
Captured 2 s after freeze detection per your advice, which is why the rings are readable at all;
`dropped` fell from ~678k to ~31k in the earlier round with the same change. Three samples per
hang, parsed programmatically rather than eyeballed, and reported the stability explicitly
because a single sample of this would have been worthless — as you established.

Eleven retractions. This one is mine and it cuts in the unusual direction: I retracted a *true*
finding because the instrument could not yet justify it, and it took your own caveat commit to
send me back to re-measure it properly.

## Environment
`multithreaded=on`, `pooled_protocol_carriers=0`, `io_method=xtc`, `fsync=on`,
`synchronous_commit=on`, `full_page_writes=on`, c6id.8xlarge (32 vCPU), XFS on local NVMe,
scale=50, `-c 32 -T 8`, dump 2 s after freeze. 2 hangs in 4 runs. Traces retained
(`ov2.xtcl`, `ov4.xtcl`) and stat-verified.
