# `--strands` has the mirrored bug; corrected: 61/65 are SUBMITTED-and-never-REAPED, `SUBMIT_FAIL` = 0

Date: 2026-09-10
libxtc: **b53d444**, verified pristine. PG "xtc" `8d067f2105`.
Re: `/tmp/libxtc-strands-fix-submit-reply-2026-09-10.md`

--------------------------------------------------------------------------------
## The answer

Fresh captures on b53d444 (the old traces could not answer this — see below):

```
                        hang 1        hang 2
never submitted              1             2
SUBMITTED, no REAP          32            29     <-- the answer
REAP, no WAKE                0             0
WAKE, no RUN                 0             0
resumed                      0             1
SUBMIT_FAIL events           0             0
```

**61 of 65 strands: the SQE was submitted, the kernel accepted it, and no completion ever came
back.** Per your table, with `SUBMIT_FAIL = 0`: *"the kernel took it and nothing came back. Ring or
wait."*

Your `io_uring_submit` return-value gap is real and worth having fixed regardless — but **it is not
this bug**. Zero `SUBMIT_FAIL` across both hangs, and 61 strands with a successful preceding SUBMIT.

--------------------------------------------------------------------------------
## But `--strands` said `never submitted 33/33`, and that is the same bug mirrored

The tool's output was:

```
=== 33 parked task(s) ===
  never submitted  33      <-- would have been my headline
  no REAP           0
```

I did not send that, because a blanket 33/33 in a brand-new bucket is the shape of an artifact, not
a finding. Two controls:

**Control 1 — does SUBMIT fire at all in this build?** Yes:
```
hang 1: SUBMIT 1936    hang 2: SUBMIT 2216
```
So not a blanket "event never emitted" failure.

**Control 2 — do the parked tasks actually have SUBMITs?**
```
                                             hang 1     hang 2
parked tasks with ANY earlier SUBMIT:         32 / 33    30 / 32
distinct tasks in SUBMIT:                        33         31
distinct tasks in PARK_TASK:                     33         32
overlap:                                         32         30
```

**32 of 33 parked tasks demonstrably have a SUBMIT** — while the tool filed all 33 as
"never submitted". The join works; the bucket predicate is wrong.

### The defect

```python
elif any(ts >= pk.ts for ts in subm_at.get(task, ())):
    buckets["no REAP"].append((pk, task))
else:
    buckets["never submitted"].append((pk, task))
```

**A SUBMIT is issued *before* the fiber parks**, so its timestamp is always **less than** `pk.ts`.
The `ts >= pk.ts` test can therefore essentially never match, and every genuinely-submitted request
falls through to `never submitted`.

This is precisely the time-direction error we just fixed, **mirrored**: WAKE/REAP/RUN come *after*
the park and needed `ts >`; SUBMIT comes *before* it and needs `ts <`. The fix applied `ts >` to the
first three correctly and then reused the same direction for a predicate that runs the other way.

**Fix:** `elif any(ts < pk.ts for ts in subm_at.get(task, ()))` — and I would suggest tightening it
to *the most recent* SUBMIT before `pk.ts` rather than *any*, since a task pointer is reused across
cycles and "any earlier submit" will match an old one. With `ts <` on these two traces: **32 and 29
`SUBMITTED, no REAP`**, and the handful of genuine `never submitted` cases survive as a small
residue rather than swallowing everything.

Your own gate would not have caught this for the same reason your planted-bug gate missed the last
one: `test_xtc_tail_strands.sh` synthesizes fibers stranding at five depths, but if its SUBMIT
records are emitted at-or-after the park timestamp, the buggy predicate matches and the bucket looks
right. **The discriminating input is a SUBMIT that is strictly earlier than its park** — which is
what real traces always contain.

## Why I could not use `hang1/hang2` from last round

You asked me to run `--strands` on the traces I had saved. I checked first: they were captured on
**b41a548, which predates SUBMIT (55c9d5f)**, so their kind histogram is
`LOOP_POLL/PARK/PARK_TASK/REAP/RUN/SPAWN/WAKE` — **no SUBMIT at all**. Running the new `--strands`
on them reports `never submitted 30`, which is a false positive *by construction*.

So the "never submitted" reading had **two independent ways** to be spuriously produced — a trace
that predates the event, and the inverted predicate. I hit both. Fresh captures on b53d444 plus the
two controls above are what separate them.

## Confirmations
* **`REAP detail=0` count: 0** across 7014 and 6848 REAP events — again no consumed-and-discarded
  completion at either deliberate-discard site. Consistent with the previous round; the `uf->dead`
  path stays uninvolved.
* Your `--strands` time-blindness fix works correctly for WAKE/REAP/RUN: on these traces those
  buckets are cleanly 0, and one fiber in hang 2 is correctly classified `resumed`, which the old
  timeless-set version could not have distinguished.
* `dropped` is 677912 and 485751 here, so every bucket remains an absence claim — which is exactly
  why I ran controls instead of trusting a count.

## Where this leaves the boundary
`SUBMITTED` ✓ → `SUBMIT_FAIL` ✗ → `REAP` ✗ → `WAKE` ✗ → `RUN` ✗, with `unreaped = 0` on every ring
in your three-sample form. The kernel accepted the SQE and the completion is neither in the ring nor
out of it. That is **ring or wait**, on your side, and it is now the only surviving branch.

The obvious next question, if you want one more instrument: does the fiber's ring ever get **entered**
after that submit — i.e. an event for `io_uring_enter`/the wait call itself, keyed by ring rather
than task. If a ring is submitted-to but never entered, that is a missing-wakeup on the submit side
rather than anything about completions.

## Your CI-flake note
Recorded, and the two details are the useful part: retrying the *whole handshake* rather than a
throwaway probe socket (the probe races accept), and a stale binary that `make` would not relink
changing the failure rate mid-investigation. I have hit the second one in this repo too — my own
`| tail -N` build pipelines hid failures the same way, which is why I now verify the binary exists
rather than trusting the log.

Nine retractions between us. Both of my last two rounds began with a tool telling me something
confident and wrong, and in both cases the thing that saved it was a control that could distinguish
two states. That is the whole method at this point.

## Environment
`multithreaded=on`, `pooled_protocol_carriers=0`, `io_method=xtc`, `fsync=on`,
`synchronous_commit=on`, `full_page_writes=on`, c6id.8xlarge (32 vCPU), XFS on local NVMe,
scale=50, `-c 32 -T 8`, dump 12 s after freeze. **2 hangs in 9 runs** (~22 %, lower than previous
rounds); passing runs 692–1929 tps. Traces retained and verified locally this time.
