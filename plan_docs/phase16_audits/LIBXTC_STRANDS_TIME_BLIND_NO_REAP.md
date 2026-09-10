# `--strands` has a time-blindness bug; corrected, all 65 strands are `no REAP`

Date: 2026-09-10
libxtc: **b41a548**, verified pristine. PG "xtc" `b3e456ff4e`.
Re: `/tmp/libxtc-reap-event-reply-2026-09-10.md`

--------------------------------------------------------------------------------
## The answer you asked for

**All 65 strands across 2 hangs are `no REAP`.** The completion never came back from the ring.

```
hang 1 (30 parked)          hang 2 (35 parked)
  no REAP        30           no REAP        35
  REAP, no WAKE   0           REAP, no WAKE   0
  WAKE, no RUN    0           WAKE, no RUN    0
  resumed         0           resumed         0
```

Per your table: **submission / ring / the wait itself**, not the reap-to-dispatch handoff.

But I have to walk you through how I got there, because **`--strands` as shipped reports the
opposite**, and the disagreement is a bug in the tool rather than in either of our conclusions.

--------------------------------------------------------------------------------
## What `--strands` printed, and why it is wrong

```
=== 30 parked task(s), classified by how far the wake got ===
  no REAP          2
  REAP, no WAKE    0
  WAKE, no RUN     28        <-- opposite of my previous finding
  resumed          0
```

That flatly contradicted my last report (no WAKE for any stranded task), so I traced one of the 28
by hand instead of accepting either answer. `pid=24.1.1`, which the tool filed under
"WAKE, no RUN":

```
  [   17]      6169 ns  RUN       pid=24.1.1   park->run ns=12869
  [   40]     24726 ns  PARK_TASK pid=24.1.1   task=0x174afba0
  [   41]     24821 ns  PARK      pid=24.1.1   fd/op=837
  [   50]     28363 ns  RUN       pid=24.1.1   park->run ns=3464
  ...  699 events for this pid, a long healthy park/resume history ...
  [15428]  36648150 ns  PARK_TASK pid=24.1.1   task=0x174afba0     <-- final
  [15429]  36648213 ns  PARK      pid=24.1.1   fd/op=822

  WAKE events for task 0x174afba0 ANYWHERE:                 207
  WAKE events for task 0x174afba0 AFTER index 15428:           0
```

**207 WAKEs, none of them after the strand.** They are all from the fiber's *healthy earlier
cycles*. The tool counted them anyway.

### The defect, in `cmd_strands`

```python
reaped = set(e.detail for e in events if e.kind_name == "REAP" and e.detail)
waked  = set(e.detail for e in events if e.kind_name == "WAKE")
...
if any(ts > pk.ts for ts in ran_after.get(pk.pid, ())):   # <-- time-aware
    buckets["resumed"].append(...)
elif task in waked:                                        # <-- time-BLIND
    buckets["WAKE, no RUN"].append(...)
elif task in reaped:                                       # <-- time-BLIND
    buckets["REAP, no WAKE"].append(...)
```

`reaped`/`waked` are **timeless sets**; membership is tested with no comparison against `pk.ts`,
while the `resumed` branch does exactly that comparison correctly. **A task pointer is stable for
the fiber's whole life** (`0x174afba0` appears in all 699 of its events), so *any* fiber with a
healthy history is guaranteed to be in `waked`, and the `WAKE, no RUN` branch swallows it before
the REAP branch is ever reached. The bug is self-concealing: the more normal work a fiber did
before stranding, the more confidently it is misclassified.

**Fix:** make the two `elif`s time-aware, exactly like the `resumed` branch already is —

```python
waked_at  = collections.defaultdict(list)   # task -> [ts]
reaped_at = collections.defaultdict(list)
for e in events:
    if   e.kind_name == "WAKE": waked_at[e.detail].append(e.ts)
    elif e.kind_name == "REAP" and e.detail: reaped_at[e.detail].append(e.ts)
...
elif any(ts > pk.ts for ts in waked_at.get(task, ())):  ...
elif any(ts > pk.ts for ts in reaped_at.get(task, ())): ...
```

With that change, on the same two traces: **30/30 and 35/35 `no REAP`, zero in every other
bucket.** That agrees with my hand-rolled join from the previous report and with `unreaped = 0`.

The irony is not lost on me: `--strands` exists because I hand-rolled a join and got a
"decisive-looking but structurally impossible answer." This time the hand-rolled join was right and
the tool was wrong — same class of error (a join that ignores a dimension of the data), opposite
direction. Neither of us should be trusted without a control, which is the actual lesson.

## Also: the retained window is not what `dropped` implies

Worth knowing when you read these captures. `xtc-tail-dropped` reported
`emitted=794367 dropped=777983`, so I expected the surviving 16384 records to be a narrow slice at
the end. They are not:

```
retained events per second-bucket:
  t= 0s : 15861      t= 1s :   97      t= 4s :  291
  t= 7s :    14      t=43s :   39      t=54s :   82
```

97 % of the retained window is the **first second**, yet it spans 0 → 54.4 s. I checked the ring is
a single global FIFO with a correct `start = seq % RING` snapshot and `CLOCK_MONOTONIC`
timestamps — so the explanation must be emission burstiness (a dense startup burst, then long
sparse stretches), not a ring or clock defect. I could not fully reconcile the arithmetic and am
flagging it rather than asserting a cause. **Practical consequence: `dropped` alone does not tell
you which time region survived**, and both my strands and their parks happen to sit inside the
retained region, which is why the corrected join is admissible here.

## Confirmations
* **REAP is emitting**: 7814 and 7678 events across the two hangs. `detail=0` count: **0** in both —
  so no completion was consumed-and-discarded at the two deliberate-discard sites during these runs.
* Your planted-bug validation (`REAP tagged=22 WAKE=17`, 2 `REAP,no WAKE`) is the right shape of
  gate, and it would have caught a real reap-side drop. It did not catch the tool's time-blindness
  because the planted test's fibers presumably have short histories — a fiber with 699 prior events
  is what exposes it.
* Your man-page correction, and recording the 85-cross-io-leak hypothesis that measured well then
  made things strictly worse (6/6 healthy → 0/6): both noted. That is the fifth measured-and-
  rejected hypothesis in this arc and it belongs in the record as much as the confirmed ones.

## Where this leaves it
`no REAP` for 65/65 strands, with `unreaped = 0` on every ring in your three-sample form. Those two
together say the CQE is **neither sitting in the ring nor coming out of it** — which points at
submission, or at the wait not being woken to look. That is your side of the boundary, and it is
the narrowest the space has been.

I have not yet checked whether the strands' fds were ever **submitted** — if you have or want a
submission-side event (an SQE queued, with its task/user_data), that would close the last
ambiguity between "never submitted" and "submitted, never completed".

## Environment
`multithreaded=on`, `pooled_protocol_carriers=0`, `io_method=xtc`, `fsync=on`,
`synchronous_commit=on`, `full_page_writes=on`, c6id.8xlarge (32 vCPU), XFS on local NVMe,
scale=50, `-c 32 -T 8`, dump 12 s after freeze. 2 hangs in 3 runs; the passing run did 1366 tps.
Traces `hang1.xtcl`/`hang2.xtcl` retained this time (I lost the previous pair to a premature
teardown — my error, and the reason this is a fresh capture rather than a re-analysis).
