> **SUPERSEDED 2026-09-07 (later same day)** -- the "submitted successfully, kernel never
> completed it => submit-side" conclusion here is ALSO wrong.  /proc/<pid>/fdinfo shows
> `cq_unreaped > 0`: the kernel DID post the completions and libxtc never drained those
> rings.  The `cmp` trace line only fires when libxtc reaps, so an unreaped CQE is
> indistinguishable from a never-posted one in that trace.  See
> LIBXTC_FDINFO_UNREAPED_CQES.md.  The per-tag counting method and the two
> guard-eliminations (0 drops, 0 batch-full) below still stand.

# CORRECTION + the real answer: exactly ONE submission never completed (my earlier MISSING_COUNT=0 was a tag-reuse artifact)

Date: 2026-09-07
libxtc: your HEAD **27d327b** + your tracing patch + two probes I added (see below).
Supersedes the "MISSING_COUNT=0 / loss is downstream of the reap" conclusion in
`/tmp/libxtc-cqe-probe-result-all-completed-2026-09-07.md`. **Please read this one instead.**

--------------------------------------------------------------------------------
## I got it wrong the first time, and here is why

I reported `comm -23` EMPTY => "every CQE arrived => the loss is downstream of the reap."

That was an artifact of the comparison, not the data. `comm -23` compares **unique tags**, and
the tag is the `xtc_aio_t *`, which lives **on the parked fiber's stack** -- so addresses are
**reused** across fibers and across successive ops on the same fiber. A later, completed op at
the same address masks an earlier missing one. Your patch traces by `a` only, so the join is
lossy by construction; I should have counted per tag rather than set-differenced.

Counting properly:

```
raw lines:      sub = 828      cmp = 827        <-- ONE missing
distinct tags:  sub =  53      cmp =  53        <-- which is why comm -23 was empty

per-tag counts where submits != completions:
  tag 0x7f1bb7fc4480:  submits=30  completions=29  MISSING=1
(all 52 other tags balance exactly)
```

And that tag is the **last `sub` line in the trace**, with submit return value **1**:

```
$ tail -1 /tmp/xtc-aio.sub
0x7f1bb7fc4480 1
```

`io_uring_submit` returned **1** -- one SQE submitted, no error, no partial, no negative.

## So the answer is your FIRST branch, not the second

> **No CQE ever arrived** -> the submission was accepted by `io_uring_submit` but the kernel
> never completed it for that fd/op. Then the bug is in what we submit or how, and the fix is
> on the submit side.

That is what the data says: **submitted successfully, never completed.** One op, for the fiber
that is stranded. I apologise for sending you the opposite conclusion earlier today -- please
discard that reply's headline; its `xtc-stranded`/task-field content still stands.

## Both of the guards you called "unreachable" are confirmed unreachable (in MY workload too)

I added two probes to test this empirically rather than take it on inspection, since your
measurement was on your repro and mine differs:

1. **The `got < max` drop** in the AIO reap branch (`else { __trace_aio("drop", ...) }`):
   **0 occurrences.** Your `got <= i < max` reasoning holds -- the loop is
   `for (i = 0; i < max; i++)` and `got` only ever increments inside it, so the guard can never
   be false. Not the bug.
2. **Batch-full with surplus CQEs left in the ring** (probe: after the drain, if `got >= max`
   and `io_uring_peek_cqe` still returns one): **0 occurrences.** So the reap is never
   truncating a burst and deferring completions to a later poll either.

Both eliminated with evidence, not argument.

## What this means and where I think it points

* `io_uring_submit` returned 1 for the op that never completed -- so this is not the
  discarded-return-value hazard you already instrumented (604 submits, 0 negative/zero). The
  submit *succeeded*.
* Nothing in libxtc dropped the completion after the fact: no `got>=max` drop, no batch-full
  deferral, and the reap trace fires unconditionally before any filtering (which is precisely
  why my first read looked clean).
* So the SQE was accepted and the kernel never produced a CQE for it. Candidates on the submit
  side, in the order I would look:
  - a **stale/closed fd** in the SQE (PG closes WAL segment fds on rotation; if a fiber submits
    `IORING_OP_FSYNC` on an fd that another fiber/thread closed between prep and submit, does
    io_uring report that as a CQE with an error, or can it be silently dropped for that ring?);
  - the SQE being **clobbered between prep and submit** (two fibers on the same ring preparing
    concurrently -- our fiber-per-session shape submits from many fibers on one loop's ring);
  - an op/flags combination the kernel accepts but never completes on this device/fs
    (XFS on EC2 local NVMe).
  I have no evidence to choose among these; that is your call. But the fact that it is
  **exactly one op, with a successful submit, on the stranded fiber**, is I think the tightest
  statement we have had yet.

## Reproduction detail so you can trust the counts
Single hung run, traces reset immediately before it (`rm -f /tmp/xtc-aio.*` per iteration), so
828/827 covers only that run. PG "xtc" HEAD, libxtc 27d327b + tracing, debugoptimized,
`multithreaded=on`, `pooled_protocol_carriers=0`, `fsync=on`, `synchronous_commit=on`,
c6id.8xlarge (32 vCPU), PGDATA on XFS / EC2 local NVMe, scale=50, `-c 32 -T 20`.

## Suggested patch change on your side (small, would make this unambiguous)
Trace `a->tag` (the task) alongside `a`, at both sites:

```c
__trace_aio2("sub", (const void *)a, (const void *)a->tag, (long)sr);
__trace_aio2("cmp", (const void *)a, (const void *)a->tag, (long)cqe->res);
```

With the task pointer in the trace I can join directly to the `xtc-stranded` suspects and tell
you *which* stranded fiber owns the missing op, plus its fd/op/offset if you add those. Happy
to apply whatever you send; the repro is ~2 minutes per attempt and hits ~50-60%.
