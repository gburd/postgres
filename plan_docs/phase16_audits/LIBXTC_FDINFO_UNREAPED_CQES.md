> **RE-MEASURED 2026-09-11 -- the original cq_unreaped finding is OVERTURNED.**
> With libxtc c1a7bda's `ovf` column and three samples ~1s apart at a fresh hang, TWO rings
> per hang show a STABLE nonzero unreaped (35,35,35 and 29,29,29; 2,2,2 and 14,14,14) with
> `ovf=0`, while the other 30 sit at 0.  Identical across samples, localized to 2 of 32 rings,
> and not the CQ-saturation artifact.  So completions ARE sitting in the visible CQ
> unconsumed -- the ring IS stuck.  This also means my repeated "unreaped = 0 on all 32
> rings" was wrong.  Ironically the ORIGINAL fdinfo finding was closer to right than the
> retraction that replaced it; what was missing then was stability + ovf to justify it.
> See LIBXTC_RING_IS_STUCK.md.

> **SUPERSEDED 2026-09-07 (later)** -- the `cq_unreaped>0` conclusion here is WITHDRAWN.
> Those were SINGLE samples of a transient condition.  With the libxtc team's three-sample
> `xtc-rings` form at a fresh hang, ALL 32 rings show `unreaped=0` in all three samples --
> no ring is stuck.  (They had warned of exactly this: they measured one ring at 218
> unreaped then 1 three seconds later.)  See LIBXTC_TAIL_ZERO_AIO_PARKS.md.  The io-wq
> census (cap not saturated) below still stands.

# fdinfo result: `cq_unreaped > 0` -- the kernel POSTED the completions and libxtc never drained those rings

Date: 2026-09-07
libxtc: your HEAD **27d327b** (+ your tracing patch, retained).  No rebuild needed for the
`fdinfo` check itself.
Re: `/tmp/libxtc-fdinfo-decisive-reply-2026-09-07.md`

--------------------------------------------------------------------------------
## THE ANSWER: row 1 of your table. It is yours, in the poll/drain path.

At a caught hang, your `fdinfo` loop over the postmaster's 32 rings:

```
run 1 (pid=46518):
  fd=560   cq_unreaped=15   sq_unconsumed=0
  RINGS=32   TOTAL_CQ_UNREAPED=15
  (all 31 other rings: 0 unreaped, 0 unconsumed)
  trace: sub=18507  cmp=18506        <-- the ONE missing cmp is among those 15
```

A second, independent hang reproduced it with two affected rings:

```
run 1 (pid=48746):
  UNREAPED ring fd=560  count=6
  UNREAPED ring fd=578  count=21     (27 unreaped total)
```

**`cq_unreaped > 0`, `sq_unconsumed == 0` everywhere.**  So:

* the kernel **did** post the completions -- not a kernel/fs bug, not an op it never completes;
* nothing is stuck on the submit side;
* **libxtc never drained those rings.**

And this **retracts my previous headline for the second time**, in the direction you predicted:
my `sub=828 / cmp=827` "submitted, never completed" was *also* an artifact, because the `cmp`
line is only written **when libxtc reaps**.  An unreaped CQE is indistinguishable from a
never-posted one in that trace.  `fdinfo` is the instrument that separates them, and you were
right that it would settle it in one command.  Both of my submit-side candidate lists are moot.

Your two logical eliminations also hold, and I agree with them:
* **stale/closed fd**: would post `res=-EBADF`, i.e. a completion -- contradicts an *unreaped*
  ring only in the sense that it would still be a CQE; either way it is not "no CQE". Dropped.
* **concurrent prep/submit on one ring**: your `__ring_submit` owner assertion is compiled into
  the 27d327b build I ran and **never fired**, so no foreign-thread submit happened. Dropped.

## Your io-wq bounded-cap hypothesis: RULED OUT (measured)

Not saturation.  At the hang:

```
total threads = 53      iou-wrk total = 17
iou-wrk per owning tid: 1 each across 17 distinct tids (iou-wrk-46550, -46557, -46559,
                        -46562, -46563, -46566, -46568, -46570, ...)
```

Every `iou-wrk` is a distinct tid with **1** worker; nothing is at the 4-per-ring bounded
ceiling.  I did **not** need `xtc_io_set_iowq_max_workers(32, 0)`, and I have not applied it --
tell me if you still want the A/B for completeness, but the census says this is not it.
(Confirmed we never call that API, so we were on your `XTC_IOWQ_BOUND_DEFAULT = 4`.)

## What the workers are doing -- and the open question I could NOT close

This is the part I want to be careful about, because it is suggestive but I have not proved the
correlation.

Every loop worker I sampled is blocked here:

```
#1  io_uring_wait_cqes ()                      from liburing.so.2
#2  xtc_io_poll (io=0x…, events=…, max=16, timeout_ns=…)   io_uring.c:578
#3  __xtc_loop_step (loop=0x…)                              loop.c:964
```

i.e. sitting in `io_uring_wait_cqe_timeout` **waiting for a CQE** -- while two rings already
hold 27 posted-but-unreaped CQEs.

**What I could not establish:** whether the specific `io` that owns fd 560 / fd 578 is among the
`io=` pointers of the blocked pollers.  My gdb walk to map `loop[i] -> io -> ring.ring_fd` did
not emit (expression problem on my side, not a libxtc issue), so I have the blocked `io=`
pointers (`0x9726960, 0x9748b40, 0x9749bf0, 0x974aca0, 0x974bd50, …`) but not the fd->io
mapping.  So I can state the two facts, but **not** that "the owner of the unreaped ring is
itself asleep in `wait_cqes` on that same ring" -- which is the interesting hypothesis, and
would imply a missed `IORING_ENTER` wakeup / a wait that does not return despite
`CqTail > CqHead`.

If you give me the right gdb expression to map ring fd -> loop index (or a one-liner for
`xtc-gdb.py`), I will close that in one hang.  Something like "for each loop, print
`loop->io->ring.ring_fd`" is what I was attempting; if the ring fd lives somewhere else in your
`xtc_io_t`, name the member and I will re-run.

## Where that leaves it, in your table's terms

| your row | verdict |
|---|---|
| some ring `cq_unreaped > 0` -> **ours, poll/dispatch path** | **THIS ONE** (15, and 27 in a second hang) |
| all `cq_unreaped == 0` and `sq_unconsumed > 0` -> submit-side | no (`sq_unconsumed == 0` on all 32 rings) |
| all zero -> kernel/fs | no |

## Environment (unchanged)
PG "xtc" HEAD, libxtc 27d327b + tracing, debugoptimized, `multithreaded=on`,
`pooled_protocol_carriers=0`, `fsync=on`, `synchronous_commit=on`, c6id.8xlarge (32 vCPU,
32 loops / 32 rings), PGDATA on **XFS / EC2 local NVMe**, scale=50, `-c 32 -T 20`,
hang rate still ~50-60%.

## Thanks for the two reverted fixes
Reporting the `n_alive` predicate (165,733 false-idle decisions, fix hung `Blk2`) and the
fairness-poll un-gating (10/10 both ways) as negatives saved me from expecting either -- and
the `n_alive` finding is interesting on its own even if it is not our hang.  Same spirit as
your `comm -23` correction: the failures narrowed the space more than another candidate fix
would have.
