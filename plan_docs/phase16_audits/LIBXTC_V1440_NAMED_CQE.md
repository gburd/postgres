# v1.44.0: the orc fix WORKS (200/200 on our topology). The lost wake is now a NAMED CQE — closed by measurement.

Date: 2026-09-11
libxtc: **v1.44.0** (tag `2c6d4db`), checkout verified pristine.
PG "xtc" `b80d7304b6`. SUT: EC2 c6id.8xlarge, eu-west-1, XFS on local NVMe.

--------------------------------------------------------------------------------
## 1. The `xtc_orc` fix: verified on the exact topology that failed 100 % before

Standalone C, supervisor and child on the **same loop** — the deterministic-loss case:

```
RESULT same-loop atomic spawn_monitor, 200 trials:
  SIGNAL=200  NOPROC=0  CLEAN=0  EXIT=0  other=0  nodown=0
```

**200/200 `SIGNAL`, zero `NOPROC`.** Before the fix this configuration was 0 % correct. Your fix
is confirmed on the shape that matters to us, not just in the abstract.

I also confirmed you fixed **both** halves, and did it as defence in depth:
* `sup.c:188` — `xtc_proc_spawn_monitor(...)`, atomic, removes the cause;
* `sup.c:107` — `return reason != 0 && reason != XTC_DOWN_NOPROC;`, removes the misclassification
  even if some other path produces `NOPROC`;
* `sup.c:486` — the supervisor's own spawn correctly left as plain `xtc_proc_spawn`, with the
  reason stated (no parent proc to own the monitor).

I asked only for the atomic call. Your comment — *"rather than left to depend on the spawn path
being correct"* — is the better engineering and I am adopting the same reasoning in our classifier.

**Consequence for us:** this unblocks putting backend children under your supervisor. We had held
them on our own `xtc_proc_spawn_monitor()` path specifically because routing them through
`xtc_sup_add_child` would have silently lost our crash → fail-stop escalation.

Method note, in case it saves you a support round: my first three attempts to write this repro were
wrong in ways your test suite would have caught — `abort()` is not contained (only
SIGSEGV/SIGBUS/SIGFPE/SIGILL are), `xtc_proc_recovery_arm()` is setjmp-shaped and returns the
signal, and a faulter that *returns* after recovery exits `CLEAN`, not `SIGNAL`. Copying
`flt_early_faulter`'s pattern — no explicit arming, relying on `__proc_entry`'s auto-armed frame —
was what worked. Your tests are good documentation.

--------------------------------------------------------------------------------
## 2. The lost wake: `xtc-cqes` closed it. Here is the named completion.

The hang still reproduces on v1.44.0 (2 in 6 runs; passing runs 1215–2157 tps), which is expected
— nothing in v1.44.0 touches the drain path. But `xtc-cqes` turned my inference into a fact.

**In hang 2, loop 0's ring (`fd 560`, `unreaped=11`, `ovf=0`):**

```
[ 6] user_data=0x7fcc372b4481  res=0  flags=0x0   aio 0x7fcc372b4480  task=0x7fcc74391300  op=3
```

`op=3` is `XTC_AIO_FDATASYNC`. `res=0` means **the kernel completed it successfully.**

**And that exact task is a strand in the same capture:**

```
=== 34 parked task(s), classified by how far the wake got ===
  no REAP  32
--- no REAP ---
    pid=31.1.1   task=0x7fcc74391300   parked at 91780449 ns
```

**Same task pointer.** The fiber `31.1.1` is parked on an fdatasync whose successful completion is
sitting in a CQ nobody drained. That is the whole bug, named end to end, no inference left:

```
SUBMIT   -> the SQE was queued            (trace)
kernel   -> completed it, res=0           (xtc-cqes)
CQE      -> sitting in loop 0's visible CQ, ovf=0, unreaped   (xtc-cqes / xtc-rings)
REAP     -> never happened for that task  (trace: "no REAP")
WAKE     -> never dispatched              (trace)
fiber    -> parked forever                (xtc-stranded / strands)
```

Its earlier history in the same trace shows the healthy cycle for contrast — this task previously
did `SUBMIT -> PARK_TASK -> REAP -> WAKE` correctly at 119–287 µs, then later parked and never got
the fourth step.

### The detail I think matters most

**The stranded fdatasync CQE is in *loop 0's* ring, but the parked fiber is `31.1.1` — homed on
loop 31.** Note the trace's own SUBMIT lines for that task: `pid=0.0.0`, then later `pid=29.0.0`.
The submits are being made from service procs on *other* loops, and the completion landed on loop
0's ring.

That reframes my earlier cross-ring test, which you kindly told me had compared the right things:
loop↔ring is indeed 1:1, so my *test* was sound, but I was asking the wrong question. The issue is
not "did the fiber's own loop poll its own ring". It is that **the completion for a fiber homed on
loop 31 is queued on loop 0's ring**, and loop 0 — your supervisor/service loop — is the one with
the persistent `unreaped` backlog across every hang I have captured (this is the sixth).

I am stating that as the observation, not the mechanism. But it is consistent with everything else:
loop 0 accumulates undrained completions; whoever is parked waiting on one of them never wakes.

## 3. What I have stopped claiming

`fd 560` is not a mystery and I retract the framing I gave it: `ring_fd = 560 + 3*loop_id`
exactly, so "fd 560 recurs" simply means **loop 0 recurs** — which is now the actual finding rather
than a curiosity.

## The narrowest remaining question

Why does an aio completion for a fiber homed on loop 31 end up on loop 0's ring, and who is
supposed to drain it? If the submit is issued from a service proc on loop 0 (the `pid=0.0.0`
SUBMIT), then loop 0 owns the completion and must reap it — and it is not doing so. That is a
drain-side question on loop 0 specifically, which is where my timestamp analysis also landed
(loop 0 had idle polls *after* an unreaped submit; the worker loops did not).

Everything else is excluded by measurement: `SUBMIT_FAIL 0`, short-submit 0, `ovf=0`,
`REAP detail=0` count 0, and now a named successful CQE with a named parked task.

## Environment
`multithreaded=on`, `pooled_protocol_carriers=0`, `io_method=xtc`, `fsync=on`,
`synchronous_commit=on`, `full_page_writes=on`, 32 vCPU, scale=50, `-c 32 -T 8`, dump 2 s after
freeze. `dropped=53301` on that capture, so absence claims are hedged accordingly — but the
**presence** of the CQE and the strand are both positive evidence and unaffected.
Traces retained.
