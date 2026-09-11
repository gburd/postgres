---
name: xtc-runtime-debug
description: >
  Inspect and diagnose a running or hung threaded PostgreSQL (multithreaded=on, libxtc
  carriers) using the full libxtc tool set: the gdb/lldb helpers (xtc-stranded, xtc-rings,
  xtc-procs, xtc-loops, xtc-mailbox, xtc-trace, xtc-tail-dump, xtc-tail-dropped) and the
  xtc_tail event timeline (xtc-tail.py --strands/--summary/--wake-latency/--around).
  Use this for ANY threaded hang, stall, lost wake, stranded fiber, starvation, or
  "where did this fiber go" question. Triggers on: "threaded hang", "fiber stuck",
  "lost wake", "xtc-stranded", "xtc-tail", "--strands", "carrier wedged",
  "which loop", "no completion", "server stopped answering under multithreaded=on".
---

# Debugging the xtc threaded runtime

Hard-won rules first. Every one of these came from a wrong conclusion that cost a round trip.

## Rule 0: presence is evidence, absence is a claim that needs a control

The tail ring is **bounded (16384 records) and overwrites oldest-first**. Before you believe
that anything is *missing*, run:

```
(gdb) xtc-tail-dropped
emitted=794367  ring=16384  buffered=16384  dropped=777983
WARNING: ... Any claim that rests on an event being ABSENT is unfalsifiable for this capture.
```

With a nonzero `dropped`, an absence may just be eviction. Two ways to rescue an absence claim:

- **Eviction direction.** Eviction removes the OLDEST. If the thing you expect would be
  *newer* than your reference event, it would have been retained — so its absence is real.
  (WAKE/REAP/RUN follow a park: absence is checkable. SUBMIT *precedes* a park: absence is
  NOT checkable this way.)
- **Positive control.** Show that events of that kind *were* retained in the same region.

## Rule 1: capture EARLY, not late

Dumping 12 s into a hang lets idle `LOOP_POLL` fill the whole ring and evict every
`PARK_TASK`, yielding a capture that reads as "0 parked tasks". Measured:

| dump delay | dropped | parked tasks visible |
|---|---|---|
| 12 s | 677,912 | 33, all misclassified |
| **2 s** | **30,856** | **36, cleanly bucketed** |

**Dump ~2 s after detecting the freeze.** Near and narrow beats late and wide.

## Rule 2: capture BEFORE teardown

`pg_ctl -m immediate stop` at capture time floods `FATAL: terminating connection due to
administrator command` and fires the crash dump. It looks exactly like a crash. Capture, then
tear down.

## Rule 3: `unreaped` means neither what you hope nor what you fear

`xtc-rings`' `unreaped` is `CqTail - CqHead`, and it **saturates at the CQ size** and cannot
see the kernel's overflow list (this kernel reports `IORING_FEAT_NODROP`). Measured: 4000
outstanding completions on a CQ of 128 read as **128**.

- `unreaped > 0` does **not** prove a ring is stuck — that is the normal state of a busy ring.
- `unreaped == 0` does **not** prove a ring is empty.

Always read the **`ovf`** column beside it (`IORING_SQ_CQ_OVERFLOW`, bit 1 of
`*io->ring.sq.kflags`). `unreaped=0` with `ovf=1` means completions ARE pending and merely
invisible. And **take three samples ~1 s apart** — a single sample of a transient proves nothing.

## Rule 4: service fibers are not strands

`local_id == 0` fibers are long-lived supervisors resting in `xtc_recv(-1)`. They show as
`park='-'` with no matching RUN forever, and that is correct. `xtc-stranded` excludes them now,
but a hand-rolled query will not — filter `local_id >= 1`, and beware that a non-zero-local_id
long-lived receiver (e.g. `1.1.1`) is the same false positive.

## Rule 5: joins must respect time direction AND the key space

Two separate disasters live here.

**Key space:** `WAKE` zeroes its pid and carries the **task pointer** in `detail`
(dispatch has a task, not a proc). Joining WAKE to a park **by pid** returns zero matches
*by construction*. `PARK_TASK` (kind 9) is the join key; the plain `PARK`'s detail is
`fd/op`, not a task.

**Time direction:** relative to a park,
- `RUN`, `WAKE`, `REAP` happen **after** → require `ts > park_ts`
- `SUBMIT` happens **before** (the SQE is queued, then the fiber parks) → require `ts < park_ts`

Getting either backwards produces a confident, clean-looking, completely wrong bucket. Both
directions have shipped as bugs in the analysis tool. Since a task pointer is stable for a
fiber's whole life, a timeless `task in set` test matches the fiber's own healthy history —
**self-concealing: the more normal work a fiber did before stranding, the more confidently it
is misclassified.**

## The standard hang workflow

```bash
# 0. reproduce with the tail enabled (PG side: PG_XTC_TAIL=1 gates xtc_tail_enable)
PG_XTC_TAIL=1 postgres -D $PGDATA -c multithreaded=on \
    -c pooled_protocol_carriers=0 -c io_method=xtc -c fsync=on ...

# 1. detect the freeze, wait ~2 s, then capture EVERYTHING in one gdb batch
sudo timeout 90 gdb -p $SRVPID -batch \
  -ex "source /path/to/libxtc/tools/gdb/xtc-gdb.py" \
  -ex "xtc-tail-dropped" \
  -ex "xtc-tail-dump /tmp/hang.xtcl" \
  -ex "xtc-rings" \
  -ex "xtc-stranded" \
  -ex "xtc-procs" \
  -ex "xtc-loops" \
  -ex "xtc-mailbox" \
  -ex "thread apply all bt" > /tmp/hang_gdb.txt 2>&1

# 2. classify the strands -- this answers "how far did the wake get?"
python3 tools/xtc-tail.py /tmp/hang.xtcl --strands

# 3. read the rest of the timeline
python3 tools/xtc-tail.py /tmp/hang.xtcl --summary
python3 tools/xtc-tail.py /tmp/hang.xtcl --wake-latency
python3 tools/xtc-tail.py /tmp/hang.xtcl --pid 27.1.1        # one fiber's life
python3 tools/xtc-tail.py /tmp/hang.xtcl --around <ts>       # neighbourhood of an event
python3 tools/xtc-tail.py /tmp/hang.xtcl --kind PARK,RUN     # filter
```

## What each tool answers

| tool | question it answers |
|---|---|
| `xtc-stranded` | which procs are parked with no pending wake (filters `local_id 0`) |
| `xtc-rings` | per-ring `ring_fd`, `owner_tid`, `unreaped`, **`ovf`**, `alive` |
| `xtc-procs` / `xtc-proc` | proc table; **runnable-but-unscheduled vs genuinely parked** — a plain `bt` cannot tell these apart |
| `xtc-loops` | per-loop state, run-queue depth, steals |
| `xtc-mailbox` | mailbox depths + contents — use this on any hand-rolled mailbox protocol |
| `xtc-trace` | in-runtime trace buffer |
| `xtc-tail-dump` | serialize the event ring to `.xtcl` |
| `xtc-tail-dropped` | **the falsifiability gate** — run it before any absence claim |
| `--strands` | bucket every parked task: never submitted / no REAP / REAP no WAKE / WAKE no RUN / resumed |

`tools/lldb/xtc_lldb.py` is the macOS equivalent.

## Reading `--strands` output

The buckets map to subsystems, which is the point:

| bucket | meaning | where to look |
|---|---|---|
| `never submitted` | no SQE was queued | our submit path / the caller |
| `no REAP` | kernel took it, no completion came back | ring, or the wait |
| `REAP, no WAKE` | reaper consumed it, never handed on | reap→dispatch handoff |
| `WAKE, no RUN` | dispatch ran, loss is downstream | waker CAS / enqueue |
| `resumed` | healthy — **your in-capture control** |

If a bucket holds *everything*, suspect the tool or the trace before the runtime: check the
kind histogram first (`grep -oE "SCHED [A-Z_]+" | sort | uniq -c`). A trace captured by a
libxtc build older than an event **cannot contain that event**, and the classifier will
happily report the corresponding bucket at 100%.

## Interpreting a ~1.000000-second `park->run`

Not I/O latency — a **timeout**. If the preceding `PARK`'s `fd/op` is an fd (not an aio
opcode 0–5), the fiber woke on its **deadline**, meaning its readiness wake was lost and the
timeout rescued it. These are *masked* lost wakes, and they cost a full second each.

Method note: the ~1 s RUNs often sit early in the retained window, so their PARK predates the
trace and a **backward** scan finds nothing (`fd/op=none`, clean and wrong). Pair **forward**
from each PARK to that pid's next RUN.

## Also: `LOOP_POLL` is idle-only

It is emitted only when a poll dispatched nothing. So `0 idle polls` means *either* "the loop
went quiet" *or* "the loop was always busy". Read it against that loop's other activity, never
alone.

## Cheat sheet

- aio ops: `PREAD 0, PWRITE 1, FSYNC 2, FDATASYNC 3, PREADV 4, PWRITEV 5` (`xtc_io.h`)
- task states: `SCHEDULED 0, RUNNING 1, PARKED 2, DONE 3` (`loop_int.h`)
- wake revents: `READABLE 0x01, WRITABLE 0x02, HUP 0x04, ERR 0x08, WAKEUP 0x10, AIO 0x20`
- `/proc/<pid>/fdinfo/<ring_fd>` gives `CqTail-CqHead` and `SqTail-SqHead` with no rebuild —
  but it inherits the same saturation caveat as `unreaped`.
- Build libxtc `-Dbuildtype=debugoptimized`; a release build strips the debug info these
  helpers walk. Confirm with `file libxtc.so.*` → "with debug_info, not stripped".
