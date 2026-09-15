# The write wedge is FIBER-SPECIFIC (fork handles the same workload fine) and is NOT a deadlock

Date: 2026-09-15. Two controls run back-to-back on the same box, which together eliminate the two most
plausible innocent explanations.

## Control 1: process mode does not wedge -- so it is not contention
Identical workload (`pgbench -c 64 -j 8`, scale 50, `shared_buffers=1GB`, `fsync=off`), same machine,
minutes apart:

| mode | throughput | zero-tps intervals |
|---|---|---|
| **process (fork)** | **3,060 tps sustained** (2823 / 3288 / 3038 across the run) | **0** |
| **fiber** (`multithreaded=on`, `pooled_protocol_carriers=0`) | 791 tps for 15s, then **0.0 forever** | wedges |

pgbench's default script updates `pgbench_branches` (50 rows) and `pgbench_tellers` from 64 clients, so
row contention is genuinely high -- but fork sustains 3,060 tps through exactly that contention with no
stall. The wedge is therefore **specific to the fiber execution model**, not an artifact of the workload.
This also gives us the first honest fork-vs-fiber write number on a quiet-ish box: fiber peaks at ~26%
of fork before collapsing.

## Control 2: the deadlock detector works on the fiber path -- so it is not an undetected deadlock
The wedged state looks like a closed wait cycle: every active client backend is blocked
(`pg_blocking_pids()` non-empty for all of them, so the "blocked by nobody" query returns EMPTY), waits
are `tuple` 55 / `ClientRead` 36 / `transactionid` 8, four waiters are queued on transaction IDs, every
transactionid-lock HOLDER is itself blocked (pids 10-14 all `active` on `tuple`/`transactionid`), and the
log contains **zero** `deadlock detected`.

But a deliberate two-session deadlock on the fiber path IS detected correctly:
`begin; update dl where id=1; ...; update dl where id=2` racing the reverse order produced
`ERROR: deadlock detected` and exactly one `deadlock detected` in the server log.

So `CheckDeadLock()` runs and works under `multithreaded=on`. The wedge is not the deadlock detector
failing to fire, and it is not a real lock cycle that we are failing to break -- it is something that
*looks* like a cycle because every participant is blocked, without being one.

## What that leaves
The waiters are all legitimate lock waits whose holders are themselves legitimate lock waiters -- i.e. a
long dependency chain, not a cycle. For the chain to never drain, the transaction at the *head* must
never commit. Combined with the earlier evidence that:
- a backend sat `active` on `WALWrite` executing `END;` while holding `RowExclusiveLock` (an earlier
  capture), and
- buffer descriptors showed `HAS_WAITERS=1`, `WAKE_IN_PROGRESS=0`, lock bits still **held**,

the remaining hypothesis is that **one commit never completes** -- a fiber that entered
`RecordTransactionCommit` / `XLogFlush` and never returned -- so its locks are never released and
everything behind it queues forever. That is consistent with all observations and with fork being fine
(fork's commit path does not park a fiber).

Next probe: catch a backend in `END;`/commit and get its stack *at that instant* (the earlier capture
found the spinner was a `XactLockTableWait` *waiter*, not the commit holder -- I need the holder). The
useful discriminator is whether the committing fiber is parked in `xtc_aio_fdatasync` / `XLogFlush`'s
`WALWriteLock` wait, or spinning, or simply never resumed.

## Methodology notes for whoever continues
- `pg_blocking_pids()` returning empty for EVERY active backend does NOT mean "no blockers"; it means the
  chain root is not itself an active client backend (e.g. it is committing, or it is an aux process).
  Query for granted transactionid-lock holders instead.
- Always run the process-mode control on the same box in the same session. The fork number above
  (3,060 tps) is what makes the fiber result interpretable; without it, 791-then-zero could plausibly
  have been "the workload is just brutally contended".
- `chr()` concatenation is needed for string literals in these `psql -c` probes because the harness
  mangles quotes; `select ... where relname = (select chr(112)||...)` works.
