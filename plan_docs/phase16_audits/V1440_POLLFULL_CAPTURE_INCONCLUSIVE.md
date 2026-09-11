# POLL_FULL capture on ce6a99d: inconclusive on THIS instance (init-time index-build failure)

Date: 2026-09-11. Instance: lava/us-east-2 c6id.8xlarge, PG b80d7304b6, libxtc ce6a99d.

## What I set out to do
Answer libxtc's two POLL_FULL questions (drains-too-slowly vs polls-and-skips) on a fresh
`ce6a99d` capture. Their `XTC_TAIL_POLL_FULL` (kind 13, budget max=8) plus the loop-0-vs-unreaped
timestamp would distinguish the two.

## What actually happened
The **first** run of this build on this box (`/tmp/v144.sh`, earlier) passed 4 benches and
reproduced 2 real hangs -- that produced the named-CQE result in LIBXTC_V1440_NAMED_CQE.md, which
stands. But every subsequent capture attempt on the SAME box hit a different failure: `pgbench -i`
dies at `alter table pgbench_tellers add primary key`, with:

```
pgbench: error: query failed: server closed the connection unexpectedly
  Query was: alter table pgbench_tellers add primary key (tid)
```

Server-side: the client backend running the ALTER exits `code=0` (clean), the postmaster then
logs "database system is shut down" with **no PANIC / FATAL / Assert / crash message**, and a
stray `backend>` single-user prompt appears in the log. So gdb attaches to nothing at capture
time ("ptrace: No such process"), and my hang-detector (no `tps`) mislabels an init failure as a
hang.

## Honest classification
- This is **not** the fsync lost-wake (that reproduces during the *measured* run and leaves a
  live hung server with a named unreaped CQE; this dies during *init* with the server gone).
- `ADD PRIMARY KEY` builds an index, which is the `create_index`/`REINDEX` family that the
  checkpointer/fiber buffer-lock ABBA deadlock (THREADED_TEST_BASELINE_2026-09.md bug #4) lives in
  -- so this is plausibly that bug firing on the fiber path during init. But I did NOT capture a
  backtrace (the process was gone), so I am **not** asserting that; it is a hypothesis.
- The clean `code=0` backend exit + silent postmaster shutdown is atypical for the ABBA deadlock
  (which wedges rather than exits cleanly), so it may be a third thing. Genuinely unresolved.

## Why I stopped rather than chase it
1. The two questions the user posed for v1.44.0 are **answered**: the orc fix works (200/200
   SIGNAL, LIBXTC_V1440_NAMED_CQE.md) and the lost wake is a named CQE (same doc). Neither needed
   this capture.
2. The POLL_FULL question is libxtc's to close and needs a clean hang capture, which this box
   would not produce. The ABBA agent owns the index-build failure family and is working it.
3. I spent several cycles in my own harness (a backgrounded pgbench + setsid + a too-broad
   `pkill -f "postgres -D /mnt/nvme"` that matches sibling datadirs) before isolating that the
   failure is init-side and product-side, not harness-side. Continuing to instrument my harness is
   not the best use of the box.

## For the next capture attempt
- Use a datadir whose path does NOT share a prefix another run's `pkill -f` would match.
- Detect init failure explicitly (check `pgbench -i` exit code and grep its log for "closed the
  connection") and label it INIT_FAIL, distinct from a measured-run hang -- do not infer "hang"
  from a missing `tps`.
- If `ADD PRIMARY KEY` reproducibly kills the fiber-path server during init, that is worth its own
  targeted repro for the ABBA agent: it is a smaller, faster trigger than the full check-threaded
  suite (one CREATE-INDEX-equivalent, no schedule).
- Then re-attempt POLL_FULL on a capture that actually hangs during the measured run, per
  libxtc's ce6a99d reply.
