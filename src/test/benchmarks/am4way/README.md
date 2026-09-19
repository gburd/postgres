# [DO NOT MERGE] Three-way table-AM benchmark: HEAP vs FLUX vs RECNO

This directory is a development benchmark, NOT for upstream. It compares the
three table access methods in this tree on a single-connection OLTP micro-workload
(200k-row load, index build, 100k-row update, indexed-column update, range select,
and WAL volume per 100k updates).

## Honest capability matrix (as of this commit)

| AM    | bulk load | index build | update | indexed-col UPDATE | full OLTP bench |
|-------|-----------|-------------|--------|--------------------|-----------------|
| heap  | yes       | yes         | yes    | new-TID (native)   | YES (baseline)  |
| flux  | yes       | yes         | yes    | in-place (stable TID) | YES          |
| recno | yes       | yes         | yes    | in-place (stable TID) | YES          |

recno is a *resurrected* AM with documented stubs (see its commit message):
recno's old-snapshot before-image read path and in-flight-crash undo re-drive are
incomplete. It passes CRUD/MVCC/rollback/crash functional tests but is not yet
benchmark-hardened; its numbers below are best-effort at the level it sustains.

## Representative result (16 vCPU, 30G RAM, PG -O2 cassert, fsync=off, single conn)

| metric                | heap    | flux     | recno    |
|-----------------------|---------|----------|----------|
| insert 200k rows      | 0.38 s  | 2.07 s   | 2.9 s    |
| build index           | 0.06 s  | 0.07 s   | 0.28 s   |
| update 100k rows      | 0.80 s  | 1.04 s   | 2.4 s    |
| indexed-col update 4k | 0.06 s  | 0.14 s   | 0.50 s   |
| WAL / 100k updates    | 32 MB   | 863 MB   | 1680 MB  |

### Reading the numbers
- The headline is **WAL amplification**: the in-place-MVCC AMs write far more WAL
  per update than heap because each in-place UPDATE durably logs the before-image
  to UNDO (heap does not). flux ~27x, recno ~52x heap on this workload. This is the
  intrinsic cost of durable in-place MVCC-with-UNDO, consistent with the earlier
  standalone FLUX-vs-heap WAL analysis. recno is heavier still (HLC + overflow +
  its own sLog).
- flux and recno complete the workload.
- heap remains far faster and far lighter on WAL for this single-connection load;
  the in-place AMs' value proposition (bloat control, in-place indexed UPDATE,
  stable TIDs) is not captured by this micro-benchmark and needs a bloat/space and
  concurrency study, which this harness does not yet do.

## Running
    src/test/benchmarks/am4way/bench4.sh      # writes ~/bench4_results.txt
The Python suite under suite/ (from the earlier RECNO benchmark work) is included
for reference and further extension (pgbench/HammerDB-style drivers); it was not
re-validated against all three AMs in this commit.

## Do not merge
Marked [DO NOT MERGE]. Development artifact for comparing the AMs during
bring-up; not upstream-quality and reports honest incompleteness for recno.
