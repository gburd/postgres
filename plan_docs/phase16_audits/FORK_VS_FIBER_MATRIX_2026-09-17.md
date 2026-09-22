# Fork vs fiber matrix, 32-core EC2, separate driver -- historical short runs

> **EVIDENCE CORRECTION -- 2026-09-22.** The throughput rows are retained as
> reported September 17 measurements, not revalidated results. The PG-side
> root-cause attribution, common-cause/loop-count claims, and crash exclusion
> were unsupported and are withdrawn. See
> [the corrected wedge report](WRITE_WEDGE_ROOTCAUSE_XTC_PROC_WAIT_FD_EINTERNAL_2026-09-17.md).
> Single 30s/45s points without recorded repetitions, uncertainty, or a complete
> matched-state/per-client progress record do not establish general parity,
> scalability, or correctness. No new EC2 results are claimed here.

Date: 2026-09-17. SUT: AWS c6id.8xlarge (32 vCPU, 61 GB, 1.7 TB local NVMe/XFS), Debian 13, libxtc
v1.48.1 built `--with-io-backend=uring` (verified: configure "L1 I/O backend... uring", links
liburing.so.2, 1152 io_uring ring fds at runtime). Driver: SEPARATE c6i.2xlarge over the private VPC
network. Reported common settings: shared_buffers=52GB, max_connections=600, fsync=on,
wal_level=replica, max_wal_size=32GB, scale-200 pgbench. Fork = process mode; fiber = multithreaded=on,
pooled_protocol_carriers=0. Built from a committed git archive of origin/xtc (post the two EC2-blocker
fixes: dead-end-backend-as-fiber and the nofile startup warning), md5-verified.

## READ-ONLY (pgbench -S), 30s per point -- reported measurements

| clients | fork tps | fiber tps | fiber/fork |
|--------:|---------:|----------:|-----------:|
|      16 |   36,299 |    35,241 |   **0.97x** |
|      32 |   71,143 |    69,860 |   **0.98x** |
|      64 |  133,470 |   131,536 |   **0.99x** |
|     128 |  253,775 |   237,229 |   **0.93x** |
|     256 |  390,475 |   320,785 |   **0.82x** |

In these short runs, the reported fiber/fork ratios are 0.97-0.98 at c<=32
and 0.82 at c=256. The separate driver removes one known confound, but these
points alone do not establish repeatable parity or explain earlier results.

## WRITE (default TPC-B-ish), 45s per point

| clients | fork tps | fiber tps | note |
|--------:|---------:|----------:|------|
|      16 |    5,252 |     3,553 | fiber 0.68x, no stalls |
|      32 |    9,896 |    (wedged) | fiber WEDGED: WALWrite=25 waiters, 0 tps |
|      64 |   18,675 |    -- | not reached |
|     128 |   33,724 |    -- | not reached |
|     256 |   45,393 |    -- | not reached |

Fork completed the recorded write points; fiber completed c=16 at 0.68x and
stopped making progress at c=32. The reported WALWrite-dominated wait shape
is an observation, not a PG-side attribution or proof of the same cause as
earlier `tuple`/`BufferExclusive` stalls.

## What the record supports

- This matrix does not meet the performance goal: fiber did not finish the
  write sweep. The read ratios remain preliminary measurements.
- Startup, scale-200 load, the read sweep and write c=16 reportedly completed
  without the earlier ENOSYS connection-refusal storm. This is not a full
  validation of the startup fixes.
- Preserve c=32 on the 32-loop SUT as the recorded reproducer. No controlled
  sweep established an eight-loop impossibility or minimum concurrency.
- Reported server responsiveness supports a live stall at the sampled times.
  Absence of PANIC does not prove absence of faults, and the record does not
  establish that shutdown was unrelated.

## Methodology notes (kept)
- Separate driver over private VPC net: the SUT ran only the server, so its 32 cores were not shared
  with pgbench -- essential for the read numbers to mean anything.
- pg_hba needed `host all all 172.31.0.0/16 trust` for the driver; SG needed intra-group 5432.
- Driver pgbench was 17.11 vs server 20devel. Successful protocol use does not
  prove identical script or measurement behavior; use a matched-version driver
  for the next comparison.
- Exact build hashes and effective configuration should accompany each new
  row. The archive description above is not a complete per-run provenance
  record; no claim is made that fresh equivalent data was verified per point.

## Next
The write wedge and the other recorded threaded correctness failures require
separate validation before performance conclusions. A libxtc report of zero
suspects does not exclude registration failures or establish PG-only ownership.
The corrected wedge report records relevant v1.49.2 source changes, but whether
that release fixes this run's failure is untested. No new probes or error
policy are introduced here. EC2 validation remains pending lava credential
repair (`InvalidClientTokenId`); do not treat local source/compile checks as a
replacement for controlled old/new runs, process/threaded gates, and repeated
fresh-state measurements with per-client progress.
