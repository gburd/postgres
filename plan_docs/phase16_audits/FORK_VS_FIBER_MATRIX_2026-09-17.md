# Fork vs fiber matrix, 32-core EC2, separate driver -- the first real apples-to-apples numbers

Date: 2026-09-17. SUT: AWS c6id.8xlarge (32 vCPU, 61 GB, 1.7 TB local NVMe/XFS), Debian 13, libxtc
v1.48.1 built `--with-io-backend=uring` (verified: configure "L1 I/O backend... uring", links
liburing.so.2, 1152 io_uring ring fds at runtime). Driver: SEPARATE c6i.2xlarge over the private VPC
network. Config IDENTICAL for both models: shared_buffers=52GB, max_connections=600, fsync=on,
wal_level=replica, max_wal_size=32GB, scale-200 pgbench. Fork = process mode; fiber = multithreaded=on,
pooled_protocol_carriers=0. Built from a committed git archive of origin/xtc (post the two EC2-blocker
fixes: dead-end-backend-as-fiber and the nofile startup warning), md5-verified.

## READ-ONLY (pgbench -S), 30s per point -- the clean comparison

| clients | fork tps | fiber tps | fiber/fork |
|--------:|---------:|----------:|-----------:|
|      16 |   36,299 |    35,241 |   **0.97x** |
|      32 |   71,143 |    69,860 |   **0.98x** |
|      64 |  133,470 |   131,536 |   **0.99x** |
|     128 |  253,775 |   237,229 |   **0.93x** |
|     256 |  390,475 |   320,785 |   **0.82x** |

Fiber tracks fork within a few percent up to the core count (c<=32 essentially even), and the gap opens
to ~18% at c=256 (8x oversubscription). This is the FIRST time fiber has been measured at read parity to
core count on a quiet, properly-isolated box -- the earlier "0.53x-0.39x" and "plateau at 260k" numbers
were on contended dev boxes and/or the wrong scheduler framing. On read, at the shipped concurrency
range, fiber is competitive.

## WRITE (default TPC-B-ish), 45s per point

| clients | fork tps | fiber tps | note |
|--------:|---------:|----------:|------|
|      16 |    5,252 |     3,553 | fiber 0.68x, no stalls |
|      32 |    9,896 |    (wedged) | fiber WEDGED: WALWrite=25 waiters, 0 tps |
|      64 |   18,675 |    -- | not reached |
|     128 |   33,724 |    -- | not reached |
|     256 |   45,393 |    -- | not reached |

Fork writes scale cleanly to 45k tps with zero stalls at every point. Fiber completes c=16 at 0.68x but
**WEDGES at c=32** -- the residual PG-side write wedge, here dominated by `WALWrite` (25 waiters) rather
than the `tuple`/`BufferExclusive` shape seen on the 8-core box. Same bug, different pressure point at
32 cores.

## Verdict, stated plainly against the north-star bar
The bar is "fiber OUTRIGHT WINS on ALL performance tests by a significant margin." We are NOT there:
- READ: fiber is at ~parity to core count and LOSES by ~18% at high oversubscription. Competitive, not
  a win.
- WRITE: fiber wedges at c=32; fork wins by default because fiber does not complete.
So on this matrix fork is ahead. Reported honestly. The read parity is real progress (and the closest
fiber has come on clean hardware); the write path is gated entirely on the residual wedge.

## What this run also established (all NEW, all from getting to 32 cores)
1. The two EC2 blockers fixed this session (dead-end-backend-as-fiber, nofile warning) hold up: the
   server started, loaded scale 200, and ran the full read sweep + write c=16 without the ENOSYS
   connection-refusal storm that blocked the previous run.
2. The write wedge is `WALWrite`-dominated at 32 cores. On the 8-core box it presented as `tuple` /
   `BufferExclusive`. That is consistent with one root cause whose most-contended lock shifts with core
   count -- and it means the wedge repro is concurrency-sensitive, so the fix must be validated at >=32
   cores, not just locally.
3. It is a WEDGE, not a crash: 0 PANIC, the server stayed up and answered until an (unrelated) fast
   shutdown; the wedge is stuck waiters, not a fault.

## Methodology notes (kept)
- Separate driver over private VPC net: the SUT ran only the server, so its 32 cores were not shared
  with pgbench -- essential for the read numbers to mean anything.
- pg_hba needed `host all all 172.31.0.0/16 trust` for the driver; SG needed intra-group 5432.
- Driver pgbench is 17.11 vs server 20devel; the read/write default scripts are protocol-compatible, so
  this did not affect results, but a matched-version driver would remove the caveat for pipelined or
  new-syntax tests later.

## Next
The write wedge is the ONLY thing between "read-competitive" and a real write comparison. It is
PG-side, reproduces at c=32 on 32 cores with a WALWrite signature, libxtc reports 0 suspect (v1.48.1).
Instrument the WAL-flush/commit path at c=32 on a 32-core box -- the concurrency where it actually bites,
which is why the 8-core traces kept eliminating the wrong things.
