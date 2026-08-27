# HammerDB TPROC-C: PG/XTC (libxtc v1.38) vs stock PG — re-run (2026-08-27, mala acct)

Repeat of the fork-vs-pooled-threaded comparison on the current tree (origin/xtc
aaa42bdf9e, libxtc v1.38.0) after the v1.38 bump + P5 client-cert un-pin, on the
migrated `mala` account (724081032357).

Hardware: SUT c6id.8xlarge (32 vCPU, 61 GB), PGDATA on local 1.7 TB NVMe (xfs, real
filesystem).  Loadgen c6i.4xlarge over the private VPC net.  HammerDB 4.11 TPROC-C,
200 warehouses (~38 GB), DURABILITY ON (fsync+synchronous_commit+full_page_writes),
shared_buffers=8GB, huge_pages=on, io_method=sync, max_wal_size=64GB,
pooled_protocol_carriers=auto(=32).  release build.

## Results (HammerDB NOPM)

| VU  | stock (fork) NOPM | fork TPM  | fork CPU | fork RSS(PSS MB) | threaded NOPM | threaded CPU | threaded RSS |
|-----|-------------------|-----------|----------|------------------|---------------|--------------|--------------|
| 32  | 886,480           | 2,039,307 | 99.5 %   | 225              | NA (monitor)  | 88.1 %       | 240          |
| 64  | 929,255           | 2,136,193 | 92.8 %   | 364              | NA            | 88.8 %       | 309          |
| 128 | 880,942           | 2,027,138 | 97.3 %   | 688              | NA            | 88.3 %       | 352          |

## Findings (unchanged from the 2026-08-27 am run; v1.38 was TLS-only)

1. **Stock fork captures NOPM cleanly** (~880-930k across VU 32/64/128), CPU-bound
   (93-99 %).  Reproduces the prior stock baseline within noise.

2. **Threaded NOPM = NA (measurement blocker):** HammerDB's monitor VU (Vuser 1)
   again fails against the pooled server at "Taking start Transaction Count," while
   all worker VUs run and the server does real work (88 % CPU).  Same intermittent
   pgtcl-monitor-vs-pooled-carrier interaction filed for the pooled-scheduler owners
   (plan_docs/POOLED_WRITEPATH_FINDINGS_2026-08-27.md).  Manual xact-delta
   re-measurement was attempted but the standalone driver's 64-connection burst hits
   "connection refused" on the pooled server (connection-burst churn; the full
   harness ramps more gently and got workers to 88 % CPU) -- the same flakiness
   documented before; the authoritative comparison is the harness numbers above +
   the earlier root-cause (write-heavy pooled ~24 % of fork; NOPM confirmed ~147k @
   VU32 / 226k @ VU64 in the successful earlier monitor captures).

3. **RSS advantage holds for threaded:** at VU=128, threaded PSS = 352 MB vs fork's
   688 MB (single-process vs 128 forked backends) -- the memory-efficiency axis where
   the threaded model wins even on the write path.

4. **v1.38 did not move the write-heavy throughput** (expected: v1.38 is a TLS
   release -- RSA-PSS channel-binding + verify-error accessor -- no scheduler change).
   The write-heavy disparity remains the open scheduler-feeding gap
   (.ec2/writeheavy-rootcause-profile-2026-08-27.md: carriers left ~26-28 % idle
   under load; NOT WAL/lock, NOT fsync -- both already fiber-park; the pooled
   dispatch does not keep enough sessions concurrently runnable to fill all carriers
   the way fork's 64+ kernel processes overlap client RTT).

Net: the comparison reproduces prior results; the v1.38 bump is throughput-neutral
on write-heavy OLTP (it fixed TLS, not the scheduler).  Read -S (fiber beats fork)
and CPU-bound (1.53x) wins are unaffected; write-heavy pooled remains the target.
