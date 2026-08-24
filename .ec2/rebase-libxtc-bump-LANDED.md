# Rebase onto master + libxtc v1.35.2 -- VALIDATED + LANDED (2026-08-24)

origin/xtc = 4a29ac44c0 (was 8a42285eec), rebased onto origin/master + libxtc
v1.35.2 (586db118).  Force-pushed after full EC2 validation on chiuso.

## AWS profile change
The `beef` profile vanished mid-session; the new sanctioned profile is `chiuso`
(account 187887018457, default region us-east-2).  All EC2 work now uses
--profile chiuso.  OTHER owners' keys in chiuso to NEVER touch: agent-sandbox-ec2,
flux-bench-key, libxtc-test2, chiuso-key.  (The old beef key xtc-rbv-20260824-070848
+ maybe-SG are stranded in the now-inaccessible beef account; can't clean without beef.)

## Validation (chiuso, us-east-1, c7i.8xlarge, cassert)
- libxtc v1.35.2 autotools build: OK (libxtc.so.1.35.2, USE_XTC_CARRIER=1)
- process regress (full main): 245 result files, 0 regression.diffs -> PASS
- threaded test_backend_runtime: 18 Ok / 0 Fail
- threaded smoke: mt=on, 8 carriers, select 40+2=42, pgbench -S c32 = 170,317 tps
  0 failed tx, FAST_STOP rc=0, 0 leftover procs
- box terminated + SG/key/pem deleted + 5-region sweep clean (no leaks)

## 3 rebase-resolution bugs caught by the gate (each a fixup commit on the candidate)
1. datachecksum_state.c: 4 new upstream abort_requested uses -> DataChecksumsAbortRequested
   (xtc's per-backend macro).  (2faf484009)
2. worker.c: in_remote_transaction is a per-backend #define, not a global; my
   resolution wrongly re-added the global -> statement-expr in decl context. (2feac135ed)
3. plancache.c: upstream ffca23839c (PlanCacheRoleCallback) reads cached_db_hash,
   which xtc made per-backend; use PgCurrentUserIdentityState()->cached_db_hash +
   drop the stale acl.h extern. (4a29ac44c0)

## Next
- Fiber LWLock livelock (BufferMapping/WALInsert, intermittent at c>=64) -- the real
  throughput blocker; now on the rebased base.
- Fix A (accept-time SSLRequest) + supervisor-reroute are on the pre-rebase line;
  re-apply/verify they're in the rebased tree before landing through the 2-review gate.
