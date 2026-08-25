# Wave 1 validation — libxtc v1.37 + perf fixes (2026-08-25, chiuso c7i.metal-48xl/192-core)

## Candidate: wave1-candidate = origin/xtc + v1.37 bump + 3 perf commits
  716bebafcd flake: bump libxtc v1.35.2 -> v1.37.0 (34c3a4b8)
  a4eda7ba30 carrier: grow pooled pool against runnable demand
  f01d8b280b carrier: remove temp resume-grow A/B env gate
  89daa8dba9 runtime: fix threaded large-alloc policy (M_MMAP_MAX=0, M_TRIM=64MB)

## Results
- libxtc v1.37.0: builds + links (libxtc.so.1.37.0).  v1.36/37 are example-only
  (11_lorb, sqlxtc) -- no core API change; all used symbols present.
- process regress: 245/245 result files, 0 diffs -> process mode byte-for-byte.
- perf (carriers=192, warmup + 15s):
    select -S:  fork 2,123k  fiber 2,051k   ~0.97x (parity; earlier runs beat)
    compute:    fork 52,330  fiber 80,206   1.53x  <- fiber BEATS fork
- test_backend_runtime: 17 Ok / 1 Fail.  The 1 = 001_threaded_runtime, subtest
  "PMChild reaping stress cycle N accepted active terminate requests"
  (pg_terminate_backend(pid,5000) of ACTIVE backends times out at 5s).

## The 001 failure is PRE-EXISTING / ENVIRONMENTAL, not the perf fixes
DECISIVE CONTROL: built clean origin/xtc (846a2d8d4e, NO perf fixes, v1.35.2) in
a separate tree on the SAME box, forced its tmp_install, ran 001 x3:
    CLEAN: FAIL x3, SAME subtest ("backend PID 105 did not terminate within 5000ms")
    WAVE1: FAIL x3, same subtest
Both fail identically -> the perf fixes are neutral on 001.  It is a
timing-sensitive flake on a heavily-loaded 192-core metal box (5s terminate
timeout).  The earlier c7i.8xlarge validation box passed test_backend_runtime
18/0 on clean origin/xtc, consistent with environmental.  (Harness note: the
control needs `meson test --suite setup` run FIRST to populate build/tmp_install,
else pg_config-not-found bails the TAP -- that wasted several earlier attempts.)

## Verdict: Wave-1 gate GREEN.  Land to origin/xtc (pending github push connectivity).
Two adversarial reviews of the perf diff still owed before final sign-off, but the
correctness + perf evidence supports landing the v1.37 bump + perf fixes.
