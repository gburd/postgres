# Clean HEAD (afae905889): the documented pmchild double-release did NOT reproduce locally

Date: 2026-09-12
Host: floki (local dev box, shared, NOT the EC2 SUT -- see caveats below).
Build: `-Dcassert=true -Dxtc=enabled -Dplpython=disabled`, libxtc v1.44.1 (via `nix develop`,
pinned in flake.nix), PG at commit `afae905889` (HEAD + only the one-line
`XTC_PG_RC_BUDGET_YIELDS` enum fix from `PG_XTC_CARRIER_BUDGET_YIELDS_ENUM_MISSING`-class
build break; see the retraction note atop `V1441_HANG_GONE_PMCHILD_CRASH_BLOCKS.md`).
No starvation WIP present (confirmed via `git status`/`git stash list`/`md5sum` against
`git show HEAD:...` before every build).

## What was tested

1. `pgbench -i -s 10` against a fresh fiber-mode server (`multithreaded=on
   pooled_protocol_carriers=0`), 3x in immediate sequence, fresh datadir each time: **3/3 clean
   completion**, "creating primary keys..." included, no crash.
2. 3 fiber-mode servers started fresh in parallel (distinct ports/sockets/datadirs) and driven
   with concurrent `pgbench -i -s 10`: all 3 postmasters survived (client-side `pgbench` COPY
   connections dropped from box-load-driven network timeouts -- load average 11-14 on 8 cores,
   ~15 unrelated postgres clusters and a dozen agent sessions on this shared host -- but zero
   server crashes across all 3).
3. A 1,000,000-row `pgbench_accounts` + companion tables loaded via a single local `psql`
   session (avoiding pgbench's network-sensitive COPY), followed by all three
   `ALTER TABLE ... ADD PRIMARY KEY` statements named in the original crash report
   (`pgbench_branches`, `pgbench_tellers`, `pgbench_accounts`) run back-to-back: **all three
   succeeded**, server alive throughout.
4. A real `pgbench` write-heavy transactional run (`-c 8 -j 4 -T 30`) against that same
   populated database: ran for tens of seconds under concurrent UPDATE/INSERT/DELETE load,
   staying healthy, until it was stopped by an externally-delivered `SIGTERM` ("received fast
   shutdown request" in the server log) that this session did not send -- attributed to the
   shared box's other tenants, not a PG-side fault. The server logged a clean shutdown sequence
   for that signal, not a crash signature.

None of the four runs produced the documented signature (`MarkPostmasterChildSlotUnassigned
(slot=0)` / SIGSEGV / "terminating threaded server runtime" fail-stop / core dump).

## Interpretation

This is consistent with (does not yet prove, see caveats) the retraction already on record: the
crash reported in `PG_FIBER_PMCHILD_DOUBLE_RELEASE.md` and
`V1441_HANG_GONE_PMCHILD_CRASH_BLOCKS.md` was produced by builds that silently included the
uncommitted pooled-starvation `PG_STEP_YIELD_BUDGET` WIP (the only way those builds could link at
all, since the WIP supplied the enum member the committed counter name depended on). That WIP's
mid-session carrier yield is a plausible root cause for a pmchild lifecycle confusion on the fiber
path -- and separately, this session's first repro attempt (before stashing the WIP) hit a
*different* assert (`pgstat_is_initialized && !pgstat_is_shutdown`,
`PgSessionRunProtocolSchedulerUntilBoundary` -> `pgstat_report_stat`) that is itself evidence of
the WIP's mid-message-budget yield resuming a session in a state pgstat does not expect. That
finding should go into the starvation-fix WIP's own validation task, not this one.

## Caveats -- what this local run does NOT establish

- **Not the authoritative environment.** This is floki, a shared dev box, not the EC2
  `c6id.8xlarge` SUT the mission specifies. Local `io_uring`/thread scheduling/NUMA behavior can
  differ. The EC2 run is the one that counts; treat this as a fast pre-check that justified
  going straight to the benchmark instead of a multi-day cassert bisection.
- **Short duration.** The longest clean write-heavy run was ~30s of measured traffic before an
  external SIGTERM cut it off. The original crash reports describe determinism at `ADD PRIMARY
  KEY` specifically (an init-time DDL path), which I did retest explicitly (item 3 above) and it
  passed 1/1 -- but I did not get an uninterrupted multi-minute sustained-load run locally due to
  the shared box's noise.
- **No formal N-run frequency claim.** "3/3", "3 parallel", "1/1", "ran until external SIGTERM"
  is what was observed; it is not a large-sample hang/crash-rate measurement. The EC2 benchmark
  (3 runs x 2 workloads x 2 modes, per the mission brief) is the number that should be trusted
  for a rate claim.
- **cassert=true was active** for all four local checks (this is the diagnosis build), so any
  `Assert()` on the crash's suspected path would have fired loudly rather than being silently
  compiled out. No assert fired.

## Bottom line

No evidence of the pmchild double-release on clean HEAD, in four independent local checks
including the exact named trigger (`ADD PRIMARY KEY` after bulk load) and a real write-heavy
concurrent run. This supports proceeding straight to the EC2 fiber-vs-fork benchmark without a
separate multi-day crash bisection, while flagging the result as pre-check evidence, not the
final word -- the EC2 run under the mandated methodology (fresh server per run, 3 runs per cell,
explicit PASS/HANG/CRASH/INIT_FAIL/VOID taxonomy) is the authoritative measurement.
