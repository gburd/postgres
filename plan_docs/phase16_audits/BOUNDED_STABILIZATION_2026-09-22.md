# Bounded stabilization checkpoint — 2026-09-22

**Result: partial repair, not a green threaded runtime. No performance claim.**

EC2 validation was blocked throughout: `aws sts get-caller-identity --profile
lava` returned `InvalidClientTokenId` (exit 254), including the final retry.
No instances, keys, security groups or other AWS resources were created.
Independent agents worked in isolated local worktrees instead.

## Integrated changes

- `405d6902b2`: pin libxtc **v1.49.2**, exact revision
  `542a67d9ea37a425a2ab7d6b11dff8993bd42871`, in both flake files.
  Fresh consumer build uses matching headers/library. Relevant delivered fixes
  include fd-registration/cancellation lifetime, paired cancellation masks and
  fiber-scoped configuration. None proves that the PG write wedge is fixed.
- `fac61a3787`: remove the unsupported LWLock clear from `a141499c86`, the
  racing wake/wait probes and their PGPROC counter. Correct the September 17
  matrix and diagnosis reports. Source files return to their pre-experiment
  contents; no replacement retry workaround or error policy was introduced.
- `705ea9c2d9`: repair lifecycle metadata for `PgCarrier.migratable`, remove
  obsolete refint owner rows, and preserve ruleutils/RI saved-plan cleanup
  before plan-cache reset without inventing root fields.
- `7626be413b`: fix a contradictory existing ownership test. It assigned
  `remote_ctx.finish_lsn` twice for each fake backend, then required both the
  old and new values. Remove the superseded assignments/checks; retain the
  distinct final values and isolation checks. This is test repair, not a
  production ownership fix.
- `151b728823`: complete dead-end client initialization/dispatch. The earlier
  `653e2afd29` admitted `B_DEAD_END_BACKEND` to the thread launcher but did not
  copy its startup data/socket or dispatch it through client startup. It
  reached `BackendMain(NULL, 0)` via worker dispatch and aborted. Fix all three
  missing conditions, preserving existing socket cleanup and leaving scheduler
  registration/fiber eligibility unchanged. Add `018_dead_end_backend.pl`.
- `d964dee0c4`, `4a557f2a3a`: repair the P1 diagnostic benchmark harness:
  stage-specific deadlines, concurrent CPU sampling, monitoring outside the
  workload database, host-identity/carrier checks, stock-binary opt-in,
  retained failure artifacts, bounded child-exit wait, sampler exit checking,
  and retained-versus-reported transaction-count checking. Shell/mock checks
  are not validation of real SSH, PostgreSQL load, or driver capacity.

## Executed local checks

Build directory: `build-stabilize-1492`, fresh Meson setup with
`-Dxtc=enabled -Dcassert=true -Dbuildtype=debugoptimized -Dplpython=disabled`.
`pkg-config` identifies xtc 1.49.2; linked library prefix is
`/nix/store/kv8kzpg1d3g7zpc7kxmnbywa04d7xrxh-xtc-1.49.2`.
This local Nix build is not an explicit-uring EC2 build or runtime-ring
verification. Do not label it as such.

| Check | Result |
|---|---|
| Fresh build, then incremental rebuilds of integrated repairs | PASS |
| Meson setup | 3/3 PASS |
| Final process regress suite | **239/239 PASS** (this configured suite, not historical 245) |
| `test_backend_runtime/regress` | failed on contradictory finish_lsn expectations before test repair; PASS after |
| `018_dead_end_backend` | process control passed and threaded rejection failed before fix; **16/16 PASS** after |
| Lifecycle checker | PASS: 180 fields, 180 buckets, 36 reset definitions, 427 owners |
| Global-lifetime boundary checker | **FAIL: 44 unclassified declarations** in integrated tree |
| P1 shell/mock selftest | 90 assertions reported by worker; independent rerun exit 0 |
| Existing benchmark selftests, shell syntax, ShellCheck | worker-reported PASS |
| `git diff --check` | PASS |
| Full fiber regression attempt | **TIMEOUT, exit 124**, not a completed suite |
| EC2 sustained load / stock performance matrix | NOT RUN: credentials invalid |

Final process and focused-test output: `/tmp/xtc-stabilize-final-tests.log`.
Dead-end positive-control log: `/tmp/xtc-deadend-green-testlog.txt`.
Negative-control log: `/tmp/xtc-deadend-red.log`.
Static logs: `/tmp/xtc-stabilize-lifecycle.log`,
`/tmp/xtc-stabilize-globals.log`.

### Fiber regression failure after the dispatch fix

Direct `pg_regress` used the full `parallel_schedule`,
`threaded_smoke.conf` (`multithreaded=on`, `pooled_protocol_carriers=0`,
`io_method=sync`, `summarize_wal=off`), `PG_XTC_CARRIER_LOOPS=4`,
`ulimit -n 131072`, and a 240-second external deadline.

`test_setup` passed. Nineteen members of the next 20-test parallel group were
reported complete; `numeric.out` stopped at a parallel `variance(a)` query
with `parallel_workers=4` and `max_parallel_workers_per_gather=4`.
The postmaster log recorded autovacuum worker startup timeouts. The test
never completed the group before the deadline. This is a localization, not
proof of the stalled operation's root cause or of the September 17 wedge.

The deadline produced a smart-shutdown request. A subsequent explicit immediate
shutdown returned after approximately five seconds of process-level
escalation. Teardown logs also contain signal-11 supervisor DOWN events and
`Assert(latch->owner_pid == MyProcPid)` in `latch.c`. Those occurred during
shutdown, not as demonstrated causes of the preceding stall. No matching
stabilization server remained afterward. Shutdown is not certified clean.

Artifacts retained under `/tmp/xtc-stabilize-fiber-regress-fixed/`, including
`log/postmaster.log` and SQL outputs; driver output is
`/tmp/xtc-stabilize-fiber-regress-fixed.log`. Earlier pre-fix startup assertion
is under `/tmp/xtc-stabilize-fiber-regress/`.

## Ownership work deliberately not integrated

The ownership agent classified all 44 declarations in its isolated worktree;
that working tree then reported **19 boundary violations**, not a green gate.
Its 21-file annotation/atomic draft remains uncommitted and is not part of
this checkpoint. The integrated tree still reports 44 unclassified entries.

The actionable review is `/tmp/xtc-global-ownership-remaining-2026-09-22.md`.
It includes session GUCs (`enable_groupagg`, `log_statement_max_length`,
`output_plugin_libraries_string`), `MySubscriptionConninfo`, RI metadata
cleanup, WAL callback/deferred-wakeup state, wait registration, logger routing,
and stale fiber-identity TLS. Making a declaration atomic cannot fix the
wrong logical owner. Do not refresh the baseline to hide these findings.

## libxtc supervision request

Comprehensive report:
`/tmp/libxtc-c-runtime-supervision-requirements-2026-09-22.md`.
Written locally; external delivery/response is not established.

It acknowledges delivered masks, monitor/spawn and cancellation cleanup.
Source-review requests (not new runtime reproductions) cover:

1. Prompt completed-fiber/task/stack reclamation. Upstream KNOWN_ISSUES says
   completed coroutine stacks remain until loop teardown. PG requests 8 MiB
   stacks, so connection churn cannot be judged by concurrent session count.
   No PG session-reuse or loop-recycling workaround is proposed.
2. Cleanup-complete semantics distinct from kill delivery/`alive == 0`.
3. Reliable or explicitly failed lifecycle delivery and monitor enrollment;
   cleanup-before-replacement ordering for group restart.
4. Process-fatal synchronous-fault policy before arbitrary C cleanup.
5. Before xproc adoption: typed OS exit/signal status, bounded terminate/reap,
   and safe exec spawning from multithreaded parents.

PostgreSQL owns transaction cleanup and shared-state integrity. Neither masks,
arenas nor supervision isolate arbitrary C pointer corruption. Genuine damage
requires whole-address-space fail-stop and external restart/WAL recovery.

## Next checkpoint and broadening order

1. Restore `lava` credentials; clean explicit-uring EC2 rebuild and verify
   exact source/header/library identity and actual runtime rings.
2. Reproduce/localize the parallel-worker stall and teardown failures with
   controlled traces and fork controls. Rebaseline fiber, worker, isolation,
   cancellation and crash gates. Handle hard runtime wait errors without
   turning them into unbounded successful-looking wakes.
3. Migrate remaining mutable state by actual owner with focused tests; make
   both static guards green without exemptions for unsafe session state.
4. Validate sustained connection churn and the library reclamation contract.
   Close safe completion/fault-policy gaps before expanding supervision use.
5. Stabilize the existing session-fiber route, make it the intended default,
   then delete the hand-built stackless scheduler. Do not optimize that
   scheduler as a competing normal model.
6. Measure **unmodified upstream / branch process / branch fiber** on matched
   resources, separate verified drivers, 85% RAM shared_buffers, fsync and
   autovacuum on, NVMe/XFS. Warm up, repeat, span checkpoints/maintenance;
   retain per-client progress, tail latency, CPU/transaction and memory slope.
7. Only then broaden runtime adoption one measured change at a time: cfg
   scoping, cancellation integration, allocator/pool policy and suitable
   synchronization primitives. Keep process mode supported throughout.

The owner's all-tests performance-win bar remains unmet. A pin update and a
few passing focused checks do not justify further broad fusion yet.
