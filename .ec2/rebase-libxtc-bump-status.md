# Rebase onto origin/master + libxtc v1.35.2 bump (2026-08-06)

## Done (on branch xtc-rebase-candidate, NOT yet on origin/xtc)
- Unshallowed the clone (was shallow: 50 commits -> full history; merge-base with
  origin/master was hidden by the shallow graft).
- Rebased all 282 xtc commits onto origin/master (f6a062c477, current upstream
  postgres + fork CI).  merge-base was 0d99579320 (2026-08-03); master +254, xtc
  +286 over it.
- libxtc bumped v1.32.0 (563329f) -> v1.35.2 (586db118) in flake.nix + flake.lock.
- Candidate HEAD: 21e59b2eae (283 commits over master).

## Conflicts resolved (with care, key ones):
- postmaster.c: took xtc's cleanup_startup_child() refactor AND ported upstream
  ead8f696b7 (fix postmaster exit when startup crashes during crash restart) into
  its body.
- read.c/readfuncs.h: upstream 9673a0aa92 made stringToNode() thread-safe
  (ReadNodeContext) -- took master's version (supersedes xtc's node-read
  annotations, same goal).
- reloptions.c: xtc renamed 4 struct fields to relopt_* (vacuum_cost_delay/limit/
  truncate/max_eager_freeze_failure_rate); dropped xtc's duplicate tab[] and
  renamed master's authoritative tab[] refs to the relopt_* names (compiles).
- worker.c/worker_internal.h: merged master-only globals (MySubscriptionConninfo,
  in_remote_transaction, LogRepWorkerWalRcvConn) with xtc's per-backend #defines.
- acl.c/ri_triggers.c: took xtc's per-backend #define conversions.
- pgcrypto/pgp.c: kept xtc's #define-constants thread-safety refactor + added the
  new upstream ignore_cipher_failure field.
- catversion.h: took master's newer stamp each time.

## Compile-verified (local, against the resolved tree)
reloptions.c, worker.c, read.c, acl.c, ri_triggers.c -- all my hand-resolutions
compile clean (they don't depend on xtc-version symbols, so a valid check).

## PENDING before force-pushing origin/xtc (the discipline: validate-then-force)
Full EC2 build with libxtc v1.35.2 + threaded validation (check-threaded-pooled,
test_backend_runtime, process regress) MUST be green.  The v1.35.2 bump also fixes
the local stale-nix-pin (xtc_tuning_check) so a full local/EC2 build now links.
Only after green: git push origin +xtc-rebase-candidate:xtc (force, approved for
xtc).  origin/xtc is UNCHANGED until then.

## libxtc v1.32.0 -> v1.35.2 range to review on EC2
v1.34.0, v1.34.1, v1.35.0, v1.35.1, v1.35.2 -- verify runtime deps (loop/exec/
proc/aio/sync/io_uring/notify/amutex) and confirm the symbols xtc uses are intact
(xtc_send/recv/proc_spawn_monitor/proc_wait_fd/aio_preadv/tuning_check/
counter_add/exec_loop_stats), + no ABI break in the accept-drain/reroute paths.
