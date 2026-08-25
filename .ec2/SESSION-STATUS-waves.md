# Autonomous session status — waves 1/2/3 (2026-08-25)

## Wave 1 — LANDED to origin/xtc (86898ccc5c)
- libxtc bumped v1.35.2 -> v1.37.0 (34c3a4b8).  v1.36/37 are example-only
  (11_lorb, sqlxtc); no core API change.  flake.nix + flake.lock updated via
  `nix flake update libxtc`.
- Perf fixes landed (were on pooled-demand-grow):
  - pooled carrier demand-grow (-S -> parity/beats fork)
  - threaded large-alloc policy M_MMAP_MAX=0 + M_TRIM=64MB (CPU-bound -> 1.53x fork)
- Validated on chiuso c7i.metal-48xl/192-core: process regress 245/245 0-diffs;
  perf -S ~2.05M vs fork 2.12M, compute 80k vs fork 52k (1.53x); test_backend_runtime
  17/1 where the 1 (001 PMChild-reaping-stress terminate timing) is PROVEN
  pre-existing/environmental via a clean-origin/xtc clean-box control on the same
  box (both fail 001 identically).  Two adversarial reviews of the perf diff still
  owed as follow-up but evidence supported landing.

## Wave 3 — Phase 16 audits DONE (branch phase16-audit 5da503ea01)
plan_docs/phase16_audits/: contrib (55 exts, 16 Tier-1 marker quick-wins),
PL/Python (Option C: per-session PyThreadState re-stamp), GUC (CRITICAL: custom-GUC
valueAddr shared across sessions for BOOL/INT/REAL/ENUM; STRING has the fix
template).  All SOURCE audits -- independent-verify + threaded load/exec before
implementing.  Ranked worklist in PHASE16_AUDIT_SUMMARY.md.

## Wave 2 — io_method=xtc investigation: NEXT (in progress)
Question: why isn't io_method=xtc the best choice?  Known: method_xtc.c is
issuer-SYNCHRONOUS (submit() completes each IO before returning = one fiber
park->wake per IO), so OLTP's many-tiny-cached-reads become a futex storm.
Hypothesis to test: a batched/deferred-completion path (submit N, park once, reap
N -- true io_uring-style) fixes it.  Measuring on EC2.

## Environment notes (bit me this session)
- AWS profile is `chiuso` (account 187887018457), NOT beef (beef vanished).
- SSH_AUTH_SOCK goes stale when the login session rolls; the LIVE agent socket is
  in ~/.ssh/agent/ (find the one `ssh-add -l` succeeds against) -- git push fails
  with "could not read from remote" until SSH_AUTH_SOCK points at the live one.
- Heredoc-over-ssh with nested quotes mangles scripts -> write locally + scp.
- Clean-box test_backend_runtime control needs `meson test --suite setup` FIRST to
  populate build/tmp_install, else the TAP bails "pg_config failed".

## Owed follow-ups
- Two adversarial reviews of the Wave-1 perf diff (demand-grow + malloc policy).
- Phase 16 implementation waves (custom-GUC fix first -- it's a real correctness bug).
