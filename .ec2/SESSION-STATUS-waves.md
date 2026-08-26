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

## Wave 2 — io_method=xtc investigation: DONE (branch io-xtc-write-fastpath d762ee45ed)
Finding (.ec2/io-method-xtc-WHY-NOT-BEST.md): the old "io=xtc futex-storms OLTP"
is STALE.  Measured on v1.37+perf tree (192-core):
  cached -S: io=xtc 2,023k vs sync 1,915k (+5.6%, futex ~0%)
  write -N:  io=xtc 47k vs sync 64k (-36%)  <- the real weakness (per-write park,
             no fast path; only READV had preadv2(RWF_NOWAIT))
  cold -S:   parity (~4,800 both)
Root: method_xtc is issuer-synchronous (one fiber park per IO); xtc_aio has no
batched submit/reap; PG OLTP issues 1 IO/ReadBuffer so no batch to amortize.
FIX built + measured: added pwritev2(RWF_NOWAIT) write fast path (mirror the read
one).  -N recovered to parity: io=xtc 52k/60k vs sync 56k/57k -- the -36%
regression CLOSED.  io=xtc now neutral-or-better across cached-read/cold-read/write.
Branch io-xtc-write-fastpath pending two reviews + wider A/B before landing.
Recommendation: keep io=sync default for now; retire the stale futex-storm warning.

## Owed follow-ups (updated)
- Two adversarial reviews of: (a) Wave-1 perf diff (demand-grow + malloc policy,
  LANDED on xtc), (b) io write fast path (branch io-xtc-write-fastpath).
- Phase 16 implementation (custom-GUC valueAddr fix first -- real correctness bug).
- Wave 4: TLS hook (check v1.37 for SNI/transport), Phase 17 deep-waits, Phase 19
  Inc-4, xtc_preempt experiment (low priority -- malloc fix already beats fork).

## Wave 4 groundwork
- **TLS swap UNBLOCKED by v1.37**: xtc_tls.h now has xtc_tls_ctx_set_sni_cb (SNI
  ClientHello context selection) + xtc_tls_create_transport (BIO-like custom
  transport) -- the two hooks the be_tls_*->xtc_tls_* swap was blocked on.
  Dispatched a read-only design agent to map the swap (be_tls_* surface in
  be-secure-openssl.c ~2598 lines <-> xtc_tls API; the crux is PG's
  be_tls_read/write *waitfor non-blocking-retry vs xtc_tls fiber-park).  Security-
  critical + large -> design-first, implement carefully MYSELF later, not fanned out.
- Two adversarial reviews dispatched: demand-grow scheduler (LANDED), and
  malloc-policy + io-write-fastpath.

## Review gate + fixups — DONE
Two adversarial committer-grade reviews of the Wave-1 perf + io changes: both
SHIP-WITH-NITS.  Confirmed: RSS bounded (no runaway); io write short-write
fall-through is correct idempotent positional re-write (not a double-write).
Actioned nits (commit 2661d776f8 on xtc):
 - F1 (MEDIUM): gate resume-path grow on pmState==PM_RUN && Shutdown==NoShutdown
   (was spawning carriers during shutdown -- the 001 reaping-stress surface).
 - F2: cache ncpus in maybe_request_grow (was sysconf per lease).
 - malloc-retention comment corrected.
 - io branch: buffered pwritev2(RWF_NOWAIT) kernel-caveat comment (58ec94db6d).
Validated on chiuso c7i.8xlarge (cassert): process regress 245/245 0-diffs;
test_backend_runtime 0 actual failures across 2 runs (run1 had only the known
015 load-flake TIMEOUT; 001 PASSED BOTH RUNS -- confirming 001 was the metal-load
flake, and F1 is neutral-to-helpful); threaded smoke clean, FAST_STOP rc=0.
Landing review-fixups + TLS design doc to origin/xtc.

## Follow-up session (continued) — Phase 16 contrib + custom-GUC design
LANDED on origin/xtc (9148d89b19):
- 8 contrib extensions marked POOLED_PROTOCOL_AFFINE (file_fdw, lo,
  pg_freespacemap, pg_visibility, pg_buffercache, sslinfo, pg_logicalinspect,
  auto_explain).  Validated: test_extensions PASS; all load under mt=on;
  auto_explain per-session GUC isolated (333ms in its own session).
- Review-gate fixups (F1 shutdown guard + F2 + comments) LANDED + validated
  earlier (b0f9952b4e).
- TLS swap design doc landed (unblocked by v1.37).

Custom-GUC bug — CORRECTED understanding (independent design review,
plan_docs/phase16_audits/CUSTOM_GUC_FIX_DESIGN.md): NOT a guc.c change; STRING's
guard is a red herring.  The fix is per-extension: redefine GUC globals as macros
over PgSessionEnsureExtensionPrivateState cells + pass &cell to DefineCustom*.
auto_explain already done (hence markable).  pg_stat_statements + postgres_fdw
need this conversion before they can be marked (deferred).

Branches: phase16-contrib-tier1 (landed), io-xtc-write-fastpath (pending kernel
A/B), phase16-audit, pooled-demand-grow (landed via cherry-pick).

## Remaining follow-ups
- pg_stat_statements + postgres_fdw per-session GUC conversion, then mark.
- xml2 libxml threaded-safety confirmation, then mark.
- io write fast path: target-kernel A/B confirming pwritev2(RWF_NOWAIT) fires, then land.
- TLS swap implementation (P0 gates first).
- plpython Option C implementation.

## Follow-up round 2 — Phase 16 tier-2 + a real preload finding
LANDED on origin/xtc:
- postgres_fdw marked AFFINE (395c53a8a6): fully per-session-converted
  (application_name GUC + whole connection cache); validated under mt=on --
  loopback FDW scan=100, pushdown=10, 0 crashes.
- Re-audit found pg_stat_statements + postgres_fdw were BOTH already fully
  per-session-converted (the earlier 'defer' used an incomplete heuristic).

NEW FINDING (real threaded-mode gap, c05fc28255,
.ec2/preload-custom-guc-threaded-gap.md): shared_preload_libraries custom GUCs are
INVISIBLE per-session under multithreaded=on.  Measured A/B: pg_stat_statements.track
resolves mt=off, errors 'unrecognized configuration parameter' mt=on, though the
module IS dlopen'd in the postmaster.  Root cause (analysis): custom GUCs live in a
per-session guc_hashtab; a preload module's _PG_init registers them in the postmaster
hashtab, inherited by forked backends but NOT by fresh per-session threaded hashtabs.
Core-runtime fix needed (seed/share preload custom-GUC registry into per-session GUC
state) -- design-first.  Consequence: pg_stat_statements marker is correct but can't be
validated until this is fixed -> HELD on branch phase16-contrib-tier2.

xml2: stays PROCESS (defer-with-invariant) -- pg_xml_init installs a process-global
libxml error handler (xmlSetStructuredErrorFunc) with no cross-carrier serialization;
a pre-existing core-xml-type hazard that owns both the core xml type and xml2.

## Remaining follow-ups (updated)
- CORE: fix the preload-custom-GUC-per-session gap (unblocks pg_stat_statements +
  any preloaded-module GUCs under threading) -- design-first, core runtime.
- CORE: make the libxml error handler session-safe (unblocks xml2 + hardens core xml).
- io write fast path: target-kernel A/B confirming pwritev2(RWF_NOWAIT) fires, then land.
- TLS swap implementation (P0 gates first); plpython Option C implementation.
- pg_stat_statements marker (branch phase16-contrib-tier2) lands after the preload fix.

## Follow-up round 3 — surfaced-gap designs + io fast path landed
LANDED on origin/xtc (f4a309609a):
- io=xtc write fast path (pwritev2 RWF_NOWAIT): reviewed SHIP-WITH-NITS, validated
  (process regress 245/245 byte-for-byte, io=xtc smoke clean, -N regression closed,
  fast path confirmed firing on AL2023 kernel 6.1.x).  io=xtc now neutral-or-better
  across read+write OLTP; io=sync stays default.

DESIGNS PRODUCED (both design-ready, NOT implemented -- correctly gated):
- preload custom-GUC gap: design + ADVERSARIAL REVIEW.  Review caught a fatal flaw:
  seeding the descriptor alone is BROKEN (valueAddr is a frozen postmaster-heap
  pointer -> all sessions' SHOW/SET race one shared cell).  Correct fix needs a NEW
  extension-supplied per-session value accessor (mirror built-in threaded_accessor +
  RebindSessionGUCVariablePointer) that the seed rebinds through -- core + a new
  extension-facing API.  Larger; needs the accessor-API design + two-review gate.
  Recorded in PRELOAD_CUSTOM_GUC_FIX_DESIGN.md.  pg_stat_statements marker stays HELD.
- libxml error handler: design.  Finding DE-ESCALATES the concern: libxml error
  globals are THREAD-LOCAL not process-global; with cooperative + preemption-off +
  pinned fibers + NO yield between pg_xml_init/done, xml2 is SAFE TODAY (implicit
  invariant).  Durable fix = per-context handlers (xmlCtxtSetErrorHandler, libxml2
  >=2.13) version-gated in shared pg_xml_init/done (hardens core xml + xml2); libxslt
  error globals flagged as a sibling to audit.  Recorded in
  LIBXML_ERROR_HANDLER_THREADED_DESIGN.md.

## Remaining follow-ups (updated)
- preload custom-GUC: design the extension accessor-registration API, then implement
  core seed+rebind (unblocks pg_stat_statements + all preloaded-module GUCs).
- libxml: implement per-context handlers (version-gated) + audit libxslt, then mark xml2
  (or mark xml2 now with the documented no-yield invariant guarded -- lower-risk interim).
- TLS swap implementation (design done, P0 gates first); plpython Option C.
- pg_stat_statements marker (branch phase16-contrib-tier2) lands after preload fix.

## Landed this whole session (origin/xtc summary)
libxtc v1.35.2->v1.37.0; pooled demand-grow (-S parity/beats fork); malloc policy
(CPU-bound 1.53x fork); io=xtc read+write fast paths; review-gate F1/F2 fixups; 9
contrib AFFINE markers (7 stateless + auto_explain + postgres_fdw); TLS + 2 GUC/libxml
fix designs; 3 threaded-mode findings root-caused (livelock-was-misdiagnosis, malloc,
preload-GUC gap).  All EC2 verified-clean, no leaks.

## Follow-up round 4 — agent team fan-out (4 designs) + xml2 LANDED
Dispatched 4 parallel read-only design/audit agents; all delivered:
- TLS P0 gates CONFIRMED from libxtc v1.37 source (yield-free AGAIN mode; clean EOF
  = XTC_OK n==0) -> TLS swap design implementation-ready, retry-mode correct.
- Preload-custom-GUC accessor API design (corrected, self-consistent): option (b)
  RegisterCustomGUCSessionAccessor*, reuses existing accessor union, core seed rebinds
  per session like RebindSessionGUCVariablePointer; no-accessor session GUC fail-closed.
- libxslt audit: xml2's only global hazard = per-call xsltCleanupGlobals(); error
  globals only-read.
- plpython Option C plan (corrected): real work is GIL (PyGILState_Ensure/Release per
  entry), not PyThreadState re-stamp; move 2 exec-stack heads + GD per-session.

LANDED on origin/xtc (b35b60cb08) -- xml2 threaded-safe + AFFINE, validated:
- xml.c XtcPgNoStealEnter/Leave tripwire on pg_xml_init/done (enforce no-yield invariant)
- xslt_proc.c skip xsltCleanupGlobals() under mt=on (cross-carrier libxslt-registry race)
- xpath.c mark xml2 AFFINE
- Validated: process regress 245/245 byte-for-byte, xml2 process regression PASS,
  concurrent xslt_process x8 clean (all HELLO, 0 crashes/asserts), NoSteal tripwire 0-fired.
- 4 implementation-ready designs committed (b9c6f5e4a3).

## Remaining follow-ups (updated)
- IMPLEMENT preload-custom-GUC accessor API + core seed/rebind (design ready+reviewed)
  -> unblocks pg_stat_statements marker (branch phase16-contrib-tier2).
- IMPLEMENT TLS be_tls_*->xtc_tls_* swap (design + P0 gates confirmed; P1 onward).
- IMPLEMENT plpython Option C (GIL PyGILState per entry + 2 stack heads + GD per-session).
- Durable libxml/libxslt per-context error handlers (drop the implicit no-yield reliance).

## Follow-up round 5 — preload custom-GUC fix LANDED (through 2-review gate)
LANDED on origin/xtc (9793ba0024): the preload custom-GUC per-session accessor API.
 - New extension API RegisterCustomGUCSessionAccessor* + core registry/seed/rebind;
   unblocks preloaded-module custom GUCs under mt=on (SHOW pg_stat_statements.track now
   works, per-session isolated; was 'unrecognized configuration parameter').
 - TWO-REVIEW GATE: design review corrected the approach (accessor needed, not just a
   descriptor seed); implementation review found a BLOCK (F1 NULL-deref crash on
   runtime-scoped no-accessor customs -> whole-process fail-stop) + F2 fail-closed hole
   + F3 unbooted cell -- ALL fixed and re-validated.
 - Validated: process regress 245/245 byte-for-byte; preloaded pgss mt=on track/max/save
   visible, pg_settings scan works (F1 gone), per-session track isolation PASS, 0 crashes.
 - pg_stat_statements accessor registrations landed; AFFINE marker DEFERRED.

NEW FINDING (separate): pg_stat_statements VIEW doesn't work under mt=on ('must be
loaded via shared_preload_libraries'; mt=off fine) -- a pgss shmem-under-threading gap
(pgss/pgss_hash from shmem_startup not visible to the querying session's runtime state),
NOT the GUC fix.  Investigation agent dispatched to determine pgss-specific vs general
extension-shmem-under-threading + fix size.  pgss AFFINE lands after this.

## Remaining follow-ups (updated)
- pgss shmem-under-threading gap (in flight) -> then pgss AFFINE.
- IMPLEMENT TLS be_tls_*->xtc_tls_* swap (design + P0 gates confirmed; ready).
- IMPLEMENT plpython Option C (GIL PyGILState per entry + 2 exec-stack heads + GD).
- Durable libxml/libxslt per-context error handlers (drop implicit no-yield reliance).

## Follow-up round 6 — pg_stat_statements FULLY threaded (chain closed)
LANDED on origin/xtc (342f813a52): pgss shmem fix + AFFINE marker.
 - Fix (a): pgss/pgss_hash moved from pgss_runtime_state() to plain PG_GLOBAL_SHMEM
   statics (process-wide singletons set once by shmem_startup; the runtime-scoping
   stranded them in the early-fallback runtime, invisible to carrier sessions).
 - Validated: pgss view rows>0 under mt=on (was 'must be loaded' error), tracked query
   calls>=1, reset works, session-GUC isolation PASS, 245/245 byte-for-byte, 0 crashes.
CHAIN CLOSED: preload-GUC gap -> accessor API (2-review gate) -> pgss session GUCs
-> pgss shmem fix -> pg_stat_statements fully functional + AFFINE under multithreaded=on.

Contrib AFFINE now: the 7 stateless + auto_explain + postgres_fdw + xml2 + pg_stat_statements.

## Remaining follow-ups
- IMPLEMENT TLS be_tls_*->xtc_tls_* swap (design + P0 gates confirmed; largest ready item).
- IMPLEMENT plpython Option C (GIL PyGILState per entry + 2 exec-stack heads + GD; design ready).
- Durable libxml/libxslt per-context error handlers (xmlCtxtSetErrorHandler >=2.13 +
  xsltSetTransformErrorFunc) -- drop the implicit no-yield reliance (currently guarded
  by the XtcPgNoStealEnter/Leave tripwire).
- General extension-shmem-under-threading fix (c): core adopts early extension-module
  state into the threaded runtime (only needed if a future extension's shmem_startup
  stashes state in PgRuntimeEnsureExtensionPrivateState; pgss used the small local fix).
- Phase 16 broader contrib sweep; sanitizers/stress; Phase 17/19 deep-wait items.
