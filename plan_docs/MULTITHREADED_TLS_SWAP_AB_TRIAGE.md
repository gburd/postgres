# A/B follow-up track: TLS swap scope + triage of the other three

Session date: 2026-08-04.  Branch `xtc-tls-swap` off origin/xtc HEAD
`affdc85ee25`.  All build/test on EC2 c7i.4xlarge (AL2023), profile beef,
us-east-1.  libxtc built from the pinned rev
`563329f9487739ce33709b5fc210ba89bde03b87` (v1.32.0), autotools
`dist/configure --with-io-backend=uring --enable-shared`, installed to
`/usr/local`.

## PRIMARY: be_tls_* -> xtc_tls_* TLS backend swap -- NOT IMPLEMENTED THIS INCREMENT (honest scope)

Verdict: the swap is a single all-or-nothing ~2500-line security-critical unit
with one confirmed hard gap (SNI).  It should NOT land as a code change this
increment.  Two of the three historical blockers are now cleared by the v1.32.0
pin; the third (no partial-safe increment + SNI) stands and is decisive.  What
this session DID accomplish: re-validated the whole gate against genuine
v1.32.0 at runtime (the exact thing that could not be proven by local
reasoning), and confirmed the two previously-open source-tree blockers are
resolved so the swap is now purely a coherent-rewrite-when-forced task, not a
blocked one.

### Why not a clean swap (evidence, read end-to-end this session)

1. The carrier is ALREADY not blocked during steady-state TLS I/O.  PG's custom
   BIO (`port_bio_read`/`port_bio_write`, be-secure-openssl.c) routes all TLS
   socket traffic through `secure_raw_read`/`secure_raw_write` on the
   non-blocking socket.  On EWOULDBLOCK, `be_tls_read`/`be_tls_write` return
   `errno=EWOULDBLOCK` + `waitfor`, and `secure_read`/`secure_write`
   (be-secure.c) call `WaitEventSetWait`, which under `USE_XTC_CARRIER` YIELDS
   the carrier (the interrupt-wake eventfd drain path at FeBe pos 3).  The
   handshake uses `WaitLatchOrSocket`, also carrier-aware.  So the stated
   motivation ("OpenSSL BIO reads block the carrier when a fiber does TLS I/O")
   does not hold for the running code -- the yield already happens.  The swap
   ENABLES a future TLS-fiber unpin (TLS state moves into `xtc_tls_t`, off the
   per-OS-thread OpenSSL error queue); it is not a fix for current behavior.

2. xtc_tls owns the fd itself (`xtc_tls_create(ctx, fd)` does its own
   recv/send); it has no hook to inject PG's `secure_raw_read/write`.  PG
   deliberately interposes a custom BIO so it controls recv()/send() for the
   raw_buf pushback of pre-TLS-negotiation bytes (`port->raw_buf`), the Win32
   signal window, and interrupt handling.  Swapping to xtc_tls means
   re-homing that pushback + interrupt discipline -- not a drop-in.

3. SNI is a HARD, CONFIRMED gap in xtc_tls v1.32.0.  PG's
   `sni_clienthello_cb` (via `SSL_CTX_set_client_hello_cb`) parses the
   ClientHello and installs a DIFFERENT per-host `SSL_CTX` (cert/key/CA)
   mid-handshake (`SSL_set_SSL_CTX`).  xtc_tls binds ONE `xtc_tls_ctx_t` per
   connection fixed at `xtc_tls_create`, with NO ClientHello callback and no
   mid-handshake context swap (verified in the v1.32.0 xtc_tls.h -- the
   accessor set is complete, but there is no context-selection callback).
   `004_sni.pl` (102 subtests) is in the threaded harness and would break.
   The existing be-secure-openssl.c "ssl_sni no-migrate invariant" assert
   already documents this exact xtc_tls deferral (#29).

4. No partial-safe increment.  Handshake + custom BIO + I/O + all accessors
   (version/cipher/bits/subject-DN/issuer-DN/serial/server-cert-hash/
   has-peer-cert/ALPN) + SNI move as one unit.  There is no half that serves a
   connection identically AND is independently testable; a half-wired path is
   exactly the forbidden weaker-security path.

### Historical blockers now CLEARED by the v1.32.0 pin (progress recorded)

- RUNPATH / stale-1.8.0-libxtc blocker (plan_docs "RUNTIME-LIB BUG CAUGHT"):
  RESOLVED.  This session's postgres binary binds `libxtc.so.1 ->
  /usr/local/lib/libxtc.so.1 -> libxtc.so.1.32.0` at runtime (ldd), and
  v1.32.0 exports the full 20-symbol xtc_tls API including every security
  accessor (`xtc_tls_get_server_cert_hash`, `get_peer_subject_dn`,
  `has_peer_cert`).  The short-struct-read CVE-class risk is gone.
- migratable-proc API (the "UNPIN BLOCKED" libxtc request): LANDED.
  `xtc_proc_opts_t.migratable` exists in v1.32.0's xtc_proc.h.  So the
  "no forcing function / cannot unpin" reason is no longer categorically
  blocked -- the swap can co-land with a TLS-fiber unpin when someone drives
  that, but it still must be one coherent reviewed change WITH the SNI story
  resolved (accept ssl_sni=off on the migratable path, or wait for xtc_tls #29).

### What xtc_tls v1.32.0 DOES cover cleanly (for the eventual swap)

Server handshake, non-blocking read/write with wants_read/wants_write, graceful
shutdown, cert/key/CA load, encrypted-key passphrase_cb, verify_peer_mode
tri-state (REQUEST maps PG's "ask but don't require client cert"), CRL
file/dir, cipher_list/ciphersuites_13/groups, prefer_server_ciphers, ALPN, and
channel binding via `xtc_tls_get_server_cert_hash` (RFC-5929
tls-server-end-point, spec-proven byte-equivalent to PG's
`be_tls_get_certificate_hash`).  The ONLY server-side gap for PG is SNI.

### Validation this session (genuine v1.32.0 runtime, EC2)

- meson build (`-Dcassert -Dssl=openssl -Dxtc=enabled -Dtap_tests`):
  BUILD OK, 0 warnings / 0 errors.  ldd: libxtc.so.1 -> 1.32.0.
- autoconf build (`--enable-cassert --with-openssl --enable-tap-tests
  --with-libxtc`): built; ONE pre-existing warning on origin/xtc HEAD
  (`launch_backend.c:1553: ISO C90 forbids mixed declarations and code`,
  -Wdeclaration-after-statement) -- NOT introduced here (zero code changes),
  flagged for a later cleanup; meson build does not surface it (different
  flags).
- Process regress: 245/245.
- Process ssl (`make -C src/test/ssl check` + meson ssl suite): 001=272,
  002_scram=28, 003_sslinfo=21, 004_sni=102 -> all green (423 total).
- Threaded ssl harness (`make -C src/test/ssl check-threaded-ssl
  PG_TEST_EXTRA=ssl`): 001=272, 002_scram=28, 004_sni=102 -> Result PASS,
  402 tests.  Carrier confirmed up in the node log ("starting startup thread
  carrier", walwriter/bgworker/autovacuum thread carriers) -- server-side TLS
  handshake, client-cert peer DN, CRL reject, SCRAM channel binding, and the
  ssl_sni gate all exercised on the backend-fiber path, not silently in
  process mode.  003_sslinfo correctly excluded (process-only contrib, Phase
  16 territory).

So the OpenSSL be_tls_* path is byte-for-byte unchanged and green in both
process and carrier modes on genuine v1.32.0; the harness is ready as the live
regression gate for whenever the swap co-lands with a TLS-fiber unpin.

## SECONDARY: triage of the other three follow-ups

### 1. Phase 19 Increment 4 (lazy re-placement to fork+exec)

Status: BLOCKED on Phase 17, by design; the fork+exec plumbing it needs is
already DONE.  Increments 1-3 landed (classification 66dc4d18674; the meaty
fork+exec route `postmaster_pooled_protocol_process_fallback` in
launch_backend.c behind `xtc_force_process_fallback`, exec'd-child state
restore, crash contract TAP 013; server-wide preload detection 23e99a9b159).
Inc 4 is "abort the uncommitted command mid-flight and re-place a LIVE session
as a process backend" on late-discovered incompatibility (LOAD / CREATE
EXTENSION / first fmgr call).  It REQUIRES a clean mid-command unwind, which is
Phase 17 machinery that does NOT exist on the current tree (grep for
clean-unwind / command_unwind / replace_session_as_process: absent).  Phase 17
was itself re-scoped (its real carrier-monopolizer target is
ProcWaitOnSemaphore, not a stackless rewrite), so the clean-unwind primitive
Inc 4 needs is not yet built.  Remaining work: modest routing glue once the
unwind primitive exists (branch dfmgr's "needs-process" onto the existing
PROCESS_FALLBACK route mid-command); the blocker is the unwind primitive, not
the fork+exec route.  Plan: keep case 3/4 fail-closed ERROR (with the Inc-1
actionable message) until Phase 17 lands the unwind; do Inc 4 as a small
follow-on then.  NOT actionable now.

### 2. TSan

Status: NOT a libxtc gap anymore -- it is a toolchain-env blocker.  The
premise (libxtc has no `__sanitizer_*_switch_fiber` fiber annotations) is
OUTDATED: libxtc v1.16.0 added the ASan fiber-switch annotations AND the
distinct TSan fiber-IDENTITY API (`__tsan_create_fiber` /
`__tsan_switch_to_fiber` / `__tsan_destroy_fiber`, clang-only via
`__has_feature(thread_sanitizer)`, guarded by `XTC_TSAN_FIBERS`).  v1.32.0
still has them (verified: coro_int.h `tsan_fiber` token + coro_fctx.c
annotations).  So the CODE gap is closed on both libxtc and PG sides.  The
remaining blocker is a nix clang-compiler-rt <-> glibc GLIBC_PRIVATE version-
tag skew: a `clang -fsanitize=thread` binary dies at load with "undefined
symbol: __nptl_change_stack_perm, version GLIBC_PRIVATE".  Verdict: this is
NOT a new libxtc request and NOT a PG code change; it is a build-environment
task (match compiler-rt to nix glibc, or run TSan against a system
clang+glibc outside the nix devshell).  The happens-before tracking will work
once the toolchain is consistent -- the annotations are proven present.  Plan:
run TSan-on-the-carrier from a glibc-consistent clang stdenv; no code owed.

### 3. thr Coccinelle durability-gate set

Status: EXISTS, out-of-scope for this branch.  Confirmed in the SEPARATE
worktree /home/gburd/ws/postgres/thr (branch `thr`, HEAD c4f01a79d52
"docs+scaffold: Coccinelle durability-gate mission").  It has the
`plan_docs/COCCINELLE_DURABILITY_PLAN.md` staged plan (encode coding
invariants as machine-checked spatch rules under src/tools/, proposable to
pgsql-hackers) and a top-level `cocci-gate` directory that is present but EMPTY
-- scaffolded, not worked.  It was NOT built or tested on this instance (a
different worktree/branch with its own build).  Plan: leave to the thr
worktree; nothing for the xtc branch to do.
