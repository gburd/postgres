# libxtc TLS — findings & requests from the PostgreSQL threaded-runtime integration

Date: 2026-08-26
libxtc: v1.37.0 (rev 34c3a4b8), `src/io/tls_openssl.c`, `src/inc/xtc_tls.h`
Consumer: PostgreSQL `xtc` branch, `be_tls_* -> xtc_tls_*` swap, phases P1-P3 landed.

Context: PostgreSQL now drives server-side TLS through the `xtc_tls_*` API on the
carrier-fiber path (custom transport over PG's socket, retry-mode AGAIN driven by
PG's own wait loop). P1-P3 (ctx build, transport, handshake, read/write/close, ALPN,
version/cipher/bits + SCRAM channel-binding cert hash) are implemented and pass the
PostgreSQL `src/test/ssl` suite (001_ssltests, 002_scram, 004_sni) in threaded mode.
The items below are what we had to work AROUND or DEFER; none are P1-P3 blockers, but
each forces PostgreSQL to keep a connection on OpenSSL (pin) or accept reduced
fidelity. Fixing them lets us widen the xtc TLS surface (P4-P7) and eventually retire
the OpenSSL fallback for server TLS.

Legend: [BUG] behaves incorrectly vs OpenSSL; [GAP] missing capability; [NIT] minor.

---

## 1. [BUG] RSA-PSS server cert -> wrong SCRAM channel-binding hash
`xtc_tls_get_server_cert_hash` (tls_openssl.c:~1218-1276) derives the digest via
`OBJ_find_sigid_algs(X509_get_signature_nid(cert), &md_nid, NULL)`. For an RSA-PSS
signature (`NID_rsassaPss`) that yields `NID_undef` (PSS carries the digest in the
algorithm PARAMETERS, not the sig OID), so the code falls back to SHA-256.

PostgreSQL's own getter (be-secure-openssl.c `be_tls_get_certificate_hash`) prefers
`X509_get_signature_info(cert, &md_nid, ...)` (OpenSSL 1.1.1+), which resolves RSA-PSS
to its real digest (e.g. SHA-512). RFC 5929 tls-server-end-point requires the cert's
actual signature-hash (upgraded to SHA-256 only for MD5/SHA-1).

Consequence: for an RSA-PSS-signed server cert whose real digest is NOT SHA-256, the
xtc binding hash differs from what an OpenSSL server (and a stock libpq client)
computes -> SCRAM-SHA-256-PLUS fails. RSA/ECDSA-SHA256 (the common case, and all PG
TAP certs) are unaffected because both paths yield SHA-256.

Request: use `X509_get_signature_info()` when available (guard for <1.1.1 / BoringSSL,
falling back to the current OBJ_find_sigid_algs path). This is a drop-in match to
PostgreSQL's algorithm and closes the only known channel-binding divergence.
Workaround today: PostgreSQL will pin RSA-PSS server certs to OpenSSL once it parses
the cert at init; a fixed getter removes that need.

---

## 2. [GAP] No client-certificate verify-failure detail hook (errdetail parity)
PostgreSQL installs an OpenSSL verify callback (`verify_cb`) that captures a rich
per-depth errdetail ("certificate verify failed at depth N: <reason>; subject=...,
issuer=..., serial=...") surfaced in the handshake error message. `xtc_tls`'s verify
path (tls_openssl.c: `SSL_set_verify(ssl, vmode, NULL)` at :288, then a plain
`SSL_get_verify_result() != X509_V_OK` gate at :1073) has no callback and no way to
report WHY verification failed.

Consequence: on the xtc path, a client-cert verification failure is correct (handshake
fails closed) but the operator/log loses the diagnostic detail OpenSSL provides.

Request: an optional verify-result-detail accessor, e.g.
`int xtc_tls_get_verify_error(const xtc_tls_t *, long *x509_err, char *buf, size_t)`
returning the `X509_STORE_CTX` error code + `X509_verify_cert_error_string()` text (and
ideally the failing depth/subject). Or a settable verify callback on the ctx. This is
needed for PostgreSQL P5 (client-cert auth) to reach log parity; until then we pin
`ssl_ca`-configured servers to OpenSSL.

---

## 3. [GAP] No custom finite-field DH parameters (`ssl_dh_params_file`)
`xtc_tls_opts_t` exposes `groups` (ECDHE curve list) but no way to load explicit
finite-field DH parameters. PostgreSQL supports `ssl_dh_params_file` (custom FFDHE
params for legacy TLS 1.2 DHE ciphersuites) via `SSL_CTX_set_tmp_dh`.

Consequence: a server configured with `ssl_dh_params_file` cannot be represented; we
pin such servers to OpenSSL. Low urgency (TLS 1.3 uses `groups`/ECDHE; FFDHE is legacy),
but noted for completeness.

Request: an `opts.dh_params_file` (or a `dh_params_pem` buffer) field, applied via
`SSL_CTX_set_tmp_dh`/`SSL_CTX_set_dh_auto` on the OpenSSL backend.

---

## 4. [GAP] No SSL_CTX escape hatch for extension init hooks (`openssl_tls_init_hook`)
PostgreSQL lets extensions install an `openssl_tls_init_hook(SSL_CTX*, bool)` to tweak
the raw context at init. `xtc_tls` owns the `SSL_CTX` internally and exposes no handle.

Consequence: a server with such a hook installed must stay on OpenSSL (we pin it). This
is arguably by-design (the hook is inherently OpenSSL-specific), but a documented
"escape hatch" — e.g. `xtc_tls_ctx_get_native_handle(ctx) -> void*` (OpenSSL: SSL_CTX*)
with a clear "backend-specific, may be NULL" contract — would let a consumer apply such
hooks without abandoning the xtc stack. Optional / lowest priority.

---

## 5. [NIT] Peer subject/issuer DN is RFC 2253 only; PostgreSQL logs want slash form too
`xtc_tls_get_peer_subject_dn` / `_issuer_dn` render via `X509_NAME_print_ex(..,
XN_FLAG_RFC2253)` (tls_openssl.c:1101) -> `CN=x,O=y`. This MATCHES PostgreSQL's
`port->peer_dn` (which is also RFC 2253). But PostgreSQL's `be_tls_get_peer_subject_name`
/`_issuer_name` (used in `log_line_prefix %s`, and by the `sslinfo` extension) use the
legacy SLASH form `/CN=x/O=y` (`X509_NAME_to_cstring` = `X509_NAME_oneline`-ish).

Consequence: when PostgreSQL P5 wires these getters, log/sslinfo output on the xtc path
will be RFC 2253 where stock PG emits slash form — a cosmetic format change, not a
correctness issue. We can post-process, but a note in the xtc docs that these are
RFC 2253 (and, if easy, an optional flag or a second slash-form getter) would help.
`xtc_tls_get_peer_serial` (BN_bn2dec, decimal) already matches PG exactly — good.

---

## 6. [GAP / future] SNI ClientHello context selection (server side)
`xtc_tls_ctx_set_sni_cb` exists (server SNI selection callback), but PostgreSQL's SNI
support (`ssl_sni=on`, per-host cert/CA from `pg_hosts.conf`) additionally needs to
INSTALL a different context/cert mid-ClientHello. This ties into libxtc deferred #29
(the ClientHello context-swap). Until that lands, PostgreSQL pins `ssl_sni=on`
connections to OpenSSL AND keeps TLS-bearing fibers non-migratable. Tracking only; no
action requested beyond #29.

---

## What works well (for your regression awareness)
- Custom transport (`xtc_tls_create_transport` + read/write cbs): the BIO-like contract
  (>0 / 0=EOF / XTC_E_AGAIN / negative-hard-error) is exactly right and let us honor
  PostgreSQL's `raw_buf` pushback without touching the fd. This is the linchpin of the
  integration — thank you for it.
- Retry-mode AGAIN (no internal park): composes perfectly with PostgreSQL's existing
  wait/park loop; zero double-park.
- `xtc_tls_get_server_cert_hash` RFC 5929 logic (MD5/SHA-1 -> SHA-256 upgrade, SHA-256
  fallback) matches PG byte-for-byte EXCEPT the RSA-PSS case (#1).
- ALPN: `opts.alpn_protos` wire-encoding + `xtc_tls_get_alpn_selected` cleanly support
  PostgreSQL's direct-SSL requirement.
- `xtc_tls_ctx_destroy` NOT closing the fd, and per-connection `xtc_tls_t` holding no
  implicit fd ownership: correct for PG's Port-owns-the-socket model.

## One integration caution we hit (not a libxtc bug)
`xtc_tls_ctx_destroy` frees the ctx wrapper immediately with no refcount, while live
`xtc_tls_t` objects dereference `t->ctx` (e.g. in `xtc_tls_get_server_cert_hash` role
check). A consumer that rebuilds the ctx on config-reload while connections are live
MUST NOT destroy the old ctx in place (use-after-free). PostgreSQL retires old contexts
to a never-freed list instead. If you'd consider refcounting the ctx wrapper (so
`xtc_tls_destroy` on the last connection drops the ctx), that would let consumers avoid
the bounded leak. Optional.

## UPDATE (2026-08-27, after v1.38.0): #1 and #2 FIXED, one residual

v1.38.0 fixed finding #1 (RSA-PSS channel-binding hash now uses
X509_get_signature_info) and added xtc_tls_get_verify_error() for #2 -- both
verified in PG: 002_scram (SCRAM channel binding + client-cert PLUS) now PASSES
on the xtc path, and ssl_ca (client-cert) is UN-PINNED.  Thank you.

### Residual [GAP]: verify-error accessor gives the RESULT, not the failing CERT
xtc_tls_get_verify_error(tls, &x509_err, buf, len) returns SSL_get_verify_result +
X509_verify_cert_error_string -- enough to log line 1 of PG's errdetail
("Client certificate verification failed at depth 0: <reason>") which now MATCHES.
But PG's OpenSSL verify_cb ALSO logs a second line naming the FAILING cert:
  Failed certificate data (unverified): subject "/CN=ssltestuser", serial number N,
  issuer "/CN=Test CA ..."
Reproducing that needs the FAILING peer certificate (subject/serial/issuer) at the
point of failure -- available to OpenSSL's per-depth X509_STORE_CTX verify callback,
but NOT reachable from the final-result accessor (xtc_tls_has_peer_cert() returns 0
on a failed verify, so the getters can't fetch it either).  7 src/test/ssl
001_ssltests "log matches" assertions require this second line and stay red on the
xtc path.  Auth is CORRECT (revoked/untrusted/missing-intermediate all rejected);
this is a diagnostics-parity gap only.

Request: a verify CALLBACK (OpenSSL SSL_set_verify(..., cb) shape) invoked per
depth with the X509_STORE_CTX, OR an accessor that returns the failing cert's
subject/serial/issuer + depth after a failed handshake (e.g. via
SSL_get0_verified_chain / the last cert seen).  That closes the last errdetail-
parity gap and turns check-threaded-ssl 001 fully green on the fiber path.
