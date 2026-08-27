# TLS be_tls_* -> xtc_tls_* swap, P2+P3 — validation (2026-08-26)

Branch `tls-xtc-p1` @ c3df51b5e7 (P1 3790603755 + P2/P3 + review fixes).  chiuso
c7i.4xlarge, AL2023, OpenSSL, cassert build for the TAP run.

## What P2/P3 do
- **P2**: be_tls_init builds a parallel xtc_tls SERVER context (xtc_tls_context)
  from the default-host cert/key + crl + cipher(list/13) + groups + min/max-version
  GUCs (GUC -> xtc_tls_opts_t), alongside SSL_context.  Gated on `multithreaded`.
- **P3**: fiber TLS connections are served by xtc_tls via the CUSTOM TRANSPORT
  (secure_raw_read/write callbacks -> raw_buf drained first), handshake parks
  through PG's WaitLatchOrSocket seam, read/write use xtc_tls XTC_E_AGAIN retry-mode
  mapped to PG's *waitfor, close = close_notify + destroy (Port owns fd).  ALPN
  ("postgresql") negotiated; version/cipher/bits + SCRAM cert-hash getters wired.

## Two-review gate (both BLOCK -> fixed -> re-review)
Review 1 (protocol/SCRAM) + Review 2 (threading/lifecycle) each returned BLOCK:
- B1 fd path dropped raw_buf (direct-SSL hang / pipelined reject) -> custom transport.
- B2 direct-SSL rejected (alpn_used=false) -> ALPN wired.
- B2 SCRAM-PLUS advertised but cert-hash NULL (default TLS+password auth failed,
  fail-closed) -> be_tls_get_certificate_hash_xtc via xtc_tls_get_server_cert_hash.
- B3 xtc_tls_ctx_destroy on SIGHUP freed a ctx live fibers hold (UAF) -> retire to
  xtc_tls_retired_contexts (never free; ponytail bounded leak, one per reload).
- N1 process-mode invariance -> ctx build gated on `multithreaded`.
Plus check-threaded-ssl exposed client-cert (P5) + SNI (P6) regressions -> defer-with-
invariant pins: build_xtc_tls_context DECLINES (retire + disable -> OpenSSL) when
default_host==NULL, ssl_sni on, ssl_ca configured, or ssl_passphrase_command set.
P3 activation surface = server-auth-only TLS (SCRAM/password over TLS); client-cert,
SNI, encrypted keys stay pinned to OpenSSL-on-fiber until P5/P6/P7.

## Validation results
- **check-threaded-ssl surface** (001_ssltests + 002_scram + 004_sni):
  - PROCESS mode: PASS (byte-for-byte).
  - THREADED mt=on (thread-per-session): PASS.  Direct-SSL (001), SCRAM channel
    binding tls-server-end-point (002), SNI gate (004) all green on the fiber path.
- **Ad-hoc mt=on smoke** (earlier run): TLS query over sslmode=require; pg_stat_ssl
  shows t|TLSv1.3|TLS_AES_256_GCM_SHA384 (proves xtc getters, not OpenSSL); reconnect
  3/3 (R6 no-resumption); 8 concurrent TLS conns all correct; plain (sslmode=disable)
  falls through; bad-cert startup FATALs like stock; 0 crashes/asserts.
- **Process regress**: (see TLSREG marker) — must be 245/245, 0 diffs.

## Deferred (owned by later phases)
- P4: peer-cert introspection (subject/issuer/serial DN) — currently safe empties.
- P5: client-cert verify (verify_peer_mode + CA store + peer_cn/dn extraction) + ALPN
  detail; today ssl_ca configs pin to OpenSSL.
- P6: SNI (per-host ClientHello ctx selection); today ssl_sni pins to OpenSSL.
- P7: ssl_dh_params_file / openssl_tls_init_hook / encrypted-key passphrase fallbacks.
- Retired-ctx bounded leak -> refcount/epoch retirement if reloads become frequent.

## Re-review (SHIP-WITH-NITS, c3df51b5e7)
All 5 prior BLOCK findings verified fixed, no new defect, duplicate-body bug gone,
retire-list on all 3 paths in postmaster-durable context (bounded leak), pins match
the OpenSSL enable triggers.  Nit (documented in code + here, tracked P4/P5): RSA-PSS
server certs may diverge on the SCRAM cert-hash (PG uses X509_get_signature_info;
libxtc uses OBJ_find_sigid_algs) -> SCRAM-PLUS could fail for an RSA-PSS-signed server
cert on the xtc path.  RSA/ECDSA-SHA256 (common case + all TAP certs) unaffected.
Process regress 245/245 0-diffs (shared-file change does not regress the non-TLS path).

## P4+P5 (2026-08-27, origin/xtc 8f1d587adb)
- P4: peer-cert introspection getters (subject/issuer DN, serial) wired to xtc_tls_get_*.
- P5: client-cert verify (verify_peer_mode=REQUEST + ca_file) + peer_cn/dn/cert_valid
  extraction + per-connection ssl_loaded_verify_locations; CheckCertAuth core assert
  fixed (port->ssl -> port->ssl_in_use; the xtc path has no OpenSSL SSL*).
- check-threaded-ssl (001/002/004) showed the xtc client-cert path AUTHENTICATES
  correctly (revoked/untrusted/missing-intermediate all rejected, all outcome asserts
  pass), but 12 ': log matches' fail: the rich verify-FAILURE errdetail from PG's OpenSSL
  verify_cb is unavailable on the xtc path (libxtc gap #2, filed).  Decision: keep ssl_ca
  servers pinned to OpenSSL until the libxtc verify-detail hook lands, to avoid shipping
  degraded failure diagnostics.  P5 verify/extraction code kept + correct for un-pin.
- Result: process + threaded check-threaded-ssl (001/002/004) PASS; process regress
  245/245 0-diffs.  Bugs caught by the gate: (1) ssl_loaded_verify_locations set at
  init not per-conn (P5 no-op); (2) TRAP Assert(port->ssl) in CheckCertAuth (crash).

## TLS phase status
- P1 (dispatch), P2 (ctx build), P3 (transport/handshake/rw/close/ALPN/SCRAM cert-hash),
  P4 (introspection), P5 (client-cert verify code): LANDED.
- Active xtc surface: server-auth-only TLS (SCRAM/password over TLS).
- Pinned to OpenSSL-on-fiber (defer-with-invariant): ssl_ca/client-cert (P5, until
  libxtc verify-detail hook), ssl_sni (P6, until libxtc ClientHello ctx-swap #29),
  ssl_passphrase_command (P7), ssl_dh_params_file (P7), openssl_tls_init_hook (P7),
  RSA-PSS server cert SCRAM hash (until libxtc uses X509_get_signature_info).
- libxtc requests filed: plan_docs/.../LIBXTC_TLS_INTEGRATION_FINDINGS.md.
