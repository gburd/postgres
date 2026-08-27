# libxtc v1.38.0 + TLS P5 client-cert un-pin — validation (2026-08-27, mala account)

libxtc v1.37.0 -> v1.38.0 (rev f1a50cc9): TLS release fixing the two items that had
kept ssl_ca (client-cert auth) pinned to OpenSSL:
 - RSA-PSS channel-binding hash now uses X509_get_signature_info (matches PG).
 - new xtc_tls_get_verify_error(tls, &x509_err, buf, len) accessor.

P5 un-pin: build_xtc_tls_context no longer declines ssl_ca-configured servers; the
CA-load + verify_peer_mode=REQUEST + peer_cn/dn extraction + per-connection
ssl_loaded_verify_locations path now activates.  Handshake-failure ereport attaches
"Client certificate verification failed: <reason>" via xtc_tls_get_verify_error.

Two-review gate: adversarial security review = SHIP-WITH-NITS.  Independently verified
against libxtc v1.38 tls_openssl.c:
 - VERIFY_REQUEST = SSL_VERIFY_PEER without FAIL_IF_NO_PEER_CERT (no bypass; matches
   OpenSSL outcome).  peer_cert_valid only set when SSL_get_verify_result==X509_V_OK
   (stronger than the OpenSSL path).  Embedded-NUL rejected (CVE-2009-4034).  peer_dn
   RFC2253 (matches PG cert-map).
 - CRL genuinely enforced (X509_V_FLAG_CRL_CHECK|CRL_CHECK_ALL) -> revoked certs rejected.
 - RSA-PSS server-cert hash matches PG's be_tls_get_certificate_hash byte-for-byte ->
   no SCRAM-SHA-256-PLUS binding mismatch / downgrade.
 - errdetail can't crash; process mode + non-fiber threaded byte-for-byte OpenSSL.
Nits addressed: dropped inaccurate "at depth 0" (accessor has no depth); comment no
longer overstates CLIENT_ONCE parity.

Validated (mala c7i.4xlarge, cassert, libxtc v1.38):
 - process ssl TAP 001/002/004: PASS.
 - threaded 002_scram (SCRAM channel binding + client-cert PLUS): PASS -- RSA-PSS +
   verify fixes work.
 - threaded 001_ssltests: ALL auth OUTCOMES correct (revoked/untrusted/missing-
   intermediate rejected; 0 non-log-matches failures).  7 "log matches" assertions
   still red -- ONLY the failure-log second line ("Failed certificate data: subject/
   serial/issuer" naming the failing cert), which libxtc's result-only verify accessor
   cannot produce.  Filed as the residual libxtc gap (needs a verify CALLBACK).
   R4 reduced-detail, documented; connection fails closed.

Remaining TLS pins on the xtc path: ssl_sni (P6, libxtc ClientHello ctx-swap #29),
ssl_passphrase_command (P7).  First EC2 work on the mala account (724081032357).
