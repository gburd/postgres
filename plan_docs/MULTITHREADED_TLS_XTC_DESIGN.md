# Design: route backend TLS through libxtc `xtc_tls` on the fiber/carrier path

Status: DESIGN ONLY. No code changed. Read-only survey of the current
OpenSSL backend, `xtc_tls.h` (libxtc v1.37), and the carrier wait seam.

Scope: swap PostgreSQL's *backend* TLS (`be_tls_*`) from direct OpenSSL to
libxtc `xtc_tls` **only** when the backend runs as an xtc fiber
(`USE_XTC_CARRIER` build + `xtc_in_backend_fiber`). Process mode and every
non-fiber context keep the current OpenSSL path byte-for-byte.

Citations: `be-secure-openssl.c` and `be-secure.c` are under
`src/backend/libpq/`; `libpq-be.h` under `src/include/libpq/`; carrier seam in
`src/backend/postmaster/pg_xtc_carrier.c` and
`src/backend/storage/ipc/waiteventset.c`. `xtc_tls.h` line numbers are into
`/tmp/libxtc-137/src/inc/xtc_tls.h`.

---

## 0. Key finding up front (drives the whole design)

PG's `be_tls_read`/`be_tls_write` do **not** block on the socket themselves.
They return `-1`/`EWOULDBLOCK` with `*waitfor = WL_SOCKET_READABLE|WRITEABLE`
(be-secure-openssl.c:966, 974 for read; 1046, 1054 for write). The blocking
socket wait happens one level up, in `secure_read`/`secure_write`, via
`WaitEventSetWait(FeBeWaitSet, ...)` (be-secure.c:224, 356).

On a fiber, that `WaitEventSetWait` **already parks the carrier**:
`WaitEventSetWaitBlock` routes to `xtc_pg_wait_fd` when `xtc_in_backend_fiber`
(waiteventset.c:1454, 1483) which calls `xtc_proc_wait_fd` and yields the
fiber (pg_xtc_carrier.c:1301+). So today's OpenSSL-on-a-fiber path is *already*
non-blocking-and-fiber-yielding at the socket boundary.

Consequence: **xtc_tls's `XTC_E_AGAIN` + `xtc_tls_wants_read/_write` model is
the same shape as PG's `waitfor` model.** We do NOT want xtc_tls to park
internally; we want its would-block-and-retry mode, driven by PG's existing
`secure_read`/`secure_write` loop and the existing carrier-aware
`WaitEventSet`. This is the least-disruptive fit and reuses the park machinery
already proven for the OpenSSL path. See §3.

---

## (a) `be_tls_*` -> `xtc_tls_*` mapping table

| PG entry point (be-secure-openssl.c) | OpenSSL today | Port state touched | xtc_tls equivalent (xtc_tls.h) | Notes |
|---|---|---|---|---|
| `be_tls_init` (:143) | `SSL_CTX_new`, cert/key/CA/CRL load, cipher/DH/ECDH/proto min-max, disable tickets/reneg/compression, SNI hosts parse | module globals `SSL_context`, `SSL_hosts`, `ssl_loaded_verify_locations` | `xtc_tls_ctx_create(XTC_TLS_SERVER, &opts, &ctx)` (:230) per host config; `xtc_tls_ctx_set_sni_cb` (:319) once | Most GUC-driven knobs are now `xtc_tls_opts_t` fields (:200-238): `cert_file`,`key_file`,`ca_file`,`crl_file`,`crl_dir`,`cipher_list`,`ciphersuites_13`,`groups`,`min_version`,`max_version`,`prefer_server_ciphers`,`verify_peer_mode`,`passphrase_cb`. DH-params file and the `openssl_tls_init_hook` have NO xtc_tls surface — see risk register. |
| `be_tls_destroy` (:757) | `SSL_CTX_free(SSL_context)` | clears `SSL_context`, `ssl_loaded_verify_locations` | `xtc_tls_ctx_destroy(ctx)` (:246) for each ctx | Must free every per-host ctx (mirror `host_context_cleanup_cb` :742). |
| `be_tls_open_server` (:766) | `SSL_new`, `ssl_set_port_bio`, install info/alpn/clienthello cbs, `SSL_accept` retry loop, ALPN check, peer cert extract (CN/DN) | `port->ssl`, `port->peer`, `port->ssl_in_use`, `port->alpn_used`, `port->peer_cn/_dn`, `port->peer_cert_valid` | `xtc_tls_create` (:290) OR `xtc_tls_create_transport` (:400); loop `xtc_tls_handshake` (:470) until `XTC_OK`; then `xtc_tls_get_alpn_selected` (:565), `xtc_tls_has_peer_cert` (:571), `xtc_tls_get_peer_common_name`/`_subject_dn` (:580-582) | Handshake retry loop rewritten around `XTC_E_AGAIN`+`xtc_tls_wants_read/_write` (§b). Store the `xtc_tls_t*` in a new Port field (§e), not `port->ssl`. |
| `be_tls_close` (:731) | `SSL_shutdown`, `SSL_free`, `X509_free(peer)`, free cn/dn | `port->ssl`,`port->peer`,`port->peer_cn/_dn`,`port->ssl_in_use` | `xtc_tls_shutdown` (:520) then `xtc_tls_destroy` (:430) | `xtc_tls_shutdown` can return `XTC_E_AGAIN`; close path today calls `SSL_shutdown` once and ignores the result — keep that "best-effort, one shot" behavior (do not loop/park on close). |
| `be_tls_read` (:923) | `ERR_clear_error`,`SSL_read`,`SSL_get_error` -> map WANT_READ/WRITE to `*waitfor`, else errno | reads `port->ssl` | `xtc_tls_read(tls, ptr, len, &n)` (:508); map `XTC_E_AGAIN`+`wants_read/_write` -> `*waitfor` | Full rewrite in §b. Drop the `XtcPgNoStealEnter/Leave` bracket (:1249) — see §e/§3. |
| `be_tls_write` (:1000) | `ERR_clear_error`,`SSL_write`,`SSL_get_error` -> `*waitfor`/errno | reads `port->ssl` | `xtc_tls_write(tls, ptr, len, &n)` (:517) | Full rewrite in §b. |
| `be_tls_get_cipher_bits` (:2210) | `SSL_get_cipher_bits` | `port->ssl` | `xtc_tls_get_cipher_bits` (:558) | direct. |
| `be_tls_get_version` (:2222) | `SSL_get_version` | `port->ssl` | `xtc_tls_get_version` (:552) | direct (both return e.g. "TLSv1.3"). |
| `be_tls_get_cipher` (:2231) | `SSL_get_cipher` | `port->ssl` | `xtc_tls_get_cipher` (:555) | direct. |
| `be_tls_get_peer_subject_name` (:2240) | `X509_NAME_to_cstring(X509_get_subject_name(peer))` | `port->peer` | `xtc_tls_get_peer_subject_dn(tls, buf, len)` (:580) | **Format risk**: PG's `X509_NAME_to_cstring` emits `/CN=.../O=...` slash form; xtc_tls returns RFC2253 (`CN=...,O=...`). See risk register. |
| `be_tls_get_peer_issuer_name` (:2249) | `X509_NAME_to_cstring(X509_get_issuer_name(peer))` | `port->peer` | `xtc_tls_get_peer_issuer_dn(tls, buf, len)` (:582) | same format risk. |
| `be_tls_get_peer_serial` (:2258) | `X509_get_serialNumber`->`BN`->decimal | `port->peer` | `xtc_tls_get_peer_serial(tls, buf, len)` (:585) | decimal string both sides. |
| `be_tls_get_certificate_hash` (:2278) | RFC5929 sig-algo-aware digest of *server* cert | `port->ssl` | `xtc_tls_get_server_cert_hash(tls, buf, buflen, &out_len)` (:600) | xtc_tls documents the exact RFC5929 MD5/SHA1->SHA256 rule (:594-600). PG returns palloc'd buffer; adapter copies from stack buf. |
| internal `port_bio_*`/`ssl_set_port_bio` (:1140-1240) | custom BIO over `secure_raw_read/write` | — | replaced by either fd path or `xtc_tls_transport_t` (:390) | see §3 recommendation. |
| internal `sni_clienthello_cb`/`ssl_update_ssl` (:1870-2100) | ClientHello ctx swap | `SSL_hosts` | `xtc_tls_sni_cb_t` returning a pre-created SERVER ctx (:305) | see §d. |
| internal `alpn_cb` (:1834) | `SSL_select_next_proto` on `PG_ALPN_PROTOCOL_VECTOR` | `port->alpn_used` | `opts.alpn_protos` (wire form) (:180) + `xtc_tls_get_alpn_selected` | see §d. |
| internal `verify_cb` (:1600) | logs cert-verify failures, stashes `cert_errdetail` | `SSL ex_data` | **no xtc_tls hook** | see risk register (error-detail parity). |
| internal `info_cb` (:1779) | DEBUG4 handshake tracing | — | none | drop; DEBUG-only. |
| `be_gssapi_*` | (separate, GSS) | — | out of scope | GSS is a different secure transport, untouched. |

`xtc_tls_set_hostname` (:445) is client-side only; backend (server role) never
calls it.

---

## (b) read/write retry-vs-park reconciliation + concrete rewrite

### The two models

- **PG waitfor protocol**: `be_tls_read` returns `-1`, sets `errno=EWOULDBLOCK`
  and `*waitfor = WL_SOCKET_READABLE|WRITEABLE`; `secure_read` then blocks on
  `WaitEventSetWait(FeBeWaitSet, ...)` and retries (be-secure.c:191-296).
- **xtc_tls model**: `xtc_tls_read` returns `XTC_E_AGAIN` with `*out_n = 0`;
  `xtc_tls_wants_read/_write` tell the direction; caller arms the watch and
  retries (xtc_tls.h:497-506, 528-547).

These are **the same protocol** with different spellings. xtc_tls does NOT
park internally in this mode — it is the async/non-blocking event-loop
discipline the header describes (xtc_tls.h header comment lines 25-47). So we
keep PG's `secure_read`/`secure_write` loop unchanged and let it drive
`XTC_E_AGAIN`. On a fiber, the `WaitEventSetWait` inside that loop already
parks the carrier (waiteventset.c:1454 -> xtc_pg_wait_fd) — **the fiber-yield
comes for free from the existing seam**; we do not add a park to `be_tls_*`.

### Recommendation

**(a)-not, (b)-yes**: use xtc_tls's would-block/retry mode and let PG's existing
`waitfor` loop drive it. Do NOT rely on internal parking. Rationale:

1. Zero change to `secure_read`/`secure_write` (be-secure.c) and to
   `FeBeWaitSet` — the highest-traffic, most-invariant-laden code.
2. The carrier park at `WaitEventSetWait` is already the audited, tripwire-
   guarded yield boundary (`xtc_pg_affine_section_depth()==0`,
   pg_xtc_carrier.c:1327). Adding a second park inside `xtc_tls_read` would
   create an *unaudited* yield point mid-`be_tls_read`, which the no-steal
   bracketing (§e) was specifically built to forbid.
3. Interrupt handling (latch, postmaster death, `interrupt_wake_fd` drain at
   be-secure.c:262-286) stays in the one place that already handles it.

### Concrete rewrite of `be_tls_read`

Dispatch (§e) picks this body only on the fiber path; OpenSSL body retained
otherwise.

```c
ssize_t
be_tls_read(Port *port, void *ptr, size_t len, int *waitfor)
{
    size_t   n = 0;
    int      rc;
    xtc_tls_t *tls = port->xtc_tls;   /* new Port field, §e */

    errno = 0;
    rc = xtc_tls_read(tls, ptr, len, &n);
    switch (rc)
    {
        case XTC_OK:
            /* n==0 here is a clean peer close (close_notify) -> return 0 */
            return (ssize_t) n;
        case XTC_E_AGAIN:
            *waitfor = xtc_tls_wants_write(tls) ? WL_SOCKET_WRITEABLE
                                                : WL_SOCKET_READABLE;
            errno = EWOULDBLOCK;
            return -1;
        default:
            /* hard error: report like SSL_ERROR_SSL/SYSCALL do today */
            ereport(COMMERROR,
                    (errcode(ERRCODE_PROTOCOL_VIOLATION),
                     errmsg("TLS read error: %s", xtc_strerror(rc))));
            errno = ECONNRESET;
            return -1;
    }
}
```

Notes:
- No `ERR_clear_error`/`SSL_get_error` — xtc_tls owns the backend error state.
- No `XtcPgNoStealEnter/Leave` bracket: that bracket exists only because
  OpenSSL's error queue is per-OS-thread (be-secure-openssl.c:1247). xtc_tls's
  own I/O is designed for the single-threaded loop discipline; the affine-span
  concern moves inside libxtc. **Confirm with libxtc** that `xtc_tls_read`
  performs no OS-thread-affine work the fiber could be stolen away from
  (it should not, per the header's async discipline) — risk register item.
- Clean close: today `SSL_ERROR_ZERO_RETURN` maps to `n=0` (be-secure-openssl.c
  :988). With xtc_tls, `XTC_OK` with `*out_n==0` is the clean EOF — same
  `return 0`. Verify libxtc does not instead surface EOF as a distinct code.

### Concrete rewrite of `be_tls_write`

```c
ssize_t
be_tls_write(Port *port, const void *ptr, size_t len, int *waitfor)
{
    size_t   n = 0;
    int      rc;
    xtc_tls_t *tls = port->xtc_tls;

    errno = 0;
    rc = xtc_tls_write(tls, ptr, len, &n);
    switch (rc)
    {
        case XTC_OK:
            return (ssize_t) n;          /* short write is fine */
        case XTC_E_AGAIN:
            *waitfor = xtc_tls_wants_read(tls) ? WL_SOCKET_READABLE
                                               : WL_SOCKET_WRITEABLE;
            errno = EWOULDBLOCK;
            return -1;
        default:
            ereport(COMMERROR,
                    (errcode(ERRCODE_PROTOCOL_VIOLATION),
                     errmsg("TLS write error: %s", xtc_strerror(rc))));
            errno = ECONNRESET;
            return -1;
    }
}
```

The cross-direction cases (`SSL_read` wanting write, `SSL_write` wanting read)
that PG handles today (be-secure-openssl.c:966-974, 1046-1054) are preserved by
consulting `xtc_tls_wants_read/_write` rather than assuming the direction.

### Handshake loop in `be_tls_open_server` (fiber path)

Mirror the existing `aloop:` (be-secure-openssl.c:895) but around
`xtc_tls_handshake`, reusing PG's `WaitLatchOrSocket` (as today at :899) so the
fiber parks through the same seam:

```c
for (;;)
{
    int rc = xtc_tls_handshake(port->xtc_tls);
    if (rc == XTC_OK) break;
    if (rc != XTC_E_AGAIN) { /* ereport COMMERROR + return -1 */ }
    int waitfor = xtc_tls_wants_write(port->xtc_tls)
                    ? WL_SOCKET_WRITEABLE : WL_SOCKET_READABLE;
    (void) WaitLatchOrSocket(NULL, waitfor | WL_EXIT_ON_PM_DEATH,
                             port->sock, 0, WAIT_EVENT_SSL_OPEN_SERVER);
}
```

---

## (c)/(d) SNI + cert/key/CA/CRL + client-cert verify + ALPN mapping

### SNI (the crux the two new hooks unblock)

Today (OpenSSL): one shared `SSL_context`, `SSL_CTX_set_client_hello_cb` ->
`sni_clienthello_cb` (be-secure-openssl.c:840, 1930) parses the ClientHello
server_name, matches against `SSL_hosts->sni` / `default_host` / `no_sni`, then
`ssl_update_ssl` swaps cert/key/CA into the live `SSL` (be-secure-openssl.c
:1870). LibreSSL (no clienthello cb) falls back to a single context.

Map to xtc_tls:
1. `be_tls_init` builds one `xtc_tls_ctx_t*` **per host config** via
   `xtc_tls_ctx_create(XTC_TLS_SERVER, opts, &ctx)` (each with its own
   cert/key/ca/crl/verify_mode), replacing the per-host `SSL_CTX` in
   `HostsLine->ssl_ctx`. Keep the same `SSL_hosts` parse/match structures;
   only the stored context type changes.
2. Register `xtc_tls_ctx_set_sni_cb(base_ctx, pg_sni_cb, NULL)`
   (xtc_tls.h:319). The callback signature is
   `xtc_tls_ctx_t *(*)(xtc_tls_t*, const char *server_name, void*)`
   (xtc_tls.h:305). Its body is the *match half* of today's
   `sni_clienthello_cb`: given `server_name`, walk `SSL_hosts->sni`
   (case-insensitive, matching be-secure-openssl.c:2050), fall to
   `default_host`/`no_sni`, and **return the matching pre-created
   `xtc_tls_ctx_t*`** instead of calling `ssl_update_ssl`. The whole
   `ssl_update_ssl` cert/key/CA-copy dance disappears — context swap is native
   (xtc_tls.h:300-303 "exactly OpenSSL's SSL_set_SSL_CTX shape").
3. `ssl_loaded_verify_locations`: today set true inside the swap when a CA is
   present (be-secure-openssl.c:2040). In the new model, set it from whether the
   *selected* ctx was created with a CA — either track a per-ctx bool in the
   `HostsLine`, or set it in `pg_sni_cb` after selection.

**Migration invariant preserved.** The existing tripwire
`Assert(!(xtc_in_backend_fiber && ssl_sni && ...is_migratable()))`
(be-secure-openssl.c:882) documented that OpenSSL could not swap context mid-
handshake on a migratable fiber. `xtc_tls_ctx_set_sni_cb` now *can* swap the
context, and its contract says the callback "runs on the carrier driving the
handshake ... must not block" (xtc_tls.h:315-316) — i.e. no yield across the
swap. So the invariant can be **relaxed** for the xtc_tls path (SNI + migratable
becomes allowed), but that relaxation is a *separate, later* change gated on
verifying the callback truly runs yield-free on the driving carrier. For the
initial swap, keep the assert (SNI backends stay pinned) — do not widen scope.

### cert/key/CA/CRL loading (GUC-driven)

All fold into `xtc_tls_opts_t` at ctx-create time (xtc_tls.h:200-238):

| GUC (be-secure.c) | today (be-secure-openssl.c) | xtc_tls_opts field |
|---|---|---|
| `ssl_cert_file` / per-host `ssl_cert` | `SSL_CTX_use_certificate_chain_file` (:558) | `cert_file` |
| `ssl_key_file` / `ssl_key` | `SSL_CTX_use_PrivateKey_file` + check (:583,600) | `key_file` |
| `ssl_ca_file` / `ssl_ca` | `SSL_CTX_load_verify_locations` + client CA list (:634) | `ca_file` |
| `ssl_crl_file` | `X509_STORE_load_locations` + CRL flags (:668) | `crl_file` |
| `ssl_crl_dir` | same (:668) | `crl_dir` |
| `SSLCipherList` (TLSv1.2) | `SSL_CTX_set_cipher_list` (:718) | `cipher_list` |
| `SSLCipherSuites` (TLSv1.3) | `SSL_CTX_set_ciphersuites` (:730) | `ciphersuites_13` |
| `SSLECDHCurve` | `SSL_CTX_set1_groups_list` (:2160) | `groups` |
| `ssl_min_protocol_version` | `SSL_CTX_set_min_proto_version` (:502) | `min_version` (map PG enum -> `XTC_TLS_VER_12/13`) |
| `ssl_max_protocol_version` | `SSL_CTX_set_max_proto_version` (:525) | `max_version` |
| `SSLPreferServerCiphers` | `SSL_OP_CIPHER_SERVER_PREFERENCE` (:740) | `prefer_server_ciphers` |
| `ssl_passphrase_command` (+reload) | `ssl_external_passwd_cb`/`dummy_ssl_passwd_cb` (:410,522) | `passphrase_cb`+`passphrase_userdata` (wrap `run_ssl_passphrase_command`; signature drops `rwflag`, xtc_tls.h:157) |
| `ssl_dh_params_file` | `initialize_dh`/`load_dh_file` (:2118) | **NO xtc_tls field** — risk register |

Server hardening PG does explicitly — no tickets, no session cache, no
compression, no renegotiation, moving-write-buffer (be-secure-openssl.c
:640-712) — is documented as an **unconditional default** inside xtc_tls
(xtc_tls.h:238-244). So those calls simply drop; verify the defaults match.

### client cert verification mode

PG wants "request a client cert but don't fail if absent"
(`SSL_VERIFY_PEER | SSL_VERIFY_CLIENT_ONCE`, be-secure-openssl.c:857, 2032) —
the decision to require it is made later per hba line. That is exactly
`XTC_TLS_VERIFY_REQUEST` (xtc_tls.h:141: "request; accept a handshake with
none"). Set `opts.verify_peer_mode = XTC_TLS_VERIFY_REQUEST` when a CA is
configured; leave `XTC_TLS_VERIFY_DEFAULT`/none otherwise. Post-handshake,
`port->peer_cert_valid` derives from `xtc_tls_has_peer_cert` (:571).

### ALPN ('postgresql')

Today: `SSL_CTX_set_alpn_select_cb` -> `alpn_cb` selecting
`PG_ALPN_PROTOCOL_VECTOR` (be-secure-openssl.c:828, 1834), post-accept checked
against `PG_ALPN_PROTOCOL` (:1000-region ALPN block). Map to
`opts.alpn_protos` in wire form (xtc_tls.h:178-181 shows `"\x02h2..."` form) —
build the wire string from `PG_ALPN_PROTOCOL`. After handshake, call
`xtc_tls_get_alpn_selected` (:565) and set `port->alpn_used` iff it equals
`PG_ALPN_PROTOCOL`. The "reject non-postgresql ALPN with fatal alert" behavior
should be enforced by only advertising `postgresql`; verify xtc_tls sends the
`no_application_protocol` alert on mismatch (it should, standard ALPN).

---

## (e) process-mode gating design — thin dispatch, one Port field

**Invariant**: `multithreaded=off` and any non-fiber context stay byte-for-byte
OpenSSL. The gate is the same one the rest of the runtime uses:
`#ifdef USE_XTC_CARRIER` (build) AND `xtc_in_backend_fiber` (runtime,
pg_xtc_carrier.c:317).

Recommended shape: **thin runtime dispatch inside `be-secure-openssl.c`**, not
a whole second translation unit. The OpenSSL functions stay; each of the six
hot/lifecycle entry points gets a one-line head check:

```c
ssize_t
be_tls_read(Port *port, void *ptr, size_t len, int *waitfor)
{
#ifdef USE_XTC_CARRIER
    if (port->xtc_tls != NULL)          /* set only on the fiber path */
        return be_tls_read_xtc(port, ptr, len, waitfor);
#endif
    ... existing OpenSSL body ...
}
```

Why `port->xtc_tls != NULL` and not `xtc_in_backend_fiber` at the read site:
the fiber that opened the connection is the only one that reads/writes it, and
`be_tls_open_server` decides once (under `xtc_in_backend_fiber`) which stack to
use and records it by setting either `port->ssl` (OpenSSL) or `port->xtc_tls`
(xtc). Reads/writes then dispatch on which pointer is non-NULL — robust even if
the runtime flag were ever transiently wrong, and it keeps the per-call check a
single pointer test.

Port changes (libpq-be.h, in the existing SSL block ~:298):
- add `xtc_tls_t *xtc_tls;` alongside `SSL *ssl;` (guarded so process/no-SSL
  builds keep offsets — mirror the existing `#ifdef USE_OPENSSL ... #else void*`
  pattern at libpq-be.h:300-306; use a `void *xtc_tls` placeholder when
  `!USE_XTC_CARRIER`).
- peer cert introspection: today reads `port->peer` (an `X509*`). On the xtc
  path there is no `X509*`; the getters instead call `xtc_tls_get_peer_*`.
  Store nothing extra — `be_tls_get_*` dispatch on `port->xtc_tls != NULL` the
  same way, calling the xtc getter (which reads from the `xtc_tls_t`). `peer_cn`
  / `peer_dn` / `peer_cert_valid` continue to be filled in
  `be_tls_open_server` (from `xtc_tls_get_peer_common_name` etc.) so downstream
  auth code (`hba`, SCRAM) is unchanged.

Decision point (once) in `be_tls_open_server` head:

```c
#ifdef USE_XTC_CARRIER
    if (xtc_in_backend_fiber && be_tls_xtc_available())
        return be_tls_open_server_xtc(port);   /* sets port->xtc_tls */
#endif
    ... existing OpenSSL body, sets port->ssl ...
```

`be_tls_xtc_available()` returns whether an `xtc_tls_ctx_t` was successfully
built at init (i.e. libxtc was compiled `--with-tls` != none; `xtc_tls_*`
returns `XTC_E_NOSYS` otherwise, xtc_tls.h header lines 49-56). If xtc TLS is
unavailable, fall through to OpenSSL even on a fiber — pinned, as today.

`be_tls_init` must build **both** the OpenSSL contexts (for the non-fiber /
fallback path) and, when available, the xtc_tls contexts. Or, lazier: build
OpenSSL contexts always; build xtc contexts on first fiber use. Prefer building
both at init so a config error is caught at server start (matches current
FATAL-at-start behavior). This is the one real cost of dual-stack: init does
two loads. Acceptable; the load is startup/reload-only.

Net: **two implementations of the six functions, one dispatch, shared GUC
parsing and `SSL_hosts` matching structures.** No change to `be-secure.c`, to
`FeBeWaitSet`, or to callers.

---

## (f) risk register

| # | Risk | Why | Mitigation / gate |
|---|---|---|---|
| R1 | **DH params file** (`ssl_dh_params_file`) has no `xtc_tls_opts` field | PG loads custom DH params (be-secure-openssl.c:2091-2160); xtc_tls only exposes `groups` | Defer-with-invariant: TLS1.3 (default floor is 1.2) uses ECDHE `groups`, not finite-field DH; DH-params only matter for legacy TLS1.2 FFDHE. Guard: if `ssl_dh_params_file` is set AND fiber path chosen, either fall back to OpenSSL (pin) or emit a clear "ssl_dh_params_file unsupported under threaded TLS" error. Later phase: request an xtc_tls dh-params knob. |
| R2 | **`openssl_tls_init_hook`** (extension-installed) not representable | It takes a raw `SSL_CTX*` (libpq-be.h:377); xtc_tls has no `SSL_CTX` to hand out | Same as today's SNI case, which already *warns* the hook is ignored (be-secure-openssl.c:475). On the xtc path, if a non-default hook is installed, fall back to OpenSSL (pin the fiber) so the hook still runs. Documented process-only-extension analogue (Phase 19 spirit). |
| R3 | **Peer DN/subject format** differs | PG `X509_NAME_to_cstring` = `/CN=x/O=y` slash form (be-secure-openssl.c:2380); `xtc_tls_get_peer_*` = RFC2253 `CN=x,O=y` | `port->peer_dn` is *already* RFC2253 in `be_tls_open_server` (:1050 uses `XN_FLAG_RFC2253`) — so `peer_dn` matches. But `be_tls_get_peer_subject_name`/`_issuer_name` (log_line_prefix `%s`, `sslinfo`) use the *slash* form. Parity risk for logs/`sslinfo`. Gate: TAP + a `sslinfo` regression comparing both stacks; if xtc gives RFC2253, either accept the log format change (document) or post-process. |
| R4 | **cert-verify error detail** (`verify_cb` -> `cert_errdetail`, be-secure-openssl.c:1600-1700, surfaced at :955) has no xtc hook | The rich "verification failed at depth N: ... subject/serial/issuer" errdetail is OpenSSL-callback-driven | Accept reduced detail on the xtc path initially (handshake still fails correctly; just less errdetail). Gate: TAP checks the *failure* happens, not the exact detail string. Later: request a verify-callback hook from libxtc if detail parity matters. |
| R5 | **Handshake cancellation / auth timeout** | Today `SSL_accept` loop asserts `!port->noblock` and relies on `StartupPacketTimeoutHandler` (be-secure-openssl.c:906-915) | The xtc handshake loop uses the same `WaitLatchOrSocket` + `WL_EXIT_ON_PM_DEATH`, so the same timeout/PM-death path applies. Verify a mid-handshake interrupt (backend cancel) unwinds cleanly: `xtc_tls_destroy`+`xtc_tls_ctx` untouched, fd owned by Port. |
| R6 | **Session resumption / tickets** | PG explicitly disables tickets + session cache (be-secure-openssl.c:640-655) | xtc_tls disables both by default (xtc_tls.h:238-244). Confirm no resumption is offered; TAP: a reconnect must full-handshake. Low risk (both off). |
| R7 | **Renegotiation** | PG sets `SSL_OP_NO_RENEGOTIATION` (be-secure-openssl.c:700) | xtc_tls disables renegotiation by default (xtc_tls.h:239). TLS1.3 has none anyway. Confirm TLS1.2 path also refuses client-initiated reneg. |
| R8 | **No-steal bracket removal** | be_tls_read/write today wrap OpenSSL in `XtcPgNoStealEnter/Leave` because OpenSSL's error queue is per-OS-thread (be-secure-openssl.c:1247,1315) | Removing it on the xtc path is only safe if `xtc_tls_read/write` do no OS-thread-affine work across an internal yield. Per the header they run in the loop's non-blocking discipline (no internal park in AGAIN mode). **Gate: confirm with libxtc that xtc_tls_read/write are yield-free** (they return AGAIN rather than parking); the `xtc_pg_affine_section_depth()==0` tripwire (pg_xtc_carrier.c:1327) still guards the outer park. |
| R9 | **Introspection getter gaps / buffer sizing** | xtc getters are buffer-filling with `XTC_E_RANGE` (xtc_tls.h:588); PG getters `strlcpy` into caller buf | Size buffers to `NAMEDATALEN`-ish / existing PG limits; handle `XTC_E_RANGE` by truncation matching current `strlcpy` behavior. `be_tls_get_certificate_hash` must copy the stack buffer into a palloc'd result (existing contract). |
| R10 | **Dual-stack config divergence** | init builds OpenSSL AND xtc contexts from the same GUCs | A mapping bug could make the two stacks accept different connections. Gate: run the full `src/test/ssl` TAP suite under both `multithreaded=off` (OpenSSL) and `on` (xtc) and diff behavior. This is the primary correctness gate. |
| R11 | **`raw_buf` pushback** (buffered pre-TLS bytes, be-secure.c:118-135, libpq-be.h:308) | On SSLRequest, bytes already read must feed the TLS engine before the socket | The fd path (`xtc_tls_create`) reads straight from the fd and cannot see `raw_buf`. This **forces the custom-transport path** (`xtc_tls_create_transport`) whose `read_cb` drains `raw_buf` first via `secure_raw_read` — exactly what `port_bio_read` does today (be-secure-openssl.c:1147). See §3 note below. |

### §3 fd-path vs custom-transport recommendation

**Recommend `xtc_tls_create_transport`** (xtc_tls.h:400), not the fd path,
because:
- PG already funnels all socket I/O through `secure_raw_read`/`secure_raw_write`
  (be-secure.c:301, 388), and the OpenSSL backend already wraps them in a custom
  BIO (`port_bio_read/write`, be-secure-openssl.c:1147, 1177).
- `raw_buf` pushback (R11) requires the TLS engine to consume already-read bytes
  first — only the transport callbacks can do that.
- The transport `read_cb`/`write_cb` contract (`XTC_E_AGAIN` = would-block,
  `0` = EOF, xtc_tls.h:378-386) maps 1:1 onto `secure_raw_read`'s
  `recv`-returns-`EAGAIN`/`0` behavior. The callbacks become thin wrappers:

```c
static int pg_tls_read_cb(void *ud, void *buf, size_t len) {
    Port *port = ud;
    ssize_t r = secure_raw_read(port, buf, len);   /* drains raw_buf then recv */
    if (r > 0) return (int) r;
    if (r == 0) return 0;                            /* EOF */
    if (errno == EWOULDBLOCK || errno == EAGAIN || errno == EINTR)
        return XTC_E_AGAIN;
    return XTC_E_IO;                                 /* hard */
}
```

This keeps the recv/send in PG's hands (Win32 signal handling, interrupt
windows, `raw_buf`) exactly as the BIO does today, while the TLS state machine
moves to xtc_tls. The fd path would lose `raw_buf` and duplicate the socket
handling.

---

## Phased implementation plan (what to validate at each step)

Each phase A/B-measured on `check-threaded-pooled`; process mode stays green
throughout (`gmake check`).

- **P0 — verify libxtc contract (no PG code).** Confirm with libxtc: (i)
  `xtc_tls_read/write` are yield-free in AGAIN mode (R8); (ii) `XTC_OK`+`n==0`
  is clean EOF (§b); (iii) transport `read_cb` returning `XTC_E_AGAIN` is
  retried correctly; (iv) SNI callback runs on the driving carrier yield-free
  (SNI-migration relaxation, §d). Gate: written answers; no code.

- **P1 — Port field + dispatch skeleton, OpenSSL still used.** Add
  `port->xtc_tls` (libpq-be.h) and the `#ifdef USE_XTC_CARRIER` head checks in
  the six entry points, all still falling through to OpenSSL (xtc branch
  unreachable). Gate: `gmake check`, `gmake check-threaded` unchanged; offsets
  stable.

- **P2 — xtc context build in `be_tls_init` (server ctx only, no SNI).**
  Build `xtc_tls_ctx_t` from the `default_host` GUCs alongside OpenSSL.
  `be_tls_xtc_available()` returns true. Gate: init succeeds/FATALs identically
  for good/bad certs; no connection uses it yet.

- **P3 — `be_tls_open_server_xtc` + read/write/close on the transport path.**
  Wire `xtc_tls_create_transport`, the handshake loop, the read/write rewrites
  (§b), close. Gate: `src/test/ssl` TAP under `multithreaded=on` — connect,
  simple query, disconnect over TLS on a fiber; A/B latency vs OpenSSL-on-fiber.
  Validate R5 (cancel mid-handshake), R6/R7 (no resumption/reneg).

- **P4 — introspection getters on the xtc path.** version/cipher/bits, peer
  subject/issuer/serial/CN, cert hash (SCRAM channel binding). Gate: `sslinfo`
  regression + SCRAM-PLUS TAP under both stacks; confront R3 (DN format) and
  R9 (buffer sizing) explicitly; document any log-format change.

- **P5 — client cert verify (`VERIFY_REQUEST`) + ALPN.** Gate: cert-auth hba
  TAP (present/absent/invalid client cert), ALPN negotiation test; R4
  (errdetail parity) assessed — accept reduced detail with a TODO if needed.

- **P6 — SNI via `xtc_tls_ctx_set_sni_cb`.** Per-host ctx build + selection
  callback (§d). Keep the pin (assert at be-secure-openssl.c:882 stays). Gate:
  the SNI TAP (`001_ssltests`-style multi-cert) under `multithreaded=on`,
  pinned.

- **P7 — fallback wiring for unsupported knobs.** DH-params (R1) and
  `openssl_tls_init_hook` (R2): detect and pin-to-OpenSSL with a clear log.
  Gate: a config with `ssl_dh_params_file` / a hook still works on a fiber (via
  OpenSSL fallback), and a config without them uses xtc.

- **P8 (later, separate) — relax SNI no-migrate.** Only after P0(iv) proven:
  allow `ssl_sni` + migratable on the xtc path, widening the assert at
  be-secure-openssl.c:882. Its own A/B + forced-migration stress gate.

## P0-gate confirmation (from libxtc v1.37 source, 2026-08-26)

Both P0 gates CONFIRMED from libxtc source (/tmp/libxtc-137, tls_openssl.c) --
the design's retry-mode assumption is correct; no rewrite needed.

- **P0-gate-1 (yield-free AGAIN mode): YES.** xtc_tls_read (tls_openssl.c:896-927)
  and xtc_tls_write (:931-958) are a single SSL_read_ex/SSL_write_ex + switch on
  SSL_get_error: WANT_READ/WANT_WRITE -> set wants_read/wants_write and return
  XTC_E_AGAIN immediately.  xtc_tls_handshake/_shutdown same shape.  A whole-backend
  grep for xtc_io_*/xtc_proc*/xtc_yield/poll/select/epoll/O_NONBLOCK = ZERO matches
  -- there is no internal park; readiness is entirely the caller's (PG's
  secure_read/write waitfor loop, which parks the carrier via xtc_pg_wait_fd).
- **P0-gate-2 (clean EOF): XTC_OK with *out_n == 0.** SSL_ERROR_ZERO_RETURN
  (close_notify) -> *out_n=0; return XTC_OK (tls_openssl.c:921-924).  No distinct
  XTC_E_EOF.  Abrupt mid-record FIN -> XTC_E_INTERNAL (== ECONNRESET, same as
  OpenSSL today).
- **Custom-transport would-block propagation: clean.** transport_bio_read
  (:304-323): rc==XTC_E_AGAIN -> BIO_set_retry_read; return -1 -> SSL_ERROR_WANT_READ
  -> XTC_E_AGAIN up to the caller; write symmetric.  A non-blocking read_cb/write_cb
  returning XTC_E_AGAIN propagates as AGAIN, no internal retry/park.
  xtc_tls_create_transport requires both callbacks non-NULL (:784-786).
- **Model: retry mode is the ONLY model libxtc v1.37 offers** (no hook from its TLS
  layer to PG's carrier for internal-park) -- the design's be_tls_read/write bodies
  match the source one-for-one.  PROCEED to P1 implementation.

## P1 landed-to-branch + P2-ready API map (2026-08-26, libxtc v1.37)

P1 (Port field + dispatch skeleton) is IMPLEMENTED on branch `tls-xtc-p1` and
builds clean locally (full ninja) with USE_XTC_CARRIER.  It is deliberately a
NO-OP: `be_tls_xtc_available()` returns false -> `be_tls_open_server` never takes
the xtc branch -> `port->xtc_tls` stays NULL -> all 10 dispatch head checks
(open/read/write/close + 6 getters, +1 decision point) are dead -> OpenSSL path
byte-for-byte.  The `void *xtc_tls` Port field is unconditional so struct offsets
match across process/threaded builds.  The 12 `*_xtc` bodies are `elog(FATAL)`
stubs (unreachable).  NOT landed to xtc: dead scaffolding earns its place only
once P2/P3 make it live + validated (don't ship FATAL stubs to trunk).

Exact v1.37 xtc_tls server API (confirmed from /tmp/libxtc-137/src/inc/xtc_tls.h),
the ground P2/P3 build on:
- ctx: `int xtc_tls_ctx_create(xtc_tls_role_t, const xtc_tls_opts_t *, xtc_tls_ctx_t **)`;
  `void xtc_tls_ctx_destroy(xtc_tls_ctx_t *)`; `xtc_tls_ctx_set_sni_cb` (P6).
- `xtc_tls_opts_t` -> PG GUC map (a zeroed struct = old default behavior):
  cert_file<-ssl_cert_file, key_file<-ssl_key_file, ca_file<-ssl_ca_file,
  crl_file/crl_dir<-ssl_crl_file/ssl_crl_dir, cipher_list<-ssl_ciphers,
  ciphersuites_13<-ssl_tls13_ciphers, groups<-ssl_groups (or ssl_ecdh_curve),
  min_version/max_version<-ssl_min/max_protocol_version, prefer_server_ciphers
  <-ssl_prefer_server_ciphers, verify_peer_mode (P5), alpn_protos (P5, "postgresql"),
  passphrase_cb/_userdata<-ssl_passphrase_command (R-fallback if hook-based).
- transport/handshake (P3): `xtc_tls_create` + `xtc_tls_create_transport` (custom
  transport over the PG socket), `xtc_tls_set_hostname`, `xtc_tls_handshake`
  (retry-mode AGAIN -> park via secure_open loop), `xtc_tls_read/write/shutdown`,
  `xtc_tls_wants_read/write`, `xtc_tls_clear_wants`, `xtc_tls_destroy`.
- introspection (P4): `xtc_tls_get_version/cipher/cipher_bits`,
  `xtc_tls_get_peer_common_name/subject_dn/issuer_dn/serial`,
  `xtc_tls_get_server_cert_hash` (SCRAM channel binding -- confront R3 DN-format
  + R9 buffer-size here), `xtc_tls_has_peer_cert`, `xtc_tls_get_alpn_selected`.

P2 START: build the server `xtc_tls_ctx_t` in `be_tls_init` from default_host GUCs
alongside SSL_context; flip `be_tls_xtc_available()` to return `ctx != NULL`.  Gate:
init succeeds/FATALs identically for good/bad certs; no connection uses it yet
(open still needs P3).  Needs an ssl-cert EC2 box (src/test/ssl infra) to validate.
