# P-A1 VALIDATED: the fiber execution path parks in place, no lease PANIC, 245/245 — plus a pre-existing fiber COPY bug found

Date: 2026-09-14. Validated by the PM directly (after the implementing agent was stopped mid-validation).
Branch `optionA-fibers`. libxtc v1.45.0 (`pkg-config --modversion xtc` = 1.45.0).

## The change validated
`966d05a186` — `PostgresRunSession` (postgres.c:7445) now branches on `PgRuntimeIsPooledProtocol`
instead of the blanket `PgRuntimeIsThreadBacked`, so the FIBER execution path falls through to plain
`PgSessionRun` (parking IN PLACE via `xtc_pg_wait_fd`) instead of entering the pooled stackless
scheduler staging (`PgSessionRunProtocolSchedulerStaging` -> `PgRuntimeProtocolSchedulerLeaseBackend`).
Per the two-model directive this is the SURVIVING threaded model's execution path (fiber-per-session on
the fixed `xtc_exec` loop pool), not a "thread-per-session" feature.

## Results (local, 8-core, libxtc v1.45.0, io_uring)
- **Builds clean** (meson: cassert + `-Dxtc=enabled` + `-Dplpython=disabled`; and autoconf).
- **Fiber model comes up as a FIXED thread pool**: `xtc: carrier scheduler thread up (8 loops, 8
  supervisors, ...)`, sessions launched as fibers (`xtc: client backend launched as xtc fiber`),
  `migratable=1` for client backends.
- **No lease PANIC at scale — the fix's purpose:** read-only pgbench at c=64 / 128 / 256 produced
  **0** occurrences of `could not lease protocol read park` or `PANIC` in the server log.
  tps: c=64 24,824 / c=128 20,764 / c=256 16,767 (declining with concurrency — a separate scaling
  issue, see below; the point here is no PANIC and sustained progress).
- **Process mode untouched: `make check` = ALL 245 TESTS PASSED (245/245), exit 0, 0 not-ok.**

## Pre-existing bug FOUND (not caused by this change): fiber-path COPY loses a client socket read
Large sustained COPY on the FIBER path intermittently fails:
```
LOG:  could not receive data from client: Connection timed out
CONTEXT:  COPY pgbench_accounts, line 1393717
LOG:  unexpected EOF within message length word
ERROR:  unexpected EOF on client connection with an open transaction
FATAL:  terminating connection because protocol synchronization was lost
```
client side: `pgbench: error: PQputline failed`. The server stays alive; the backend fiber exits 1.

**Attribution established by A/B, so this is NOT from the P-A1 change:**
| build | model | `pgbench -i -s 20` result |
|---|---|---|
| BASE `36bfae7ebe` (pre-fix) | fiber (carriers=0) | **2 of 4 FAILED** |
| FIXED `966d05a186` | fiber (carriers=0) | 1 of 2 failed |
| FIXED `966d05a186` | pooled (carriers=8) | **3 of 3 PASSED** |
Small COPYs (s=1, s=5) pass on the fiber path; it is scale/duration dependent (failed ~2s in, at row
~1.39M). So: a pre-existing, intermittent, FIBER-PATH-SPECIFIC loss of a streaming client socket read
under sustained COPY. The pooled path does not show it.

Note for the investigation: on the fiber path `WaitEventSetWait` parks on the **epoll fd** via
`xtc_pg_wait_fd(set->epoll_fd, WL_SOCKET_READABLE, cur_timeout)` (waiteventset.c:1483) and then
harvests with a non-blocking `epoll_wait` — an epoll-inside-io_uring nesting. A lost/short edge there, or
the `WL_TIMEOUT` return being treated as a hard timeout by the COPY read loop, are the first two
hypotheses. Use the v1.45.0 xtc_tail + xtc-rings/xtc-cqes per the xtc-runtime-debug rules before any
"lost wake" claim. This is now the top fiber-path correctness bug, because the fiber model is the
surviving threaded model.

## Also fixed here (pre-existing, unrelated): `2828e4414f`
`LoadedSSL` is defined under `#ifdef USE_SSL` but was used unguarded in `backend_startup_accepted`, so
`--without-ssl` builds failed to link (`undefined reference to LoadedSSL`). Guarded the use site; with
SSL compiled out the SSL-disabled pre-negotiation branch applies unconditionally for TCP clients.
Found while setting up the autoconf `make check` for this validation.

## Not done
- gdb backtrace explicitly showing a fiber parked inside `xtc_pg_wait_fd` (inferred from the model
  coming up as fibers + the no-PANIC result + the code path, but not captured).
- interrupt/cancel/NOTIFY/timeout regression matrix under the fiber model (the root-cause doc traced
  these as independent of the staging machinery — still unverified empirically).
- A regression test under `src/test/modules/test_backend_runtime`.
- The declining tps with concurrency (24.8k -> 16.8k from c=64 -> 256) is unexplained and is the
  fiber-model scaling question; separate from this fix.
