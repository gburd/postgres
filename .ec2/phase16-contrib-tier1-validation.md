# Phase 16 contrib Tier-1 markers — validation GREEN (2026-08-25, chiuso c7i.4xlarge)

Marked POOLED_PROTOCOL_AFFINE: file_fdw, lo, pg_freespacemap, pg_visibility,
pg_buffercache, sslinfo, pg_logicalinspect (7 stateless leaf modules) + auto_explain
(already per-session-converted: 17 GUC accessors + per-exec nesting/sampled + hooks
set-once at load).

Validated under multithreaded=on pooled (libxtc v1.37 build):
- test_extensions backend-model regression: PASS
- All 8 extensions CREATE EXTENSION / LOAD succeed (no fail-closed rejection):
  file_fdw fdw=1, pg_freespacemap fn=t, pg_visibility fn=15, pg_buffercache rows=t,
  sslinfo ssl_is_used=f (correct), pg_logicalinspect ok, lo ok.
- auto_explain per-session GUC: conn4 `LOAD 'auto_explain'; SET
  auto_explain.log_min_duration=333; SHOW` -> 333ms in its own session; plain
  connections before/after return 42 -> no cross-connection breakage, per-session
  storage confirmed.
- server log: 0 fail-closed rejections, all backend fibers exit code=0.

Harness note: `psql -tAXq ... | tail -1` ate output on 2nd+ connections in an
earlier iteration (a capture artifact, NOT a bug) -- full-output re-run confirmed
all connections work.

DEFERRED (need per-session conversion first): pg_stat_statements (5 custom GUCs,
only partial session-state, + hooks + shmem), postgres_fdw (custom GUC), xml2
(libxml process-global parser/error state -- confirm core pg_xml_init threaded
handling first).
