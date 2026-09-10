# Fix B: xml2 per-context libxml error handlers -- concurrency evidence

## What was changed

Converted contrib/xml2's context-less `xmlReadMemory` calls to
`xmlNewParserCtxt()` + `pg_xml_ctxt_seterror()`/`pg_xml_xpath_seterror()` +
`xmlCtxtReadMemory()`, so concurrent XML parsing from multiple carriers does
not race libxml2's carrier-thread-global error handler
(`xmlSetStructuredErrorFunc`). Sites: `pgxml_xpath()`, `xpath_table()`'s
per-row loop (`contrib/xml2/xpath.c`), and both parses in `xslt_process()`
(`contrib/xml2/xslt_proc.c`). `pg_xml_ctxt_seterror`/`pg_xml_xpath_seterror`
un-staticed in `xml.c`, prototyped in `xml.h` under `#ifdef LIBXML_VERSION`.
Background: `plan_docs/phase16_audits/LIBXML_ERROR_HANDLER_THREADED_DESIGN.md`,
`LIBXSLT_ERROR_GLOBALS_AUDIT.md`.

## Environment

EC2 `c6id.4xlarge`, autoconf build, `USE_XTC_CARRIER=1`, libxtc v1.43.0
(`b977d21`), libxml2 2.13.8, libxslt 1.1.43 (both built from source into
`/usr/local` to get the per-context API, since the AL2023 system libxml2 is
2.10.4). Standalone server, NOT a suite target (per corrected guidance: the
threaded regress suite cannot currently vouch for anything due to an unrelated
pre-existing deadlock -- see `RELOPTIONS_THREADED_UNWIND_AUDIT.md` section 6).

`postgresql.conf`: `multithreaded = on`, `pooled_protocol_carriers = 4`,
`autovacuum = off` (to avoid an unrelated, separately-documented autovacuum/
WAL-write contention hang seen once during Fix A testing).

## Methodology

`hammer-scripts/setup.sql`: 500 well-formed `<doc><int>N</int><name>...`
documents plus 3 deliberately malformed documents (truncated tag, plain text,
bad entity reference) in one table.

Four pgbench scripts:
- `xpath_table.sql`: `xpath_table()` over the 500 well-formed rows only
  (`id <= 500`), for a clean sustained-throughput baseline.
- `xslt_process.sql`: `xslt_process()` on a random well-formed row with an
  identity-copy stylesheet, same purpose.
- `xpath_table_mixed.sql`: `xpath_table()` over the FULL table (well-formed +
  malformed), exercising `xpath_table`'s documented contract that a
  not-well-formed document yields an all-NULL row rather than an error.
- `xslt_process_mixed.sql`: a `DO` block that picks a malformed document and
  calls `xslt_process()`, catching the expected `ERROR` -- exercises the
  per-context error path itself under concurrency.

## Results

| Run | Clients | Duration | Scripts | Transactions | Failures | Notes |
|---|---|---|---|---|---|---|
| 1 | 32 | 30s | xpath_table + xslt_process (mixed malformed inline) | 13,654 (partial, aborted by pgbench on first ERROR) | 0 | pgbench's default simple-query mode aborts a client on ANY SQL ERROR; malformed docs were later moved to a dedicated script |
| 2 | 64 | 60s | xpath_table (well-formed) + xslt_process (well-formed) | 89,911 | 0 | sustained baseline |
| 3 | 64 | 45s | xpath_table_mixed (well-formed + malformed) | 34,927 | 0 | error-path-in-the-loop concurrency |
| 4 | 64 | 45s | xslt_process_mixed (malformed, caught) | 791,709 | 0 | error-path concurrency, very cheap transactions |
| 5 (final) | 96 | 90s | all four scripts combined | 133,590 | 0 | definitive combined proof |

**Total across all runs: over 1 million transactions, 96 concurrent client
connections at peak, multiplexed over just 4 carrier OS threads
(`pooled_protocol_carriers=4`), zero failed transactions, zero server
crashes, zero occurrences of the `"libxml error handling state is out of
sync with xml.c"` WARNING that would indicate the misrouting hazard this fix
targets** (`grep -c` on the server log after every run: 0).

Server survived every run; a plain `SELECT 1` after each hammer confirmed
liveness. See `hammer_final.log` for the last (combined, 96-client) run's
full postgresql.conf-configured server log.

## `-Wmissing-prototypes` / header arrangement

Rebuilt `contrib/xml2/xpath.c`, `xslt_proc.c`, and `src/backend/utils/adt/xml.c`
from scratch (removed .o, `touch`ed source) under the standard build flags
(`-Wmissing-prototypes -Wold-style-declaration -Wstrict-prototypes` etc,
present by default in both the meson and autoconf builds) -- zero warnings.
Confirms `xml.h`'s `#ifdef LIBXML_VERSION`-guarded prototypes are visible in
xml.c and contrib/xml2 (which include libxml headers before `utils/xml.h` is
processed... actually after, since xml.h is included before the libxml
headers in both contrib files, meaning the guard is INACTIVE there and the
local `extern` declarations in xpath.c/xslt_proc.c are what satisfies
`-Wmissing-prototypes` for those two TUs; xml.c itself includes the libxml
headers first, so its `xml.h` inclusion sees `LIBXML_VERSION` defined and gets
the prototypes from the header directly). Both arrangements were confirmed
warning-free.

## `xsltParseStylesheetDoc` -- cannot be made per-context

Documented as a defer-with-invariant in
`LIBXML_ERROR_HANDLER_THREADED_DESIGN.md` section 7: `xsltParseStylesheetDoc`
takes only a `doc`, and libxslt's only per-object error-routing knob,
`xsltSetTransformErrorFunc`, needs a `xsltTransformContextPtr` that doesn't
exist until later (`xsltNewTransformContext()`). Confirmed against the linked
libxslt 1.1.43 headers: no per-context stylesheet-parse API exists. Safe today
because the `ssdoc` document it parses was itself produced by our own
per-context `xmlCtxtReadMemory()` moments earlier -- `xsltParseStylesheetDoc`
walks an already-parsed, already-error-checked tree; it does no further
libxml parsing of untrusted input. The Fix B concurrency hammer (this
document) is the guard that would catch a wrong assumption here.

## What was NOT proven

- The threaded regress suite's own `xml2` test (`gmake ... check-threaded-pooled`
  scoped to `xml2`) was ALSO run and passed (see the main task report), but
  per the corrected guidance that suite-level result carries less weight than
  this standalone hammer, since the suite is independently known-unreliable
  for reasons unrelated to xml2.
- Migration-safety (a fiber migrating carriers mid-parse) was not directly
  exercised: fibers are pinned (`migratable=0`) in the current runtime, so
  this is not reachable today; the design doc's Section 2b analysis covers
  why the fix is migration-safe by construction regardless (no thread-local
  or global slot is ever written).
- Older libxml2 (<2.13, no per-context API, legacy global-handler fallback
  path) was not exercised on this hardware, since it required building
  libxml2 2.13.8 from source specifically to get coverage of the new code
  path; the system libxml2 (2.10.4) was never linked against these fixes in
  this session.
