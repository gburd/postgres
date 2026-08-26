# libxml per-context error handlers — validated on libxml2 2.14 + landed (2026-08-26)

Attached xml_errorHandler to each parser/xpath context via xmlCtxtSetErrorHandler /
xmlXPathSetErrorHandler (5 core xml.c sites), gated HAVE_XML_PER_CTXT_ERRHANDLER
(libxml2 >= 2.13).  Touches no OS-thread-global, so migration-safe by construction --
drops the implicit "no fiber yield in the pg_xml_init/done window" reliance (the
XtcPgNoStealEnter/Leave tripwire stays as belt-and-suspenders; global install stays the
<2.13 path).

Validated by BUILDING libxml2 2.14.3 from source on the AL2023 box (AL2023 ships
2.10.4 which is <2.13) so the active per-context path is exercised:
- LIBXML_VERSION 21403 -> HAVE_XML_PER_CTXT_ERRHANDLER ON; PG linked libxml2.so.16.
- process regress 245/245 0-diffs, xml/xpath no diffs (error-message parity preserved
  on the per-context path).
- threaded mt=on: core xpath=hi, valid xml ok; CONCURRENT 8 sessions across carriers
  (evens xpath, odds parse-error) -> each session's result/error stayed with that
  session (s2/s4/s6/s8=ok, s1/s3/s5/s7=ERROR), 0 crashes/asserts, NoSteal tripwire
  0-fired.  Per-context handler isolates errors across concurrent carriers WITHOUT the
  no-yield invariant.
- On <2.13 (earlier AL2023 2.10.4 run) the helpers are no-ops -> byte-for-byte legacy.

Follow-up: convert xml2's context-less xmlReadMemory sites to xmlNewParserCtxt +
xmlCtxtReadMemory to attach per-context there too (xml2 already safe via tripwire +
xsltCleanupGlobals-skip; this is further hardening).
