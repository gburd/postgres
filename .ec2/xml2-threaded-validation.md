# xml2 threaded-safe + AFFINE — validated + landed (2026-08-26)

Three pieces (branch xml-nosteal-tripwire):
1. xml.c: XtcPgNoStealEnter/Leave tripwire bracketing pg_xml_init/pg_xml_done
   (enforces the "no yield in the libxml handler-install window" invariant; assert-
   carrier-only, no-op elsewhere).
2. xslt_proc.c: skip xsltCleanupGlobals() under multithreaded=on (it mutates libxslt
   PROCESS-GLOBAL registry+mutex per call; xml2 populates no registry -> pure hazard;
   process mode byte-for-byte).
3. xpath.c: mark xml2 POOLED_PROTOCOL_AFFINE.

Validated (chiuso c7i.4xlarge, cassert build so the tripwire is LIVE):
- process regress 245/245, 0 diffs (core xml.c byte-for-byte); xml2 process regression PASS.
- threaded mt=on: core xml xpath='hi', xml2 CREATE ok, xpath_string='hi',
  single xslt_process -> <out>HELLO</out>.
- CONCURRENT xslt_process x8 across carriers: all 8 = HELLO, 0 crashes/asserts,
  0 libxslt-registry-race errors -- the cross-carrier hazard the xsltCleanupGlobals
  skip targets is gone.
- NoSteal tripwire: 0 assertions fired -> confirms no yield in the pg_xml_init/done
  window (the invariant the design relies on).

Durable follow-ups (not blocking): per-context handlers -- xmlCtxtSetErrorHandler
(libxml2>=2.13) + xsltSetTransformErrorFunc -- to drop the implicit no-yield reliance.
