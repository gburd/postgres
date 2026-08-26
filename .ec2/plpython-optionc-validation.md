# PL/Python Option C -> POOLED_PROTOCOL_AFFINE — validated + landed (2026-08-26)

Single embedded CPython interpreter, per-session state, GIL via PyGILState_Ensure/
Release, GD per-session.  Two-review gate: design review (GD question resolved) +
implementation review (BLOCK -> 3 whole-process-crash bugs, all fixed):
 - B1: _PG_init re-runs per session under mt=on -> process-once plpython_process_inited
   guard (flip while holding the GIL, before PyEval_SaveThread); per-session re-entry
   is a no-op.
 - B2: proc-cache teardown DECREFs (function_manager bucket, no GIL) ->
   PLy_procedure_delete self-acquires the GIL.
 - B3: SRF cleanup from ShutdownPLyFunction (ExprContext shutdown / LIMIT) +
   RemovePLyProcedureCache (xact abort), no GIL -> PLy_function_cleanup_srfstate
   self-acquires the GIL.
Rule enforced: every fn touching a Python object runs under a held GIL (re-entrant
PyGILState = no-op bump when reached from a handler).  GD refcount confirmed balanced
(extension_modules reset before function_manager).

Validated (chiuso c7i.4xlarge, Python 3.9.25, cassert so PyGILState_Check asserts live):
- plpython regression (process) PASS (byte-for-byte).
- threaded mt=on: CREATE EXTENSION plpython3u; basic fn=42; GD write/read=99; SD per-fn
  counter=2; SRF=3; SRF+LIMIT=1 (B3 ExprContext path); nested-SPI plpython=42; subxact=7;
  abort-path SRF raising an exception -> ERROR propagates, server alive, 0 crashes (B3
  xact-abort path).
- CONCURRENT 6 sessions each set GD to a distinct value -> all read back their own
  (s1=101..s6=106): per-session GD isolation holds, 0 crashes, 0 GIL-check-assert fires.

plpython is now POOLED_PROTOCOL_AFFINE under multithreaded=on.  Ceiling (ponytail:):
whole-call GIL hold serializes Python across carriers for a plpython fn blocking in SPI
-- a throughput optimization deferred to the wait-boundary scheduler, not a correctness
gap.  One-line revert (marker -> PROCESS) is the escape hatch.
