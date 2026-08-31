# Fusion staging validation (2026-08-31, mala c7i.4xlarge, libxtc v1.40.3, cassert)

## Option A staging (pooled_protocol_fiber_sessions) — PASS
- GUC OFF (default): mt=on, fiber_sessions=off, carriers resolves to 16 (auto=core-count,
  stackless pool) -- byte-for-byte unchanged.  -S smoke 183k tps.
- GUC ON: fiber_sessions=on, carriers resolves to 0 (fiber-per-session) -- the staged flip
  works exactly as designed.  -S smoke 54.8k tps (c=8 on 16 vCPU; the win is at high
  concurrency + write load once the libxtc task->state fix unblocks it).
- explicit pooled_protocol_carriers=16 + fiber_sessions=on: carriers=16 (knob correctly
  ignored -- explicit value wins).
- process mode (mt=off): fiber_sessions=off, carriers=0, queries fine.
- PROCESS REGRESSION: 245/245 subtests passed, 0 fail (byte-for-byte).

## F0d (xtc_dump on threaded crash) — PASS
PG_XTC_INJECT_CRASH=1 on a fiber backend: INJECT_CRASH fired, R1-contained SIGNAL DOWN
observed as GENUINE-CRASH, and xtc_dump emitted the full runtime dump to the server log
(=== xtc runtime dump === ... all loops with run-queue/park/mbox stats ... thread backtrace
... === end dump ===) BEFORE "terminating threaded server runtime after backend fiber
crash".  The dump completes cleanly; the primary post-mortem for a fail-stopped threaded
crash now lands in the log.

## Follow-up finding (NOT a regression from these changes)
The cassert crash test surfaced a pre-existing crash-TEARDOWN race:
  TRAP: failed Assert("slot > 0 && slot <= PMSignalState->num_child_flags")
  pmsignal.c:304, in RegisterPostmasterChildActive <- InitProcess <- AutoVacLauncherMain
i.e. an aux fiber (autovac launcher / bgwriter) is mid-InitProcess/RegisterPostmasterChild
when the genuine-crash fail-stop tears the runtime down, and its pmsignal slot index is
out of range during teardown.  Independent of F0d (F0d only adds the dump before
g_xtc_genuine_crash=1; the assert is on a separate concurrent aux-startup path) and of the
Option A knob (default off).  It is a cassert-only teardown-race on the crash-escalation
path.  Scoped follow-up: make aux-fiber InitProcess/RegisterPostmasterChildActive robust to
a concurrent runtime fail-stop (or hold the escalation until aux startup quiesces).  Filed
here; not blocking (crash path, cassert-only, the process still fail-stops correctly).
