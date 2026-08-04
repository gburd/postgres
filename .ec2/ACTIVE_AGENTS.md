# In-flight EC2 / background agents (coordinator handoff)

All three 2026-08-04 forward-item agents are DONE. No agents in flight.
All EC2 resources torn down + verified clean across 5 regions (profile beef).

1. 1ae1a227 -- TSan on the carrier: DONE. Runs on system clang18+glibc (NOT a
   libxtc gap). plan_docs/MULTITHREADED_TSAN_FINDINGS.md: hot-cell family
   (PgRuntime*HotCurrent*) = real new-to-threading design Q; scheduler-counter
   race = spinlock-blind TSan false positive. Box torn down + verified.
2. 90ec52e7 -- design docs: DONE (pushed 700bf6613ec). F4-structural =
   recommend NONE (all 3 OTP behaviours fail benefit-per-risk at parity; one
   small win: fold registry pthread_mutex into F2). Inc-4 = 2-3 commit
   increment, fd-exactly-once-close is the sharp edge. No box.
3. eb7e8f50 -- metal A/B: DONE but INVALID (harness misconfig: mt lane ran
   UNPOOLED -> fork()=ENOSYS -> all conns failed -> mt=NA, cascaded). Agent
   LEFT THE BOX RUNNING; coordinator terminated + verified. Findings +
   re-run recipe in .ec2/ab-20260804-metal/FINDINGS.md. NEEDS A CLEAN RE-RUN
   (assert pooled AT START; hard-stop between cells).

## NEXT (coordinator, when resuming the beat-fork measurement)
- RE-RUN the metal A/B with the two harness fixes (assert pooled_protocol_carriers
  in effect at benchmark start; escalate -m fast->immediate + verify pid-gone +
  lock-removed between cells). This is the still-open beat-fork MEASUREMENT.
- Secondary: confirm mt `-m fast` stops cleanly from a HEALTHY pooled server
  under load (very likely the degenerate-unpooled artifact, not a regression).
