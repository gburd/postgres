# Active background agents (2026-08-04, beef account) -- for post-compaction handoff

origin/xtc = 2b41b4883f4 (F0b,F1,F2,F0a fusion + libxtc v1.32.0 + crash-fixes + F4 audit).

Three forward items launched as background agents:

1. eb7e8f50-1cb3-416 -- EC2 beef METAL A/B (item 1: beat-fork MEASURE).
   Key xtc-ab-*. Metal box (expensive). Measures: HammerDB fork-vs-mt NOPM
   (VU 192/384), F3 steal-backoff effect on select@384 update_sg_lb_stats,
   LWLock contention profile, thread-count check. Verdict: is mt still ~98.6%
   of fork + what's the beat-fork lever (LWLock vs scheduling). ~4-6h.

2. 1ae1a227 -- TSan: DONE (2026-08-04). Runs on system clang18+glibc. Findings in plan_docs/MULTITHREADED_TSAN_FINDINGS.md: hot-cell family = real design Q; scheduler-counter race = spinlock-blind false positive. Box torn down + verified.
   Key xtc-tsan-*. c7i.8xlarge. Diagnostic race inventory against the carrier
   (F1 counters / F2 queue / bringup / wake paths), classified real/benign/libxtc.
   Does NOT fix races (follow-up per race). ~4h.

3. 90ec52e7-9831-4b8 -- Design docs ONLY (no EC2, no code). Produces
   plan_docs/MULTITHREADED_F4_STRUCTURAL_DESIGN.md (xtc_svr/orc/reg/xproc
   fit+benefit-per-risk at 98.6% parity) + plan_docs/MULTITHREADED_INC4_UNWIND_DESIGN.md
   (Phase 19 Inc-4 abort-and-re-place plan). Commits local/branch, no push.

COORDINATOR MUST after each: (a) get_subagent_result, (b) INDEPENDENTLY verify
teardown across all 5 regions (--profile beef) -- agents have leaked boxes ~4x
this project, (c) two-review gate before landing any code, (d) verify origin/xtc
SHA == validated commit after any land.

libxtc requests in /tmp: libxtc-tls-sni-transport-request.md (TLS SNI+transport;
team is working on it), libxtc-log-emitter-gap-question.md (answered by v1.32.0).
TSan is NOT a libxtc gap.
