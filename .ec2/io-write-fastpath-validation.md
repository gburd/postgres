# io=xtc write fast path — validated + landed (2026-08-25)

pwritev2(RWF_NOWAIT) write fast path in method_xtc.c, mirroring the read fast path.
Reviewed SHIP-WITH-NITS (short-write fall-through is correct idempotent positional
re-write, not a double-write; nit = buffered-write NOWAIT kernel caveat, documented).

Measured:
- metal (earlier): io=xtc -N 47k (no fast path) -> ~52-60k (with) vs io=sync ~56k
  -- the -36% write regression CLOSED; the recovery proves the fast path FIRES on
  AL2023 kernel 6.1.x (else -N would stay 47k).
- validation (c7i.4xlarge, kernel 6.1.180, cassert): process regress 245/245 0-diffs
  (io=xtc gated behind fiber -> process byte-for-byte); io=xtc threaded smoke clean
  (mt=on, io_method=xtc, select 42, -N=4050, -S=132k, 0 crashes, FAST_STOP rc=0).

io=xtc is now neutral-or-better across cached-read / cold-read / write OLTP.
io=sync remains the documented default; io=xtc is a valid opt-in for read-heavy /
cache-friendly workloads and no longer a write-loser.
