# Metal A/B run 2026-08-04 -- INVALID (harness misconfig), but two findings

Box: m8idn.metal-96xl i-08ef4f185c4a88cda, us-east-1, key xtc-ab-20260804-104442.
origin/xtc 2b41b4883f4, libxtc v1.32.0, RELEASE, io_method=sync + huge_pages
both lanes, fsync=off. TERMINATED + SG/key/pem deleted + 5-region-verified clean.
The agent aborted mid-diagnosis and LEFT THE BOX RUNNING (sub-agent-leak pattern
again -- coordinator terminated it). Salvaged logs in .ec2/ab-20260804-metal/.

## Result table (what we got before it stalled)
  lane,vus,rep,nopm
  fork,192,1,2135144   <- the ONLY valid cell
  mt,192,1,NA          <- FAILED (see root cause)
  fork,192,2,NA        <- cascaded (stuck mt postmaster held the lock file)

## ROOT CAUSE (harness, NOT a parity regression): mt lane ran UNPOOLED
The mt lane postmaster (pid 74531) started WITHOUT an effective
pooled_protocol_carriers -- i.e. it came up in THREAD-PER-SESSION mode, which
tries to fork() a process per connection. Server log is a solid wall of:
  LOG:  could not fork new process for connection: Function not implemented
(ENOSYS -- the documented "threaded runtime CANNOT fork" constraint). All 192
HammerDB connections failed to spawn -> mt r1 = NA -> HammerDB's own
"New Order Procedure Error / invalid transaction termination" is the driver
reacting to connections that never got a working backend. The agent's earlier
PROBE verified pooled mode worked (steal_backoff=t, 192 carriers), but the actual
benchmark start used a config path where pooled_protocol_carriers was not in
effect. Classic: verified-in-probe != verified-in-the-actual-run.
LESSON for the re-run: the mt lane MUST assert `SHOW pooled_protocol_carriers`
== 192 AND `SELECT count(*) FROM pg_stat_xtc_carriers` > 0 AT BENCHMARK START
(fail the cell loudly if not), not just in a pre-flight probe. carriers=auto
caps at 256; set it EXPLICITLY.

## SECONDARY FINDING (real, worth a look): mt `pg_ctl stop -m fast` HUNG
Once the postmaster was spinning on fork()=ENOSYS for every incoming conn,
`pg_ctl stop -m fast` never completed ("waiting for server to shut down...."
100+ dots then "failed"). It took `received immediate shutdown request` +
`issuing SIGKILL to recalcitrant children` (15:39) to clear it. WHETHER this is
specific to the pathological unpooled-can't-fork state or a general mt fast-stop
weakness is UNKNOWN from this run -- the postmaster was in a degenerate loop, not
a normal pooled steady state. FOLLOW-UP: reproduce mt `-m fast` from a HEALTHY
pooled server under load and confirm it stops cleanly (it does in the TAP suite
+ prior A/Bs, so this is very likely the degenerate-state artifact, not a
regression). Do NOT assume a fast-stop regression on this evidence alone.

## What this run did NOT deliver (still open -> re-run needed)
- mt/fork NOPM ratio (the "still ~98.6%?" question): NO DATA.
- F3 steal-backoff effect on select@384 update_sg_lb_stats: NOT REACHED.
- LWLock contention profile (the beat-fork lever): NOT REACHED.
- thread-count check (~200 not 4634): NOT REACHED.
The prior session's post-thread-fix A/B (documented in MULTITHREADED_PLAN.md:
100% fork select@192, 115.7% update@192, 98.6% OLTP VU=192, ~212 threads) remains
the best current evidence; THIS run neither confirms nor refutes it.

## Re-run recipe (fix the two harness bugs)
1. mt lane: write pooled_protocol_carriers=192 into the ACTIVE postgresql.conf
   (or -c on the postmaster command line), then AT START assert SHOW returns 192
   AND pg_stat_xtc_carriers has rows; abort the whole run if not.
2. Between cells: after `pg_ctl stop -m fast -w -t 120`, if it does not return
   0, escalate to `-m immediate` and VERIFY the postmaster pid is gone + the lock
   file is removed BEFORE starting the next cell (never start-on-stale-lock).
3. Keep the per-cell timeout, but on a NA cell TEAR DOWN the server hard before
   the next cell so one bad cell cannot cascade (this run: mt-NA cascaded into
   fork-r2-NA because the stuck postmaster held the lock).
