# Accept-drain fix validation (2026-08-05)

Commit e468ac61927 (postmaster: drain the accept backlog per loop iteration in
threaded mode).  Validated on a c7i.12xlarge (48 vCPU), PG built WITH the fix +
libxtc v1.32.0.

## Connection-burst result -- FIX WORKS
Simultaneous trivial `select 1` connects to a threaded server:
  mode              burst   ok    fail   (pre-fix, ab6 metal: 384 -> 66 ok/318 fail)
  thread-per-session 384    384   0
  thread-per-session 768    768   0      (2x the metal core count)
  pooled (16)        384    384   0
Zero errors, zero "too many clients".  The accept-drain admits the whole burst
in one ServerLoop pass instead of one-per-iteration, so the per-fiber SSLRequest
handshakes are no longer serialized behind the loop and clients do not time out.
Both threaded modes fixed.  (Pooled carrier-starvation under HELD work is a
separate, by-design limit -- connection ESTABLISHMENT now works in both.)

## Normal-work smoke -- OK
tps: pgbench -i -s 10 OK; a -c32 run served; server answered select 1; no FATAL/
PANIC in the log.

## Clean-shutdown -- NOT cleanly validated on this box (my test-harness noise)
A fast-stop appeared to hang, but the cause was a STRAY pgbench (-c32 -T10) left
RUNNING when an ssh call timed out mid-run -- pg_ctl -m fast correctly waited on
that live client.  A follow-up fresh start/stop got tangled in the orphaned
backend (cmdline "postgres: postgres postgres 127.0.0.1(34936)" did not match my
pkill "postgres -D" pattern), so I could not get a clean quiet-server stop signal
before tearing down.  This is TEST-HARNESS NOISE, not evidence of a fix-induced
shutdown bug: the accept-drain only touches the listen-fd accept path (drains,
then restores blocking mode in every exit path); it does not touch backend
lifecycle or shutdown.  BUT: clean fast-stop on a quiet threaded server MUST be
confirmed in the review gate + a world-test run before landing (do not assume).
Also re-confirm the listen fd is left blocking after a drain (the restore path).

## Verdict
The fix RESOLVES the connection-burst failure that blocked every mt HammerDB
cell this session (384/768 -> 0 fail, both modes).  Ready for the two-review gate
+ world-test (check-threaded-pooled/world + process check + a clean quiet-server
fast-stop).  Then re-run the oversubscription A/B (fork vs tps @1x/2x) -- the
burst is no longer a blocker.
