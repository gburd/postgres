# Threaded connection accept: fd LEAK is the deeper root cause (2026-08-05)

## What the accept-drain fix did and did NOT fix
Commit 37285c8daba (AcceptConnectionDrain, landed + world-tested green: process
regress 0 fail, test_backend_runtime 18/0, clean quiet-server fast-stop rc=0).
It fixes a ONE-SHOT burst: 384 and 768 simultaneous psql `select 1` -> 0 fail
(both tps and pooled), vs 318/384 fail pre-fix.

But it does NOT fix a SUSTAINED / tight connect storm:
- HammerDB @384 VUs (all Tcl threads connect at once): still 264/384 fail
  "server sent an error response during SSL exchange"; tps cell = NA again.
- `pgbench -C` (connect-per-transaction) at c=8..200: fails, and REPEATED storms
  WEDGE the server -- afterwards even a single psql fails with the same error.

## Root cause of the sustained-storm failure: an fd LEAK in the threaded postmaster
On the wedged postmaster (live, in ep_poll, still "listening" backlog 2048) the
process held **8,591 open fds** (listen socket was fd 2203 and climbing).  The
threaded connection handoff leaks file descriptors under a connect storm: each
connection dup()s the client socket for the pooled/tps handoff
(postmaster_pooled_protocol_launch: logical_start->client_sock.sock =
dup(client_sock->sock); backend_thread_launch dups too), and under a storm those
dup'd fds (and/or per-session eventfds/self-pipes) are not all closed, so the
postmaster's fd table fills.  Once near the nofile limit, new connections fail at
socket setup -- surfaced to the client as "error response during SSL exchange"
(the negotiation read gets a reset/short read).  That is why:
- one-shot 384 works (fd pressure transient, GC'd before exhaustion),
- sustained/repeated storms fail and eventually wedge (fds accumulate, never
  recovered).

This is NOT the accept-drain's doing (that only touches the listen fd and
restores blocking mode; world-test proved clean quiet-server stop).  It is a
pre-existing fd-lifecycle leak in the threaded accept->handoff->teardown path,
newly EXPOSED once the accept-drain let a real storm through instead of failing
it at the door.

## The next fix (scoped, evidence-first)
1. INSTRUMENT the fd count: log open-fd count in the postmaster periodically (or
   on accept) under a `pgbench -C -c 64 -T 30` storm, and watch it climb -- pin
   the leak rate to connections handled.
2. AUDIT the dup'd client socket lifecycle in the threaded handoff:
   - postmaster_pooled_protocol_launch dup()s client_sock into
     logical_start->client_sock.sock; the exit path
     (backend_pooled_protocol_exit_logical) closesocket()s it -- verify EVERY
     path (failed launch, rejected startup, early client close, dead-end child)
     closes exactly once and none leak.  A launch that fails after the dup, or a
     session that never reaches the exit path, leaks the dup.
   - the postmaster ALSO closes its own accepted `s.sock` after BackendStartup
     (ServerLoop / the drain callback) -- confirm that still happens on every
     drain path (the drain closes s.sock after cb; verify no path skips it).
   - per-session eventfd / self-pipe fds (carrier wake, sem_wake_fd): confirm
     they are closed on session teardown, not just on carrier teardown.
3. Compare fork: fork closes the parent's copy immediately post-fork; the child
   owns its one fd.  The threaded path must have the same exactly-once discipline
   across the dup + handoff + teardown, per connection.

## Consequence for the oversubscription A/B
Still BLOCKED: HammerDB's simultaneous VU open is a sustained storm that leaks
fds and fails the tps lane (NA).  The fd leak must be fixed before the
oversubscription thesis is measurable.  This is the true blocker chain end:
unpooled-ENOSYS -> HammerDB-monitor -> io_method=xtc-futex -> pooled-carrier-
starve -> accept-serialization (FIXED) -> **fd leak under sustained storm** (now).

Fork baseline holds: @384 srv_tpm ~2.43M, ctx-switch ~414k/s, RSS ~59MB(*ps
sampling lull; re-measure mid-window).

## Status
accept-drain fix LANDED (37285c8daba, real + world-tested, keeps -- it is
necessary, just not sufficient).  The fd leak is the next scoped fix, evidence-
first (instrument the fd climb, then close the leak in the handoff path).
All EC2 torn down + 5-region verified clean.
