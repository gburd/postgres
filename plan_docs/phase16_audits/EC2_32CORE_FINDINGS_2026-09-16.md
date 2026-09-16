# EC2 32-core validation on libxtc v1.48.1: two NEW blockers found before a benchmark could run

Date: 2026-09-16. Box: AWS c6id.8xlarge (32 vCPU, 61 GB RAM, 1.7 TB local NVMe/XFS), Debian 13,
`shared_buffers=52GB`, `fsync=on`, libxtc v1.48.1 built `--with-io-backend=uring`.

## Setup verified properly (both traps checked)
- **io_uring is real, not an epoll fallback.** `configure` reports `checking for L1 I/O backend... uring`,
  the library links `liburing.so.2`, and at runtime the server shows **992 io_uring ring fds** (SqMask in
  `/proc/<pid>/task/*/fdinfo/*`). Note `nm -D | grep -c io_uring` returns **0** and is NOT a valid check --
  liburing calls are imported, not defined locally; that would have produced a false negative.
- Built from a **committed `git archive`** of `origin/xtc`, transferred base64 with md5 verification
  (65,580,490 bytes, md5 matched) -- never the working tree.
- Threaded runtime came up correctly: `carrier scheduler thread up (32 loops, 32 supervisors,
  thread-per-session backend pool)`, client backends confirmed `launched as xtc fiber`.

## BLOCKER 1: default `ulimit -n` (1024) makes the threaded server unstartable
```
FATAL: could not create interrupt-wake eventfd for PGPROC: Too many open files
```
The threaded model allocates per-PGPROC eventfds (`sem_wake_fd` + `interrupt_wake_fd`), so
`max_connections=600` needs far more descriptors than the distro default of 1024. Raising to 131072 fixed
it. **This is a packaging/documentation gap, not a bug**: a threaded PostgreSQL must either document a
minimum `nofile`, ship a systemd `LimitNOFILE=`, or (better) fail at startup with a message that names
the required limit rather than dying on the 300th PGPROC. Worth an explicit startup check.

## BLOCKER 2 (the real one): connections fall back to `fork()` and are refused with ENOSYS
After ~102 fibers had been launched and some load had run, **every** new connection failed:
```
LOG:  could not fork new process for connection: Function not implemented
pgbench: error: could not create connection for setup
```
`ENOSYS` here is `launch_backend.c:511-514`:
```c
if (multithreaded && postmaster_thread_carriers_started) { errno = ENOSYS; return false; }
```
i.e. the child reached the **fork-without-exec** path, which is correctly refused once any thread carrier
exists. But `PgRuntimeShouldThreadBackend()` lists `B_BACKEND`, and client backends *were* launching as
fibers earlier in the same run -- so something later routes a `B_BACKEND` to
`PG_BACKEND_LAUNCH_PROCESS`. State at the wedge: postmaster alive, **60 threads**, **2305 fds**, 102
fibers launched, 20 ENOSYS. Threads/fds are far below the raised limits, so this is not resource
exhaustion; it looks like a **carrier/slot capacity path that degrades to process launch** and then hits
the guard.

This is a hard availability bug: the server stops accepting connections entirely while remaining alive,
and it is reachable purely by running a normal pgbench workload at c=64 on 32 cores. It did not appear on
the 8-core dev box, so it is scale-dependent -- exactly what the EC2 run was for.

## Not yet measured (blocked by BLOCKER 2)
The fork-vs-fiber benchmark could not run: read and write pgbench both failed at connection setup. One
partial write observation before the wedge is on record from the earlier attempt on this box -- sessions
were progressing and hitting normal `ExclusiveLock on extension of relation` contention, i.e. the
workload was healthy until connections stopped.

## Also confirmed on real hardware: v1.48.1 improves the local wedge but does not close it
On the 8-core dev box (same repro, fresh initdb): v1.47.0 wedged at ~45 s with 175 zero-tps intervals;
v1.48.1 sustains 1128 -> 1449 -> 1517 -> 1473 -> 1011 -> 872 tps and wedges at ~105-120 s (~180 s on the
debug build). libxtc reports itself clean at that wedge -- `xtc-stranded` shows 75 parked, **0 suspect**,
every park sourced, with the new CQ-overflow join active. So the residual wedge is PG-side.

## Next
1. Root-cause BLOCKER 2: instrument `PgRuntimeGetBackendLaunchModel` / `postmaster_backend_thread_launch`
   to log why a `B_BACKEND` chose PROCESS. Prime suspect is a fiber-spawn failure (supervisor mailbox full
   / slot exhaustion) that returns false and lets the caller fall through to the fork path -- in which case
   the correct behaviour is to retry or reject the connection cleanly, never to attempt fork.
2. Add the startup `nofile` check from BLOCKER 1.
3. Then re-run the fork-vs-fiber matrix on the same box.

Instance terminated after the run; key/SG/pem cleaned.
