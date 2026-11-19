# libxtc requests from the PostgreSQL integration (2026-11-19)

From: the multithreaded PostgreSQL branch (xtc).
Against: libxtc main d51d9c8a (2.2.0 plus XTC_2.3 receive calls).

Each request names who asks, what is missing, the shape we propose, and
what we do without it.  None blocks us; each removes a PostgreSQL-side
workaround.  The first two are the ones we most want.

---

## 1. CPU affinity for executor loop threads

**What we need.**  PostgreSQL runs every backend as a proc on one
`xtc_exec` with a fixed number of loops, and we want each loop's worker
thread pinned to one CPU.  The goals are stable cache and NUMA placement,
no OS migration of loop threads, and predictable latency.

**What exists.**  `xtc_exec_opts_t` has `size` and `n_loops`.
`__os_thread_set_affinity(int cpu)` exists (os_thread.c:242, Linux,
FreeBSD, illumos), but it is internal, and `dist/libxtc.map` exports only
`xtc_*`.  `__xtc_exec_worker` (exec.c:191) calls
`__os_thread_apply_default_qos()` and records its NUMA node, but never
pins.  The blocking pool (blocking.c) and the io_uring io-wq workers are
not placed either.

**Proposed shape.**  Size-first and appended, so it stays compatible with
2.2:

```c
typedef struct xtc_exec_opts {
	size_t size;
	int    n_loops;
	/* 2.3: CPU placement of loop worker threads.  NULL = no pinning
	 * (today's behaviour).  When set, loop i's worker pins itself to
	 * cpus[i % n_cpus] before it runs any task.  CPU numbers are OS
	 * logical CPU ids; a number outside the process's allowed set is
	 * an XTC_E_INVAL from xtc_exec_create. */
	const int *cpus;
	int        n_cpus;
	/* 2.3: XTC_EXEC_PIN_IOWQ: also bind each loop's io_uring io-wq to
	 * the same CPU (IORING_REGISTER_IOWQ_AFF); XTC_EXEC_PIN_BLOCKING:
	 * confine the blocking pool to the loops' CPU set. */
	unsigned   pin_flags;
} xtc_exec_opts_t;

/* 2.3: the process's allowed CPUs (cgroup cpuset and sched affinity),
 * in NUMA-spread order, so a caller can build cpus[] without reading
 * /proc or cgroupfs.  Returns the count, or XTC_E_*. */
XTC_API int xtc_runtime_allowed_cpus(int *out, int max);
```

A policy enum (`XTC_EXEC_PIN_NONE | COMPACT | SPREAD`) instead of an
explicit array would also serve us, as long as it respects the cgroup
cpuset and the affinity mask, and reports the resulting map
(`xtc_exec_loop_cpu(exec, i)`).

**Without it.**  Each of our per-loop supervisor fibers calls
`pthread_setaffinity_np(pthread_self(), ...)` once at start.  That pins
the loop threads, but not the blocking pool or the io-wq workers.  We
also have to compute the allowed set ourselves.

---

## 2. xtc_osproc: pass file descriptors to the child, and start it with signals blocked

**What we need.**  Every PostgreSQL child that is not a fiber is a
fork+exec'd process, and we want to spawn and watch all of them with
`xtc_osproc`.  These children are:

- a session that loads a library not marked thread-safe;
- the syslogger;
- `archive_command` and `restore_command`.

A PostgreSQL backend child needs some descriptors at exec: the client
socket, the postmaster-alive pipe, and the syslogger pipe.  It must also
start with signals blocked, because the server installs its handlers
after exec and unblocks then.

**What exists.**  In the child, `xtc_osproc_spawn` (osproc.c:411-445)
empties the signal mask, then closes every fd from 3 up except the
control fd (`__child_close_fds_from(3, child_fd)`).  There is no way to
keep another fd or to keep signals blocked.

**Proposed shape.**  Appended to `xtc_osproc_opts_t`:

```c
	/* 2.3: descriptors to keep open across exec, in addition to 0-2
	 * and the control fd.  Each keep_fds[i] is duplicated to
	 * keep_fds_at[i] in the child if that is >= 0 (dup2, so the child
	 * sees a fixed number), else kept at its own number.  Close-on-exec
	 * is cleared on the kept descriptors.  Async-signal-safe in the
	 * child (dup2 + fcntl). */
	const int *keep_fds;
	const int *keep_fds_at;     /* may be NULL */
	int        n_keep_fds;
	/* 2.3: XTC_OSPROC_SIGMASK_KEEP_BLOCKED: block every signal in the
	 * child instead of clearing the mask; the exec'd program unblocks
	 * after installing handlers. */
	unsigned   flags;
```

The close loop then skips the kept descriptors.  `close_range` handles
gaps, so it can close the ranges between them.

**Without it.**  We keep PostgreSQL's own `internal_forkexec` for the
spawn, and use libxtc only to wait (see 3).  That leaves two spawn paths,
and PostgreSQL's pre-exec code is not async-signal-safe today; we are
cleaning that up ourselves either way.

---

## 3. Watch a process libxtc did not spawn

**What we need.**  Until 2 exists, and for children spawned before the
executor started, we want libxtc's fiber-parking wait on a pid it did not
create.

**What exists.**  `xtc_osproc_wait` needs an `xtc_osproc_t`, and only
`xtc_osproc_spawn` creates one.

**Proposed shape.**

```c
/* 2.3: wrap an existing child pid (opens a pidfd on Linux).  The
 * caller must be its parent.  The handle supports wait, try_wait,
 * signal and destroy; ctrl_fd is -1. */
XTC_API int xtc_osproc_adopt(long pid, xtc_osproc_t **out);
/* 2.3: the pidfd, or -1 where pidfd is unavailable, so an embedder can
 * add it to its own poll set. */
XTC_API int xtc_osproc_pidfd(const xtc_osproc_t *p);
```

**Without it.**  About 60 lines in PostgreSQL: `pidfd_open`, a
supervisor fiber parked in `xtc_proc_wait_fd`, then `waitid(P_PIDFD)`.

---

## 4. Still open: xtc_sup_cast_add_child (filed 2026-11-13)

We no longer need this to make progress.  `xtc_sup_add_child` accepts
any proc other than the supervisor (sup.c:1384-1388), and our per-loop
supervisor fiber is such a proc, so we add children from it.  The
request stays open for one reason.  Each add is a synchronous
send-and-reply on the supervisor's loop, and under a connection storm we
have not yet measured whether that round trip matters.  The XTC_2.3
receive mark makes the reply wait cheap, which helps.  We will report the
measurement.

---

## Thanks for the XTC_2.3 fixes

81a553e4 (only library-sent DOWNs) closed a hole in our own classifier
too.  We now receive with `xtc_recv_match_ex` and treat a message as a
DOWN only when `XTC_MSG_SYS` is set, and a test sends a forged DOWN to
prove it.  With the check removed, that test fails and the server
fail-stops.  One request about versions: main's `dist/version.in` still
says 2.2.0, so a consumer cannot tell main from the release by version.
We probe for `xtc_recv_match_ex` instead.  Bumping version.in to 2.2.99
(or 2.3.0-dev) on main would let consumers check `XTC_VERSION_*`.
