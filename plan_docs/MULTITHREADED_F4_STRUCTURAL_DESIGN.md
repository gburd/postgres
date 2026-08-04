# Structural F4+ design: libxtc OTP behaviours vs the hand-rolled runtime plumbing (2026-08-05)

Design-only pass (no code). Source tree at branch `xtc`, HEAD `2b41b4883f4`.
Read-only analysis of whether the remaining genuine fusion -- replacing
hand-rolled runtime plumbing with libxtc OTP behaviours -- is worth doing at
98.6%-of-fork parity, now that the primitive dedup is COMPLETE (see
MULTITHREADED_F4_FUSION_AUDIT.md).

## Scope and the standing conclusion this refines

The F4 audit closed the primitive question: every wait/block primitive
reachable from a client-backend fiber already yields its carrier OS thread
(Latch/WaitEventSet epoll-park, LWLock-via-PGSemaphore eventfd-park, AIO via
xtc_aio). There is NO carrier-blocking gap and NO risky "convert Latch/LWLock to
xtc" work left. xtc_pool for the carrier pool was evaluated and REJECTED (wrong
shape: borrow/return fiber limiter vs spawn-once run-forever OS threads).

What remains is STRUCTURAL: three hand-rolled runtime subsystems that have a
plausible libxtc OTP counterpart. This doc analyzes each against the installed
v1.32.0 headers (xtc_svr.h, xtc_orc.h, xtc_reg.h, xtc_xproc.h) with a
fit/size/risk/benefit verdict, ranks them by benefit-per-risk, and recommends a
single first increment (or none).

The governing bar (from the roadmap methodology): a structural replacement must
be neutral-or-better on check-threaded-pooled, keep process mode byte-for-byte,
keep a fallback until proven, and -- the honest test at 98.6% parity -- deliver
enough benefit (perf, simplicity, or DST-visibility) to justify touching working
wake/supervision machinery that took the whole project's hardest bugs to get
right.

---

## Item A -- xtc_svr / xtc_orc supervision vs the hand-rolled supervisor + carrier spawn

### Current hand-rolled implementation

Two distinct pieces of "supervision" exist, and they are NOT one subsystem:

1. **Per-loop supervisor fibers** (`pg_xtc_carrier.c`
   `xtc_carrier_supervisor_proc` / `xtc_carrier_start_supervisors`, ~line 471,
   735). One long-lived bare fiber per loop. It does exactly two things:
   - services a spawn request (`xtc_sup_spawn_msg`) by calling
     `xtc_proc_spawn_monitor()` for the backend fiber ON that loop (atomic
     spawn+monitor, race-free);
   - receives the backend fiber's DOWN (`xtc_down_decode_ex`) and CLASSIFIES it:
     CLEAN/NOPROC -> quiet log; EXIT -> quiet log (postmaster reaps under its own
     policy); SIGNAL -> set `g_xtc_genuine_crash` + kick the postmaster latch to
     drive fail-stop.

   Critically, the supervisor **never reaps and never restarts** -- the header
   comment is explicit: "exactly-once reaping and crash policy remain the
   postmaster's." It is a MONITOR + CLASSIFIER + fail-stop-escalator, not a
   restarter.

2. **Pooled-carrier spawn/bookkeeping** (`launch_backend.c`
   `backend_pooled_protocol_start_one_carrier` ~line 1203,
   `backend_pooled_protocol_maybe_start_carrier_for_work` ~line 1338,
   `pooled_protocol_carrier_count`). Elastic grow-to-a-cap: when the session
   queue outgrows idle carriers, spawn one more `pg_thread_create` carrier OS
   thread, up to `PgRuntimePooledProtocolCarrierLimit()`. Carriers are
   process-lifetime pthreads; they are never stopped, never restarted. Each
   carrier hosts an `xtc_exec` loop.

3. **Worker restart policy**: there is none in the threaded runtime. The
   postmaster owns server-worker restart (process mode). Under multithreaded=on,
   a genuine carrier/fiber crash is fail-stop (whole process), by deliberate
   design -- restart is external-supervision territory (systemd), with the
   in-tree watchdog deferred (Item C).

### libxtc primitive that would replace it

- `xtc_svr` (gen_server): call/cast/info mailbox server with a callback vtable.
  Overkill here -- the supervisor does not answer synchronous calls; it consumes
  casts (spawn requests) and info (DOWN signals). The spawn-request path is
  already a bare `xtc_recv` cast loop; wrapping it in gen_server buys nothing.
- `xtc_orc` (supervisor, `xtc_sup_start` + `xtc_child_spec`): monitors children,
  and ON a child DOWN applies a **restart strategy** (ONE_FOR_ONE) with restart
  intensity (max_restarts/period_ns) -- and if the rate is exceeded, the
  supervisor itself exits up the tree. `max_children` gives the
  simple_one_for_one bounded dynamic pool (grow-to-a-cap).

### Fit analysis

The shape looks tantalizingly close (a supervisor that monitors children and a
grow-to-a-cap pool), but the SEMANTICS diverge on the one axis that matters:
**xtc_orc's whole reason to exist is to RESTART a dead child**, and this runtime
deliberately does NOT restart.

- A dead **backend fiber** must NOT be restarted -- the session is gone; the
  postmaster reaps the PMChild slot exactly once. Mapping backend fibers to
  xtc_orc children with `XTC_RESTART_TEMPORARY` (never restart) makes xtc_orc a
  pure monitor -- but the runtime already has that via
  `xtc_proc_spawn_monitor()` + the classifier, which additionally does the
  SIGNAL->fail-stop escalation that xtc_orc has no hook for. xtc_orc's restart
  machinery, intensity window, and up-the-tree exit would all be dead weight
  configured OFF.
- A dead **carrier** must NOT be restarted either (a carrier crash is a genuine
  fault -> fail-stop). And carriers are the loops themselves; xtc_orc children
  are procs ON a loop, not the loops. `xtc_sup_opts.exec` lets a supervisor
  place children across an exec's loops, but the carriers ARE that exec -- the
  supervisor cannot supervise the threads that host it.
- The elastic grow-to-a-cap (`max_children` / simple_one_for_one) IS a genuine
  semantic match for `maybe_start_carrier_for_work`. But that match is for
  spawning FIBERS on demand up to a cap, and our grow-to-a-cap spawns CARRIER
  PTHREADS, not fibers -- the same lifecycle mismatch that sank xtc_pool.

The classifier's SIGNAL->`g_xtc_genuine_crash`->latch-kick escalation is the
load-bearing behaviour, and it is EXACTLY the policy xtc_orc replaces with
"restart the child." Adopting xtc_orc means either fighting it (TEMPORARY policy
+ a custom DOWN interception to re-implement the escalation xtc_orc doesn't
expose) or accepting its restart model, which contradicts the branch's
deliberate fail-stop crash contract (AGENTS.md: "Threaded genuine crashes
fail-stop the whole process").

**Verdict: PARTIAL fit, and the fitting parts are the parts we deliberately do
NOT want.** The monitor half is already covered by `xtc_proc_spawn_monitor`; the
restart half is a semantic conflict with the fail-stop contract; the
grow-to-a-cap half has the carrier-is-a-pthread lifecycle mismatch. This is the
xtc_pool rejection pattern one layer up: the OTP behaviour solves "restart my
dead workers," and this runtime's answer to a dead worker is "fail-stop, let
systemd restart the process."

### Size, risk, benefit

- **Size**: medium-large. Rewriting `xtc_carrier_supervisor_proc` onto
  `xtc_sup_start` with a custom DOWN handler to preserve the SIGNAL escalation,
  plus reworking `maybe_start_carrier_for_work` to `xtc_sup_add_child` semantics
  -- and carriers-are-not-fibers means the add_child path does not even map. Call
  it 300-500 lines touched across the two hottest lifecycle files.
- **Risk**: HIGH. This is the crash-classification + fail-stop-escalation path
  that took the walsender/bgworker crash family and the DOWN-kind overloading
  bugs to stabilize (roadmap "walsender + bgworker threaded crash family
  FIXED"). Any misclassification either fails to fail-stop on a real crash
  (data-safety hazard) or fail-stops on a benign teardown (availability
  regression). The working `xtc_down_decode_ex` classifier is exactly the
  fragile machinery AGENTS.md warns against churning.
- **Benefit**: LOW. No perf (supervision is off the hot path -- it fires on
  spawn and on death, not per query). Marginal simplicity (arguably NEGATIVE:
  configuring xtc_orc's restart machinery OFF and bolting the escalation back on
  is more code than the current bare-fiber loop). Some DST-visibility gain
  (xtc_orc is a libxtc proc, so its scheduling is DST-replayable) -- but the
  current supervisor is ALREADY an xtc_proc on an xtc loop, so it is already
  DST-visible; there is no raw pthread to remove here.

**Benefit-per-risk: LOW/HIGH = poor.**

### Recommendation: **DEFER (leaning SKIP).**

The monitor is already an xtc_proc; the restart behaviour xtc_orc would add is
one we deliberately reject; the grow-to-a-cap has the carrier-pthread mismatch.
Revisit only if the in-tree watchdog (Item C) lands and changes the crash
contract from fail-stop to in-tree restart -- at which point xtc_orc's restart
strategy becomes a genuine fit rather than dead weight. Until then this is
fusion for its own sake against fragile, working code.

---

## Item B -- xtc_reg registry / process groups vs the threaded backend registry

### Current hand-rolled implementation

`backend_runtime_backend.c` `ThreadedBackendRegistry*` (~line 59-399, 1589,
1678):

- A singly-linked list of `ThreadedBackendRegistryEntry {backend_id, backend,
  next}` guarded by a raw `pthread_mutex_t ThreadedBackendRegistryMutex`.
- Operations: register (link-at-head, dup-check), unregister (unlink),
  lookup-by-id (linear scan under lock).
- The load-bearing consumer is the cross-fiber wake fan-out:
  `PgBackendWakeWaitCompletionById(backend_id, ready_events)` (~1678) takes the
  lock, `ThreadedBackendRegistryLookupLocked(backend_id)`, and if found calls
  `PgBackendWakeWaitCompletion(backend, ready_events)` under the lock. This is
  how a waker on one carrier reaches a parked waiter on another carrier by its
  logical backend id.

So the registry maps `PgBackendId -> PgBackend*` for cross-fiber wakeup routing.
It is NOT a name registry and NOT a pub/sub group table; it is an id->pointer
index with a linear scan.

### libxtc primitive that would replace it

`xtc_reg`: `name (const char*) -> xtc_pid_t` lookup, plus duplicate-key
(pub/sub / process-group) registration, a crash-aware reaper
(`xtc_reg_register_mon` + `xtc_reg_reaper` auto-drops a pid on DOWN), and
via-dispatch (`xtc_svr_call_name`).

### Fit analysis

Three hard mismatches:

1. **Key type.** xtc_reg keys are `const char *` names. The runtime keys on
   `PgBackendId` (an integer). We would either stringify every backend id on
   every wake (allocation + hashing on the hot wake path) or misuse the
   duplicate-key API. Neither is better than an integer compare.
2. **Value type.** xtc_reg maps to `xtc_pid_t` (a fiber pid). The runtime maps to
   `PgBackend *` (the session's runtime object), and the wake path dereferences
   that pointer to call `PgBackendWakeWaitCompletion`. xtc_reg cannot store the
   `PgBackend *`; it would give back an `xtc_pid_t`, forcing a SECOND lookup
   (pid -> PgBackend) that does not exist today. The registry's job is precisely
   to hold that pointer.
3. **The wake fan-out is not what xtc_reg does.** xtc_reg's "reach a process by
   name" is `xtc_svr_call_name` (send a gen_server call). The runtime's wake is
   NOT a message send -- it is a direct in-memory `PgBackendWakeWaitCompletion`
   that flips park state and, where needed, does the eventfd/latch write. Routing
   it through an xtc_reg name lookup + gen_server call would add a message hop
   and a serialization boundary to a path that is currently a locked pointer
   deref.

There IS one appealing feature: `xtc_reg_reaper` / `xtc_reg_register_mon`
auto-drop on DOWN would remove the manual unregister-at-exit step. But the manual
unregister is one line on a cold path (backend teardown), and buying automatic
cleanup would cost the whole key/value/hop mismatch above.

The honest observation: the current registry is a linear-scan list under a raw
pthread mutex, and the ONE thing that could genuinely improve is (a) the O(n)
scan under contention and (b) replacing the raw pthread_mutex with an xtc
primitive for DST-visibility. But (a) is a data-structure choice (a hash table
keyed on PgBackendId), not an xtc_reg adoption -- xtc_reg's own storage is "a
per-application table guarded by a mutex," i.e. the same shape we have. And (b)
is the F2 lock-dedup increment (swap the raw pthread_mutex for xtc_lwlock),
which is a much smaller, lower-risk change than adopting a whole registry
behaviour, and does NOT require xtc_reg at all.

**Verdict: NOT A FIT.** Wrong key type, wrong value type, wrong access pattern
(direct deref vs message send). The only real improvements available (hash the
scan; DST-ify the lock) are reachable WITHOUT xtc_reg and are smaller.

### Size, risk, benefit

- **Size**: large (rewrite every register/unregister/lookup + the wake fan-out
  onto a name/pid API that does not fit) OR, if we only chase the real wins,
  small (F2 lock swap; optional hash table).
- **Risk**: HIGH for full xtc_reg adoption (the wake fan-out is core cross-fiber
  wakeup -- a lost wake is a hang, the exact bug class the project fought at the
  pthread<->fiber boundary). LOW for the F2 lock swap alone.
- **Benefit**: LOW-to-NONE for xtc_reg. The wake path is not obviously a
  contention hot spot at 98.6% parity (the profile blames LWLock contention and
  the CFS newidle tax, not the backend registry). No perf case has been made.

**Benefit-per-risk: NONE/HIGH = do not do it.**

### Recommendation: **SKIP xtc_reg.**

If registry contention ever shows up in a profile, the answer is a hash table
keyed on PgBackendId and/or the F2 raw-pthread_mutex -> xtc_lwlock swap -- not
xtc_reg, whose name/pid/message-send shape does not match the id/pointer/direct-
deref reality. Fold the lock swap into F2 if and when F2 reaches this file.

---

## Item C -- xtc_xproc watchdog vs the current fail-stop / external-supervision crash story

### Current implementation

Under multithreaded=on a genuine crash (a synchronous fault in a carrier fiber,
or a process-fallback backend crash) is **fail-stop**: the whole process
terminates fast (RLIMIT_CORE dropped at carrier start unless PG_XTC_ALLOW_CORE=1;
`pg_xtc_carrier.c` ~line 1007). Restart is external-supervision territory
(systemd). There is NO in-tree restart-after-crash for the threaded runtime.
`restart_after_crash` in process mode still works (fork model); the threaded
runtime deliberately opts out because the postmaster cannot safely
SIGQUIT+reinit shared memory with live carrier threads inside its own address
space (see TAP 013's rationale).

AGENTS.md names the future path explicitly: "a future in-tree xtc_xproc watchdog
as the path to in-tree restart."

### libxtc primitive

`xtc_xproc`: cross-fork spawn/send/monitor. A parent `xtc_xspawn`s a child that
runs its own xtc runtime; the parent gets an `xtc_xpid_t` and can `xtc_xmonitor`
it so a child CRASH/EXIT surfaces as a normal xtc DOWN with the signal/exit-code
decoded from waitpid, or `XTC_DOWN_KIND_NOCONNECTION` if the control channel
dies. POSIX-only (fork + socketpair + waitpid).

### Fit analysis

This is the ONLY one of the three that is a genuine architectural fit for its
stated purpose -- but its purpose is NOT "fuse existing plumbing." There is no
hand-rolled watchdog to replace; there is a deliberate ABSENCE (fail-stop +
external supervision). xtc_xproc would BUILD a new capability (in-tree restart),
not dedup an existing one.

The natural design: a small supervisory process (the postmaster, or a dedicated
watchdog child) `xtc_xspawn`s the carrier-hosting server process, `xtc_xmonitor`s
it, and on a crash DOWN restarts a clean image -- exactly the "in-tree
restart-after-crash" AGENTS.md defers. This is coherent and xtc_xproc fits it
cleanly (the DOWN-with-decoded-waitpid is precisely what a watchdog needs).

BUT this is a large new subsystem with a hard prerequisite chain: it changes the
crash contract (fail-stop -> restart), which the whole current design and TAP
010/013 are built around, and it re-introduces the shared-memory-consistency
problem that fail-stop exists to sidestep (a crashed carrier may have left shmem
inconsistent; restarting requires the same reinit dance the postmaster currently
cannot safely do with live carriers -- the watchdog design must sit OUTSIDE the
carrier process, restarting the whole image, which is what systemd already
does). The marginal value over "let systemd restart it" is: faster restart,
in-tree health policy, no external dependency. Real, but not a parity or
simplicity win -- a new operational feature.

**Verdict: CLEAN fit for its purpose, but it is NET-NEW capability, not fusion.**
It belongs to the Phase 18/watchdog roadmap item, not to "replace hand-rolled
plumbing with xtc behaviours." It has a real design prerequisite (the
shared-memory reinit story under threading) that is unsolved and larger than the
xtc_xproc adoption itself.

### Size, risk, benefit

- **Size**: large (new watchdog process, crash-contract change, shmem-reinit
  story, TAP rework). This is a phase, not an increment.
- **Risk**: HIGH (changes the crash contract that data-safety currently rests
  on) -- but the risk is in the shmem-reinit design, not in xtc_xproc itself.
- **Benefit**: MEDIUM (in-tree restart is a real operational feature) but ZERO
  toward the 98.6%->100% parity goal (a crashed backend restarting faster does
  not make a non-crashing benchmark faster).

**Benefit-per-risk: MEDIUM/HIGH, and orthogonal to the parity north star.**

### Recommendation: **DEFER to its own phase.**

xtc_xproc is the right tool WHEN in-tree restart is scheduled, and the header is
ready. But it is a new capability gated on an unsolved shmem-reinit-under-
threading design, not a fusion increment, and it does nothing for parity. Keep
the fail-stop + systemd contract; open the xtc_xproc watchdog as its own phase
with the shmem-reinit design as its first deliverable.

---

## Ranking by benefit-per-risk

| Rank | Item | Fit | Size | Risk | Benefit | Verdict |
|------|------|-----|------|------|---------|---------|
| 1 | C: xtc_xproc watchdog | clean (for its purpose) | large | high | medium (new feature, 0 parity) | DEFER to own phase |
| 2 | A: xtc_svr/xtc_orc supervision | partial (wrong parts fit) | med-large | high | low | DEFER (lean SKIP) |
| 3 | B: xtc_reg backend registry | not a fit | large | high | none | SKIP |

None of the three clears the 98.6%-parity bar of "enough benefit to justify
touching the working wake/supervision machinery." Two are semantic conflicts
with the deliberate fail-stop contract (A's restart, and B doesn't fit at all);
the third (C) is a genuine future feature but is net-new capability orthogonal to
parity, gated on an unsolved shmem-reinit design.

## Recommended first structural increment

**NONE, as structural OTP fusion.** At 98.6% parity with primitive fusion done,
the honest answer is that the remaining OTP behaviours are either
semantic-conflict (A), no-fit (B), or new-feature-not-fusion (C). Spending the
next increment adopting xtc_orc/xtc_reg would touch the runtime's most fragile
wake/crash machinery for low-to-no benefit -- exactly the churn AGENTS.md forbids
without runtime evidence.

If a small, low-risk step is wanted from THIS analysis, it is not a structural
OTP adoption at all but the one concrete win the registry analysis surfaced:
**fold the raw `ThreadedBackendRegistryMutex` pthread_mutex into the F2
lock-dedup increment (swap to xtc_lwlock/xtc_sync) for DST-visibility**, when F2
reaches `backend_runtime_backend.c`. That is a primitive swap with a kept
fallback, not a behaviour adoption -- small, measurable, and it removes a real
raw pthread from the DST boundary. It does not require this doc's three
behaviours.

Otherwise: the roadmap's item-1 ordering is right. MEASURE first (EC2 metal A/B
for the F3 steal-backoff effect + an LWLock-contention profile), and let the
profile -- not a fusion checklist -- pick the next lever. The structural OTP
behaviours stay deferred until either a profile demands one or the in-tree
restart phase (C) is deliberately scheduled.
