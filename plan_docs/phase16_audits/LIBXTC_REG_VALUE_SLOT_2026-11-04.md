# Request/feedback: why xtc_reg is a poor fit for PostgreSQL's backend registry, and how it could change

Date: 2026-11-04
libxtc: fix/stale-readiness-dispatch @ `6dd001e0` (`v2.1.1-6-g6dd001e0`), the
commit we pin. All claims re-verified against this exact source.
Reporter context: PostgreSQL-on-libxtc branch. We evaluated `xtc_reg` as a
replacement for our hand-rolled threaded backend registry and decided NOT to
adopt it. This write-up explains why — grounded in the current `xtc_reg`
implementation — and proposes the smallest changes that would make it a fit, in
case others hit the same wall or you want to widen `xtc_reg`'s applicability.

This is feedback, not a bug report. `xtc_reg` does exactly what its header says;
the mismatch is with a shape of registry it was not designed for.

## What our registry must do (the shape that does not fit)

`src/backend/utils/init/backend_runtime_backend.c` keeps a threaded backend
registry: a map keyed by `PgBackendId` whose VALUE is a `PgBackend *` — a
pointer to the live backend's in-memory control struct. Its one hot consumer,
`PgBackendSendInterruptById()` (called from `procsignal.c` for query-cancel,
terminate, and ProcSignalBarrier), does this under the registry lock:

```
backend = lookup(backend_id);        /* -> PgBackend *            */
if (backend)
    SendInterrupt(backend, type);    /* atomic fetch_or on backend->interrupts
                                        .pending_mask, then a non-blocking
                                        xtc_proc_wake(backend's fiber)         */
```

The operation is "given a stable logical id, reach the target's control struct
and flip a bit in it + wake it." The value we must recover is the **struct
pointer**, not the fiber pid.

## Why xtc_reg cannot express that (verified in src/orc/reg.c)

`xtc_reg` is a name -> `xtc_pid_t` table. Its node stores exactly:

```c
struct reg_node { struct reg_node *next; uint32_t hash; xtc_pid_t pid;
                  char *name; int dup; };   /* reg.c:35 */
```

and `xtc_reg_whereis()` (reg.c) returns only `*out_pid = node->pid`. There is
**no value/userdata slot** — you can associate a name with a pid, nothing else.

So to use `xtc_reg` we would have to recover `PgBackend *` from the pid it hands
back. There is no supported path:

- `xtc_proc_userdata()` (xtc_proc.h:370) reads only the CALLING proc's userdata
  — it has no pid-indexed form. We already `xtc_proc_set_userdata()` our backend
  identity on each fiber, but a sender cannot read another proc's userdata by
  pid. (Confirmed: the getter takes no pid; it operates on `xtc_self()`.)
- So we would still need our OWN `PgBackendId -> PgBackend *` map alongside
  `xtc_reg`. That is ADDING a second structure, not replacing one. For a
  consumer, "deletion beats addition"; adopting `xtc_reg` here grows the code
  and the per-register work for no gain.

## Why xtc_reg's headline feature (crash-aware auto-drop) also buys us nothing

The distinctive value of `xtc_reg` over a plain map is `xtc_reg_reaper` +
`xtc_reg_register_mon`: a registered name is auto-dropped when its pid goes
DOWN (reg.c:395 reaper loop). That matters when a dead entry is a latent hazard.

For us it is not. We unregister on the clean teardown path
(`PgBackendResetClosedState`). On a GENUINE fiber crash our policy is
fail-stop: the whole OS process `_exit()`s (a crashed backend may have corrupted
the shared address space). A stale registry entry in a process that is about to
die is irrelevant. So the auto-drop closes a leak that fail-stop already makes
unreachable — no lifecycle or correctness gain for our model.

## Why we also did NOT move the registry lock to xtc_amutex

A natural adjacent "fusion" would be to replace the registry's raw
`pthread_mutex` with `xtc_amutex` (fiber-aware), the way we did for the GUC
critical section. We deliberately did not, and this is the useful distinction:

- The GUC lock CAN be held across a fiber park (GUC code allocates/waits under
  it), so a raw `pthread_mutex_lock` there blocks the whole carrier OS thread
  and starves peer fibers — that one needs `xtc_amutex` (and the load-bearing
  save/restore seam around the park).
- The registry lock is only ever held across a list walk + `SendInterrupt`, and
  `SendInterrupt` is NON-parking (atomic `fetch_or` + a non-blocking
  `xtc_proc_wake`; no `xtc_proc_wait_fd`, no `xtc_yield`, no parking alloc). A
  lock never held across a yield cannot block a carrier. Converting it would add
  the amutex park seam around a lock that never parks — complexity and risk for
  only a DST-visibility nicety.

We documented this as an invariant comment at the lock site (nothing under this
mutex may park; if that changes, convert to `xtc_amutex`). Flagging it here
because it is a good general rule for `xtc_sync` adopters: `xtc_amutex` earns its
seam only when the critical section can park a fiber.

## How xtc_reg could change to fit (smallest first)

Either of the first two would let us drop our hand-rolled map entirely; the
third is the broader generalization.

**(1) An optional value/userdata slot per entry.** Let a registration carry an
opaque `void *` alongside the pid, returned by lookup:

```c
XTC_API int xtc_reg_register_val(xtc_reg_t *r, const char *name,
                                 xtc_pid_t pid, void *value);
XTC_API int xtc_reg_whereis_val(xtc_reg_t *r, const char *name,
                                xtc_pid_t *out_pid, void **out_value);
```

`reg_node` gains one `void *value`. Then our registry IS `xtc_reg`: register
`(backend_id-as-name, fiber_pid, PgBackend*)`, look up the `PgBackend*` to
deliver the interrupt. This is the single change that flips our verdict. It is
ABI-additive (new functions; the struct grows but is heap-internal).
Caveat worth documenting: the stored `void *` is caller-owned and may dangle if
the entry outlives it — same contract as any intrusive map.

**(2) A pid-indexed userdata getter in xtc_proc.** Orthogonal but also
sufficient: if `xtc_proc_userdata_of(xtc_pid_t)` existed (read another proc's
stored userdata by pid, with a generation check so a stale pid returns
`XTC_E_NOTFOUND`), we could keep plain `xtc_reg` for name->pid and recover the
`PgBackend*` via the userdata we already set. This is a bigger primitive-level
change (cross-proc read of per-proc state, lifetime/gen questions) and probably
less clean than (1), but it would help any consumer that associates a struct
with a fiber, not just a registry.

**(3) Make the crash-aware drop usable under fail-stop embedders.** This one is
about `xtc_reg`'s value proposition, not our blocker: the reaper's auto-drop is
only useful to embedders whose dead entries are hazards AND who do not fail-stop
on crash. Pairing it with the `xtc_orc` per-child-DOWN hook we requested
separately (`/tmp/libxtc-orc-per-child-down-hook-2026-11-04.md`) would let a
supervised, non-fail-stop embedder get both the classified DOWN and the registry
drop from one event. Not needed by us (we fail-stop), noted for completeness.

## What we actually shipped

No `xtc_reg` adoption. The hand-rolled `PgBackendId -> PgBackend *` registry
stays (it is correct, minimal, and the right shape for pointer-keyed interrupt
dispatch under fail-stop), plus a one-line invariant comment at its lock site.
If (1) lands, we would revisit: a value slot is the whole difference between
"poor fit" and "drop our map and use xtc_reg."

## Not established / scope

We did not prototype the value-slot change in `reg.c` (we do not fork your
registry). The `reg_node` layout and `whereis` pid-only return are read from the
pinned source; the proposals are from a consumer's vantage, not a tested patch.
Our own registry behaviour is verified in our tree.

## Environment

libxtc `6dd001e0` (v2.1.1-6), io_uring, Linux. Consumer: PostgreSQL xtc branch;
threaded backend registry keyed by `PgBackendId`, value `PgBackend *`, consulted
by `PgBackendSendInterruptById` for cancel/terminate/barrier delivery; crash
policy is whole-process fail-stop.
