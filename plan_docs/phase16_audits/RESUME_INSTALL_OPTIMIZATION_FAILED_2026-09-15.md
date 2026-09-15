# The "skip the 134 hot-field installs on fiber resume" optimization FAILED -- reverted

Date: 2026-09-15. Recording a negative result so nobody re-tries it from the same reasoning.

## The hypothesis (from WHY_GUCS_ON_FIBER_RESUME.md)
Hot FIELDS and hot MIRRORS carry an owner token and their generated readers compare it against the
current root, falling back to a re-derive on mismatch. I therefore reasoned they are *self-healing*, so
a fiber resume could repoint the six roots, skip reinstalling all 134 field slots (and the mirrors), and
let each stale slot fix itself on first touch -- paying only a handful of fallbacks instead of 134 eager
writes per park/wake.

Implemented as `PgRuntimeRefreshCurrentWorkInternal(rebind, full)` +
`PgRuntimeSetCurrentWorkInternal(..., full)`, with `PgRuntimeRestoreCurrentWorkLazy()` (the fiber
wait/resume path) passing `full = false`, and keeping the non-owner-validated families (1 hot cell +
122 hot buckets) installed unconditionally.

## Result: 3.5x WORSE, so reverted
Read-only pgbench at c=64, same box, same config, immediately before/after:

| build | read tps |
|---|---|
| baseline (eager install on resume) | **34,545** |
| with the skip-installs optimization | **9,938** |

That is a ~3.5x regression, not a win. Reverted (`git checkout` -- the three genuinely-good wait-path
fixes in `41a97e3d94` are untouched and remain in).

## Why the reasoning was wrong
The owner token is compared against `bridge->owner`, and `bridge->owner` is one of the ROOTS that the
resume path *does* update. So after a resume every skipped slot's stored token disagrees with the
freshly-installed root **permanently** -- nothing ever re-installs it, because the install is exactly
what I removed. The slot does not "heal on first touch": the reader takes the fallback on first touch
*and on every touch thereafter*. I traded 134 cheap eager pointer writes for an unbounded number of
fallback calls on the hottest read paths in the backend.

"Self-invalidating" was the correct reading of the reader; "self-*healing*" was my unjustified leap.
Detecting staleness is not the same as repairing it.

## Measurement caveat discovered at the same time (important)
While chasing this I found the dev box was oversubscribed ~4x: load average **32.9 on 8 cores**, with
**eight** of my own leftover test servers still running at 74-148% CPU each (several spinning on wedged
fibers) plus another agent's compiler. A post-revert read run measured **5,847 tps** -- *lower* than
both numbers above -- proving absolute throughput figures taken today are not comparable across time.
The 34,545 vs 9,938 comparison above is still usable because both were taken back-to-back under the
same load, but any single number from this session should be treated as indicative only.

Rules for the next perf attempt:
- kill every prior test server before measuring (`postmaster.pid` per datadir; check `uptime` first and
  wait for load to settle), and re-check `uptime` after the run;
- always measure A/B back-to-back on the same box, never against a number from a previous session;
- prefer a dedicated EC2 box for any number that will be quoted.

## Side finding worth its own fix: wedged fibers ignore shutdown
All seven wedged servers **survived both SIGTERM (fast shutdown) and SIGQUIT (immediate)** and required
SIGKILL. A backend fiber stuck in the buffer-lock wedge never processes the shutdown request, so the
postmaster cannot bring the cluster down. For a supervised runtime that is a real defect in its own
right -- the whole point of the supervisor tree is that a stuck child is detectable and killable. Worth
tracking separately from the wedge itself: even after the wedge is fixed, an unresponsive fiber must not
be able to block cluster shutdown.

## Where this leaves the resume cost
The eager install stays for now. If the per-resume cost is to be reduced, the viable direction is the
one item 3 of WHY_GUCS_ON_FIBER_RESUME.md named: make the reader repair the slot itself (write the
freshly-derived pointer + current owner back into the bridge on a fallback), which would make skipping
the eager install genuinely safe. That is a change to the generated accessors, not to the refresh
function, and it must be measured the same A/B way.
