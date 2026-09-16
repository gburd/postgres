# WEDGE ROOT CAUSE (v1.47.0, trustworthy tools): one io_uring ring wedges at CQ-full + overflow

Date: 2026-09-16. Found on libxtc v1.47.0 with the `park_reason` mailbox fix in place -- which is what
made this visible: with mailbox parks correctly labelled and excluded from suspicion, the remaining
anomaly stood out immediately.

## The chain, end to end
1. **One loop's ring is permanently stuck.** `xtc-rings`, 3 samples 5 s apart, bit-identical:
   `loop 7  unreaped=512  ovf=1` while loops 0-6 are all `unreaped=0 ovf=0`. CQ full **and** the kernel
   overflow flag set -- the stuck-ring signature the tool itself documents (it warns that `unreaped>0`
   alone is normal for a busy ring and must be read together with `ovf`).
2. **A fiber on that loop is parked forever.** `xtc-procs`, 3 samples 4 s apart, identical:
   `7.3.1 PARKED(fd 1364)` -- loop 7, the wedged ring.
3. **Its fd is genuinely ready.** fd 1364 is the backend's epoll fd; `/proc/<pid>/fdinfo/1364` shows
   `tfd: 1329 events: 19` (`EPOLLIN|EPOLLERR|EPOLLHUP`), and `ss -x` shows that client socket with
   `Recv-Q=116` -- the client's next command is sitting unread. Registration correct, data present,
   epoll fd therefore readable, fiber parked on it, wake never delivered.
4. **Exactly 2 sockets have unread data and exactly 2 sessions are stuck** (`Recv-Q` 116 and 75; fds
   1329/1337) -- a 1:1 correspondence, not a coincidence.
5. **Those 2 sessions hold row locks while `idle in transaction` on `ClientRead` for 2m26s**
   (`idle_for == xact_age`: they went idle immediately and never returned), so 62 others queue behind
   them (`Lock/tuple` 45, `Lock/transactionid` 17) and tps goes
   1377 -> 1476 -> 1379 -> 124 -> **0.0 forever**.

## Why this supersedes every earlier attribution in this directory
The heavyweight-lock pile-up (`tuple`/`transactionid`), the `BufferExclusive` waiters, the "held buffer
content lock", the "backend leak", and the "supervisor mailbox lost wake" were **all downstream of this
one stuck ring**. Each was a real observation; none was the cause. This is the fifth attribution and the
first one that explains every observation simultaneously, including why process mode is unaffected
(~3,060 tps on the identical workload, no io_uring in its client-read path).

## Reported, not worked around
Filed `/tmp/libxtc-io-uring-ring-wedges-cq-full-overflow-2026-09-16.md`. Per the standing directive we
are NOT adding a PG-side mitigation (e.g. a park timeout to paper over the missed wake, or steering
sessions off loop 7): a ring that reaches CQ-full-with-overflow and never drains is a runtime contract
failure, and a timeout would convert a hard hang into an invisible latency cliff while leaving the ring
broken.

Also asked for two observability improvements, both the same shape as the mailbox mislabel they just
fixed -- the runtime **has** the facts, the tool does not join them:
- `xtc-stranded` reports `0 suspect` while two fibers are provably doomed, because an fd park with a
  source looks healthy. Cross-referencing a parked fd's owning loop against that loop's `ovf`/`unreaped`
  would have identified this in one command.
- A debug-build warning when `ovf` stays set across N poll iterations with no CQ progress.

## Methodology that finally worked (worth keeping)
- **Trust no single instrument's negative.** Four of my five wrong attributions came from believing one
  tool's "nothing here". The winning move was cross-checking four independent sources that must agree:
  kernel state (`/proc/<pid>/fdinfo`, `ss -x Recv-Q`), libxtc state (`xtc-rings`, `xtc-procs`), PG state
  (`pg_stat_activity`), and the workload's own throughput trace.
- **Sample 3x.** Both the ring counters and the park set were sampled three times before any claim; the
  tool explicitly asks for this and it is what separates "busy" from "stuck".
- **Fresh initdb per run, always.** A reused server had 4 of my own killed pgbench clients left
  `idle in transaction` holding row locks, which manufactured a fake wedge and cost a full round of
  analysis.
- **Watch the whole run.** The wedge appears at ~90 s at c=64; my earlier 80 s probe window kept missing
  it and I twice concluded "no wedge" from a run that wedged 10 s later.

## Next
Blocked on the libxtc fix for the wedge. Meanwhile the productive adjacent work is the resource-tracking
prerequisite for per-fiber kill (register LWLocks, buffer content locks/pins and WAL insertion slots with
`xtc_scope`/`xtc_proc_at_exit`), which v1.47.0's `xtc_exit_pid_deadline` + `mask_depth` reporting now
makes measurable -- and which is what the owner asked for after the `quickdie()` discussion.
