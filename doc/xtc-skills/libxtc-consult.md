---
name: libxtc-consult
description: >
  Write a report to the libxtc team about a suspected runtime bug, or evaluate a new libxtc
  release for whether it addresses our blockers. Encodes the evidence standard this
  collaboration converged on after ten retractions: every claim carries a control, absence
  claims are checked against eviction, and hypotheses are tested before they are sent. Use
  when filing a libxtc issue, replying to their message, adopting a new pin, or deciding
  whether a fix actually landed. Triggers on: "message from the libxtc team", "reply to
  libxtc", "new libxtc version", "file a libxtc bug", "does this fix our issue",
  "write up what we need".
---

# Consulting the libxtc team

This collaboration works because both sides run controls and retract fast. Ten retractions
so far — **every single one an instrument that could not distinguish two states**, not
careless reading. Uphold that standard; it is why they respond to specifics.

## Before you send: the four controls

Run these on your own claim. Each has caught a false headline at least once.

1. **Does the event fire at all in this build?** Grep the kind histogram. A trace captured by a
   build older than the event *cannot contain it*, and a classifier will report the
   corresponding bucket at 100% with total confidence.
   ```
   python3 tools/xtc-tail.py t.xtcl | grep -oE "SCHED [A-Z_]+" | sort | uniq -c
   ```
2. **Does the join actually join?** Compute overlap on healthy cases. If ~92% of parked tasks
   join to a WAKE, the join works and the unjoined ones are real. If ~0%, your query is broken.
   *This is the check whose absence produced an invalid pid-keyed join.*
3. **Could the absence be eviction?** `xtc-tail-dropped` first. Then argue direction: eviction
   removes the OLDEST, so an event that would be *newer* than your reference would have been
   retained. Add a positive control showing events of that kind survived in the same region.
4. **Can you test your own hypothesis before handing it over?** If yes, do. A hypothesis sent
   as "the one query that would settle it" that you could have run yourself wastes a round trip
   — and one such (cross-ring mismatch) turned out **refuted** when finally run, 59/64 against.

## Report structure that works

1. **The answer up front**, with the numbers.
2. **How it could have been wrong**, and the controls that rule that out.
3. **Corrections to your own prior claims** — a table of every withdrawn headline and why.
   They have never once objected to a retraction; they have objected to unfalsifiable claims.
4. **What is NOT established.** Label inference as inference. "I am not going further than that"
   is a legitimate and respected ending.
5. **The narrowest next ask**, ideally one instrument or one line.
6. **Environment block**: GUCs, instance type, filesystem, scale, client count, dump delay,
   hang rate, libxtc rev.

## Adopting a new release: the checklist

1. **Is it actually tagged?** `git ls-remote --tags`. They deliberately ship untagged HEADs and
   ask for use-in-anger first — a version number in a message is not a tag.
2. **Verify each claim in the source, not the note.** They have twice described something they
   had not shipped (a man-page correction that never touched `man/`; a documented join that was
   never emitted). Check the emit sites, count them, confirm the field.
3. **Is the pinned checkout clean?** `git status --short` — local probes from an earlier
   investigation silently contaminate a tarball. Verify the shipped artifact:
   `tar xzOf lib.tar.gz ./src/... | grep -c MARKER`.
4. **Full 40-char SHA in `flake.nix`** — a short rev fails with "wrong length for hash algorithm".
5. **Can old traces answer the new question?** If the release adds an event, traces from before
   it **cannot** contain it. Re-capture.
6. **Record the constraint in the flake.nix comment**, not just a commit message.
7. **Distinguish instrument from fix.** Most recent libxtc releases have been instruments, said
   so explicitly, and did not change the hang rate. Report the rate either way.

## Mask bits vs event kinds

`XTC_TAIL_*` names both, with no visual distinction — a known naming wart. Mask bits are
`SCHED (1<<0)`, `MSG (1<<1)`, `IO (1<<2)`, `OS (1<<3)`. Everything else (`WAKE=2`, `RUN=3`,
`PARK=4`, `LOOP_POLL=8`, `PARK_TASK=9`, `REAP=10`, `SUBMIT=11`, `SUBMIT_FAIL=12`) is a **kind**
riding one of those bits. ORing a kind into `xtc_tail_enable()` is wrong.

## When you need something they do not provide

Write `/tmp/libxtc-<topic>-<date>.md` and be concrete:

- **What we are trying to do**, in their vocabulary (fibers, loops, carriers, mailboxes).
- **What we hand-rolled instead**, with the file and line count — the cost is the argument.
- **The specific semantic that blocks adoption.** Not "it does not fit" but e.g. "our DOWN
  classifier distinguishes genuine-crash from clean logical exit from FATAL; if the supervisor
  flattens these we lose the fail-stop trigger."
- **The invariant we cannot give up**, and why (e.g. a crashed backend may have corrupted shared
  memory, so backend children must be `TEMPORARY` — auto-restart is unsafe, not merely
  undesirable).
- **What a minimal API addition would look like** from our call site.

## Lessons from their side worth reusing

- **A gate can be negative-tested and still miss the case if the discriminating input is not in
  it.** Their planted-bug gate passed while the classifier was broken, because their synthetic
  fibers stranded after a handful of cycles and the bug needed a long healthy history.
- **A synthesized control is only a control if its shape matches what the system actually
  emits.** They synthesized SUBMIT *after* the park; real code emits it *before*. The gate
  encoded their assumption and certified the bug.
- **An instrument that returns a clean bill of health for the path under suspicion is worse than
  no instrument.** Their `SUBMIT_FAIL` check was `rc < 0`, but `io_uring_submit` returns the
  *count consumed* — a short submit read as success and produced exactly the symptom under
  investigation.
- **A counter that means two things without saying so** is how several retractions happened:
  `unreaped` saturates at CQ size (so `0` ≠ empty) *and* is nonzero on any busy ring (so `>0`
  ≠ stuck); `LOOP_POLL` is idle-only (so `0` polls means "quiet" *or* "always busy").

## Filing location

Reports go in `/tmp/libxtc-<topic>-<date>.md`, and a copy into
`plan_docs/phase16_audits/` so the repo carries the record. Add **SUPERSEDED banners** to
prior docs when a later measurement overturns them — do not silently leave a wrong conclusion
in the tree for someone to find.
