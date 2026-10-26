# AGENTS.md — hash-metapage-overflow-signal

You are picking up a PostgreSQL patch in progress with **zero prior context**.
Read this whole file before doing anything. It tells you what the change is,
why, the state of the upstream discussion, how to build and test it (which must
happen on an EC2 box, not locally), the methodology for the numbers, what is
left to do, and the traps already hit.

## 0. STATUS (2026-10-26) — read this first; sections 3, 4 and 7 describe the
## abandoned POC and are kept only as history

- v1 is done: commit "Don't wait for the hash index metapage lock just to
  count a tuple" on top of the 3 local commits (2 `ci:` + `dev: local
  dev-setup`), pushed to `origin/hash-metapage-overflow-signal`. Produce the
  patch with `git format-patch -v<N> <dev-setup-commit>` (i.e. `HEAD~1` while
  the patch is one commit) so the ci/dev commits never appear in the set.
- The overflow-signal POC was dropped: pending counts die with the backend and
  VACUUM only recounts when it removes tuples, so `pgbench -C` drove ntuples
  to 487 for 200k rows (maxbucket 7). Design now: `ConditionalLockBuffer` on
  the metapage; on failure defer the count in a backend-local dynahash keyed
  by index OID; wait anyway after adding an overflow page or at 32 pending.
  Metapage block ref omitted from XLOG_HASH_INSERT when deferred; increment
  carried as block-1 data when >1.
- Results, scripts, raw output: `/scratch/hegel-pg/hash-metapage/v1/`
  (RESULTS.md). 2.04x at 64 clients, 2.23x at 128; count ~1% low worst case.
- Email draft for the thread: `/tmp/hash-metapage-v1-reply.eml` (never send
  email yourself; Greg sends).
- Tests: `src/test/modules/hash` (`hash_deferred_count` regress via injection
  point `hash-insert-defer-count`, `t/001_concurrent_inserts.pl` TAP).
- `wal_consistency_checking=hash` still fails on unpatched upstream master.
- Next: review feedback from the list; XLOG_PAGE_MAGIC bump left to committer.

## 1. What this is

A performance fix for PostgreSQL **hash indexes**: concurrent single-row
`INSERT` throughput stops scaling at a low client count and then goes backwards,
because every insert takes an exclusive buffer lock on the index **metapage**
(block 0) just to bump a tuple counter and test the split threshold. All
inserters serialise on that one buffer.

This worktree carries a **proof-of-concept** of the fix (the "overflow signal"
approach), applied on top of `origin/master`. It is POC quality, not submission
quality. Your job is to turn it into a real, submittable patch (see section 7).

Branch: `hash-metapage-overflow-signal`, tracking `origin/master`.
Repo: a git worktree under `~/ws/postgres/` of the bare repo `~/ws/postgres`
(`.git` is shared). `origin` is `git@github.com:gburd/postgres.git`; its
`master` tip carries two fork-CI commits on top of upstream master — the actual
PostgreSQL source is current upstream. `upstream` remote is
`https://git.postgresql.org/git/postgresql.git`.

## 2. The bug, precisely

`_hash_doinsert()` in `src/backend/access/hash/hashinsert.c`, on EVERY insert:

    LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
    ...
    metap->hashm_ntuples += 1;
    do_expand = metap->hashm_ntuples >
        (double) metap->hashm_ffactor * (metap->hashm_maxbucket + 1);

So every inserter contends on the metapage's exclusive content lock. The v10
metapage cache (293e24e5078) removed the metapage *read* from lookups but left
this *write* on insert. Measured: at 32 concurrent inserters, ~68% of hash
inserter samples are in the buffer-content-lock wait, and one buffer (the
metapage) accounts for 99.9% of the exclusive-lock wait time.

`hashm_ntuples` only drives split *timing*. VACUUM recomputes it from scratch
(`hashbulkdelete` sets `hashm_ntuples = num_index_tuples`), so a stale counter
costs at most a slightly delayed split, never correctness.

## 3. What the POC does (the "overflow signal")

Key idea: an insert that had to **allocate a new overflow page** already knows
its bucket is over-full — that is a *local, lock-free* signal that the index
wants more buckets. Use that instead of the global counter on the hot path.

Changes (all in `src/backend/access/hash/`):

- `hashinsert.c`:
  - Each backend keeps a small fixed array of per-index pending insert counts
    (`hash_pending[]`, keyed by relid, `hash_pending_slot()`).
  - **Common path** (tuple fits without a new overflow page): do NOT lock the
    metapage, do NOT bump `hashm_ntuples`, do NOT register the metapage in the
    insert's WAL record. Just `(*pending)++`. `do_expand = false`.
  - **Overflow path** (`added_ovfl` — `_hash_addovflpage()` was called): the
    metapage is already exclusive-locked by `_hash_addovflpage()`, so flushing
    costs nothing extra there. Flush `*pending + 1` into `hashm_ntuples`, clear
    pending, test the split threshold, register the metapage in WAL, carry the
    flushed delta in the record (`xl_hash_insert.ntup_delta`).
  - `_hash_expandtable()` (unchanged) re-checks `hashm_ntuples` against the
    threshold itself and DECLINES a split the count does not warrant. This is
    the guard against a duplicate-key split loop: a bucket that overflows only
    because of duplicate keys (which a split cannot separate) asks for a split
    and is told no.
- `hash_xlog.h`: `xl_hash_insert` gains `uint32 ntup_delta` (0 unless this
  record flushed a batch); `SizeOfHashInsert` updated.
- `hash_xlog.c` (`hash_xlog_insert` redo): the metapage (block 1) is now only
  present on records that flushed, so the block-1 read is guarded with
  `XLogRecHasBlockRef(record, 1)`, and `buffer` is reset to `InvalidBuffer`
  after the block-0 release so the block-1 cleanup is a no-op when absent. Redo
  adds `xlrec->ntup_delta` instead of a hardcoded `+= 1`, so the standby
  advances `hashm_ntuples` in the same batched steps as the primary.

A pending count lost at backend exit is harmless (VACUUM recomputes
`hashm_ntuples`).

## 4. Measured results (already done — do not need to redo to proceed)

EC2 c7i.8xlarge, 32 vCPU, one NUMA node, data on tmpfs, master built -O2
no-assert, pgbench single-row INSERT, 2M preload, `synchronous_commit=off`,
20s/point, best of 3:

    clients        1      16      32      64
    master       38810  361819  288961  262614   (peaks at 16, DECLINES)
    overflow     40743  487632  558926  588115   (keeps scaling)

2.24x at 64 clients; master's contention decline is gone.

Adversarial case 1, delete-heavy (insert 2M, delete 90%, insert 2M): overflow
uses FEWER pages/buckets than master (16695/11980 vs 18692/13030) — it does not
over-split after deletes.

Adversarial case 2, high-duplicate (2M rows, 50 distinct keys): no split loop;
fewer buckets than master (4931 vs 6515). Direct check: 500k inserts of ONE key
gives `maxbucket=1229` (normal for 500k tuples, not an explosion).

Correctness: identical row counts to master on mixed load; index probes equal
seqscan on unique and heavy-duplicate values; a streaming standby replays clean
and ends at the same row count with matching probes.

Full write-up and the raw numbers: `/scratch/hegel-pg/hash-metapage/RESULTS.md`
(same machine family you'll benchmark on). The patch as benchmarked is
`/scratch/hegel-pg/hash-metapage/overflow-signal-prototype.diff` (identical to
what's applied here).

## 5. Upstream discussion (pgsql-hackers)

Thread: "Hash index inserts serialise on the metapage tuple counter"
Root: https://www.postgresql.org/message-id/ats7u1ISmgZaWcge@floki

- Greg's original post: problem, measurements, the batched-counter POC, and
  three open questions (where the pending count should live; how stale the
  split trigger may be; whether to drop the global counter entirely in favour
  of a local overflow signal — which is what this worktree implements).
- David Geier replied: +1, but disliked batched counting (involved;
  non-deterministic/workload-dependent split timing; an "inverse scaling"
  staleness-vs-concurrency problem). He proposed using overflow-pages-vs-bucket
  -pages as the signal, naming two weaknesses: deletes leave partial pages;
  splitting a bucket full of duplicate keys won't reduce overflow.
- Greg replied with the overflow-signal prototype and the section-4 numbers,
  which answer David's two weaknesses with measurements (delete case splits
  LESS; duplicate case does NOT loop, because `_hash_expandtable`'s threshold
  re-check is the guard).

Consensus direction: the overflow signal is the better answer than batched
counting. Greg committed to turning it into a real patch — that is this
worktree's purpose.

A note from a *different* thread that matters here: "WAL record CRC calculated
incorrectly because of underlying buffer modification". Under
`wal_consistency_checking='hash'` a standby can fail with "incorrect resource
manager data checksum" — this is a PRE-EXISTING bug (REGBUF_NO_CHANGE taking an
unsafe full-page image), NOT caused by this change; unpatched master fails the
same way. When you test the standby, keep `wal_consistency_checking` OFF (or
expect that orthogonal failure and do not blame this patch for it).

## 6. How to build and test — ON EC2, NOT LOCALLY

**Do not build on this machine (floki).** Edit here, rsync to an EC2 Debian
box, build and test there. This is a hard rule for this project.

Build options used (release, for benchmarking):
    meson setup BUILD SRC -Dcassert=false -Ddebug=false -Doptimization=2 \
        -Dllvm=disabled -Dtap_tests=disabled
    ninja -C BUILD && meson install -C BUILD --destdir BUILD/inst
For a correctness/debug build use `-Dcassert=true -Ddebug=true` instead.

Linux note: meson installs libpq to a multiarch subdir; put BOTH
`$INST/usr/local/pgsql/lib/x86_64-linux-gnu` and `$INST/usr/local/pgsql/lib` on
`LD_LIBRARY_PATH`.

### Launching a box
Benchmarks need >=32 vCPU to show the contention cliff (16 is too small). Use a
**c7i.8xlarge** (32 vCPU, one NUMA node) for the scaling numbers; a smaller box
is fine for correctness-only checks. AWS profile `kiwi`, region us-east-2,
Debian 13 amd64 AMI `ami-09e50693bafe8b57a`, user `admin`. VPC
`vpc-03d813ac7ce964191`, subnet `subnet-01e4020d9df567be7`. Tag every resource
`Owner=gburd,Purpose=pg-prop-hegel`. SSH with `-o IdentitiesOnly=yes`. Put the
data dir on a tmpfs (`mount -t tmpfs -o size=48G tmpfs /mnt/pgtmp`) to take the
disk out of the measurement. **Tear the box down when done** (terminate, delete
the security group and key pair, shred the .pem). There is a benchmark harness
you can copy from `/scratch/hegel-pg/hash-metapage/` and the helper scripts
pattern in `/scratch/hegel-pg/ec2/`.

### Benchmark recipe (reproduces section 4)
1. Build master and this branch side by side (two worktrees / two installs).
2. pgbench script: `\set v random(1,1000000000)` then
   `INSERT INTO h VALUES (:v);`
3. Per client level (1, 16, 32, 64): fresh table, hash index, preload 2M rows,
   `pgbench -n -c N -j N -T 20`, best of 3, re-preload between runs.
4. Server: `fsync=off synchronous_commit=off shared_buffers=8GB
   max_wal_size=16GB`.

### Correctness checks (must pass)
- Mixed load (unique + heavy-dup + deletes): row count and index-probe-vs-
  seqscan must match master exactly, for both a unique value and a
  heavy-duplicate value.
- Standby replay: base-backup a streaming standby, run 8 concurrent inserters
  that force splits/overflow, then confirm the standby replays with no crash,
  ends at the same row count, and index probes match the primary. Keep
  `wal_consistency_checking` OFF (see section 5).
- Split-loop watch: 500k inserts of a single key; `maxbucket` (via
  `hash_metapage_info(get_raw_page('idx',0))`, pageinspect) must stay the
  normal size (~1200), not explode.
- `make check` / `meson test` (at least the `hash_index` and `hash` regression
  tests, and the isolation tests).

## 7. What is left to make this submittable

1. **Pending-count storage.** The POC uses a fixed 64-slot backend-local array
   keyed by relid and silently drops the count on backend exit and on slot
   eviction when more than 64 hash indexes are touched. Decide the real home:
   the relcache entry (flushed on relcache invalidation and at transaction end)
   is the obvious candidate, but has edge cases (invalidation dropping the
   count, subtransaction abort). Whatever you choose, losing the count must stay
   correctness-neutral (it already is: VACUUM recomputes `hashm_ntuples`).
2. **WAL record change review.** Adding `ntup_delta` to `xl_hash_insert`
   changes the record format. Confirm this is acceptable (it only grows the
   fixed part by a uint32) and that `pg_waldump`/`XLOG_HASH_INSERT` decoding
   still prints sensibly. Consider whether the delta even needs to be in the
   record or whether redo can derive it (it cannot trivially — the standby has
   no per-backend pending state — so carrying it is probably right).
3. **Split-timing semantics.** Document and defend the new rule: splits trigger
   on overflow-page allocation, gated by `_hash_expandtable`'s existing
   threshold re-check. Confirm there is no workload where this delays splitting
   so much that overflow chains grow unboundedly (the threshold re-check should
   prevent this, but show it).
4. **Back-patch question.** Likely master-only (it changes a WAL record). State
   that in the submission.
5. **Regression/isolation test** covering concurrent inserts that split, and a
   standby replay test, so the behaviour is pinned.
6. Rebase onto current `origin/master` before posting; re-run section 4 numbers
   on the rebased tree.

## 8. Traps already hit (do not re-discover these)

- The redo function `hash_xlog_insert` originally read block 1 (the metapage)
  unconditionally. With the metapage absent from common-path records, that
  PANICs: "failed to locate backup block with ID 1 in WAL record". Fixed with
  `XLogRecHasBlockRef(record, 1)` — already in the patch.
- After guarding the block-1 read, a stale `buffer` handle from the block-0
  path caused a double release: "buffer 0xN is not owned by resource owner
  AuxiliaryProcess". Fixed by resetting `buffer = InvalidBuffer` after the
  block-0 `UnlockReleaseBuffer` — already in the patch.
- tmpfs for the data dir fills fast with multiple data dirs + large
  shared_buffers; size it generously (48G on a 64G box) and clean up old data
  dirs between runs, or initdb fails with "Directory nonexistent".
- `wal_consistency_checking='hash'` surfaces an unrelated pre-existing CRC bug
  (section 5). Do not let it mask or be blamed on this change.
- Build on EC2, never on floki.

## 9. Commit / submission conventions

- Commits: `--no-verify`, `-c commit.gpgsign=false`,
  `GIT_AUTHOR_*`/`GIT_COMMITTER_*` = `Greg Burd <greg@burd.me>`, imperative
  ASCII subject, no agent attribution. `export GIT_PAGER=cat GIT_EDITOR=true
  GIT_TERMINAL_PROMPT=0`.
- Produce the patch with `git format-patch -v<N> origin/master` and post to
  the existing hackers thread (section 5), not a new thread.
- Prose for the submission email follows the stop-slop discipline: no
  throat-clearing, no inflated register, state the measured facts.
