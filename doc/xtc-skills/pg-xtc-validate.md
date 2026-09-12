---
name: pg-xtc-validate
description: >
  Build, test and validate the multithreaded/libxtc PostgreSQL branch on EC2 without
  producing false green or false red results. Covers the trustworthy-gate question (which
  test targets actually mean anything right now), the known pre-existing bug stack, the
  build recipe, and the harness traps that have each fabricated a wrong answer at least
  once. Use before claiming any change is validated, and before interpreting a threaded
  test failure. Triggers on: "gmake check-threaded", "is this green", "validate the
  branch", "check-threaded-pooled", "did my change regress", "245/245", "threaded
  regress failing".
---

# Validating the pg-xtc branch

## Which gates actually mean something

**`gmake check` (process mode, expect 245/245) is currently the ONLY trustworthy green gate.**

Every threaded target fails on clean HEAD from a *stack* of pre-existing bugs, each masking the
next. Read `plan_docs/phase16_audits/THREADED_TEST_BASELINE_2026-09.md` before interpreting any
threaded result — and re-baseline after any fix, because the wall moves.

Known stack (check the doc for current status):

1. `guc.c` `ThreadedGUCUnlock` unconditional `RESUME_INTERRUPTS` — **any invalid `SET` crashed the server** — FIXED
2. `reloptions.c` same unwind hazard — FIXED
3. `dfmgr.c` rendezvous-hash race — FIXED
4. checkpointer/backend-fiber **buffer-lock ABBA deadlock** on `create_index` (`REINDEX CONCURRENTLY`)
5. `mcxt.c` `Assert(parent->firstchild == context)` via `MemoryContextDelete` ← `AtEOXact_RI`
6. `DROP SUBSCRIPTION` hangs in `logicalrep_worker_stop_internal` (apply worker never observes SIGTERM)

Bugs 1–3 were the same defect in three places: a threaded critical section pairing
`HOLD_INTERRUPTS()` with an unconditional `RESUME_INTERRUPTS()`, while `errfinish()` resets
`InterruptHoldoffCount` to 0 during **any** error unwind. Fix shape:
`if (InterruptHoldoffCount > 0) InterruptHoldoffCount--;`. **If you add a new threaded critical
section, use that pattern.**

### Reading a threaded failure
A mass failure where ~150 tests all fail at the *same* elapsed time (e.g. `3 ms`) is **one real
failure plus collateral** — the postmaster died and the rest never ran. Find the *first*
`not ok` and ignore the tail. Distinguish:

- **wedge**: identical elapsed times, server gone, `Bail out!`
- **real diffs**: differing times, actual `regression.diffs` content

Also expect **cosmetic plan-shape diffs** (`select_distinct`, `subselect`, `union`, `join`) on
runs that now get far enough to finish — EXPLAIN output, not wrong answers, from
autovacuum/ANALYZE timing under threaded scheduling. Check the diff is cosmetic before chasing.

## Config map

| GUC | meaning |
|---|---|
| `pooled_protocol_carriers = -1` | auto → core count (floor 8, ceiling 256). **Default.** |
| `= 0` | fiber-per-session |
| `> 0` | fixed stackless pool size |
| `pooled_protocol_fiber_sessions` | steers auto → 0. `boot_val false` (dormant) |

Test target configs: `check-threaded` → `threaded_smoke.conf` (**carriers=0, the fiber path with
the open lost-wake bug**, `io_method=sync`); `check-threaded-pooled` → `threaded_pooled.conf`
(carriers=4); `check-threaded-workers` → `threaded_workers.conf`.

**Do not assume `check-threaded-pooled` is the "healthy" lane** — it fails identically on clean
HEAD. That assumption was made and was wrong.

## Build recipe (EC2)

```bash
# NVMe, libxtc, then PG. Adapt .ec2/loop-poll-2026-09-07/xtcpg_build.sh
sudo mkfs.xfs -f /dev/nvme1n1; sudo mount /dev/nvme1n1 /mnt/nvme

# libxtc: FORCE -Dio-backend=uring.  `io-backend=auto` (the default) SILENTLY falls
# back to epoll when liburing-devel is absent (meson.build: elif have_uring ...
# elif have_epoll) -- producing an epoll-backed binary that is NOT the io_uring
# runtime you meant to benchmark, with no error.  An entire v1.44.1 benchmark was
# likely run epoll-backed this way and had to be discarded.  Install liburing-devel
# FIRST, pass -Dio-backend=uring so a missing liburing ERRORS, and VERIFY at runtime
# (io_uring ring fds in /proc/<pid>/fdinfo, or xtc-rings shows rings) -- do not trust
# the build log alone.
# libxtc: debugoptimized -- release strips the debug info the gdb helpers walk
meson setup build -Dtls=openssl -Dshared=true -Dbuildtype=debugoptimized -Dio-backend=uring
ninja -C build && sudo ninja -C build install
echo /usr/local/lib64 | sudo tee /etc/ld.so.conf.d/usrlocal.conf && sudo ldconfig

# PG
export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig:/usr/local/lib/pkgconfig
meson setup build --buildtype=debugoptimized -Dcassert=false -Dxtc=enabled \
  -Dlibxml=enabled -Dlibxslt=enabled -Dprefix=... -Dc_args="-fno-omit-frame-pointer -g"
ninja -C build && ninja -C build install
ls .../bin/postgres        # ALWAYS verify -- see trap 1
```

- **Never name a script `buildA.sh`** — that name collides with an unrelated project's
  OSv/docker build script and will silently run the wrong thing.
- `-Dcassert=true` for assert-only bugs; also verify a normal build.
- **`check-threaded*` are autoconf/make targets** (they pass `--temp-config=` to `pg_regress`).
  There is no meson `threaded` setup — `meson test --setup threaded` fails "not found".
  autoconf needs `libicu-devel` or `./configure` dies on ICU.
- `meson test --suite regress` alone fails "copying of initdb template failed" with an EMPTY
  `regression.diffs` unless you run **`meson test --suite setup` FIRST**.
- Use a **150 GB** root: the autoconf tree plus temp instances consumed 76 GB and silently
  broke a run on 80 GB. After killing a wedged run, `sudo lsof -nP | grep deleted` — orphaned
  `postgres` processes held **151 GB** of deleted-but-open files.

## The traps, each of which has produced a wrong answer

1. **`| tail -N` on a build hides failures.** A script printed "PG ok" while `ninja` had failed.
   Verify the binary exists, or re-run and expect "no work to do".
2. **Reusing one server across N benchmark runs.** After a timeout-kill it enters
   `could not fork new process for connection: Function not implemented` and every later run is
   garbage. This fabricated a 7/8 hang rate when the truth was 4/6. **Fresh server per run.**
3. **Unguarded `pgbench -i`** blocks a sweep forever. Wrap in `timeout`.
4. **Silent tps mis-parse.** A harness reported `NO_TPS` while the log held `tps = 41057`.
   Make parse failure loud and distinct from a real stall. Note plain `pgbench` does **not**
   print "maximum latency" — derive max/percentiles from `--log` per-transaction data.
5. **Assert the carrier count in effect** (`SHOW`, or `pg_stat_xtc_carriers` row count). Do not
   assume the GUC took. `pooled_protocol_carriers=auto` is **invalid** — the value is `-1`.
6. **Co-located load generator.** A foundational write-heavy RCA ran pgbench on the SUT;
   loopback symbols dominated both lanes and the "44% idle cores" headline was confounded.
   Driver on a **separate host**, recorded per row; a co-located run must be *tainted* in the
   output so it can never later be mistaken for a headline.
7. **Aggregate tps hides unfairness.** 103k tps at c=32 looked like clean scaling while 8
   sessions got everything and 24 got nothing; p99 was 0.097 ms in the same run whose max was
   30 s. **Always check per-client progress**, not just the aggregate.
8. **Multi-path `scp` copies nothing.** `scp host:'a b c' dest/` fails with "No such file or
   directory" and copies zero files. **One scp per file, then `stat` each.** An unverified fetch
   plus a premature teardown lost trace files that then had to be regenerated.
9. **Large tarball scp truncates deterministically.** Ship with
   `base64 f | ssh host 'base64 -d > f'` and verify md5.
10. **`local var=$1 x=$BASE/$var` breaks under `set -u`.** Split into separate `local` statements.
11. **heredoc-over-ssh with nested quotes mangles scripts.** Write locally, scp, verify `wc -l`.
12. **`pgrep -f "bash /tmp/x.sh"` matches your own ssh command line.**

## Focused targets (per AGENTS.md)

Doc-only: `git diff --check`. Ordinary code: `gmake check` + `gmake check-threaded`.
Runtime-root / lifecycle / GUC / teardown / worker / wait-boundary changes: **also**
`gmake check-threaded-workers`, `check-threaded-world-core`, `check-runtime-lifecycles`,
`check-global-lifetimes`.

New backend-runtime tests go in the **split** object-family files under
`src/test/modules/test_backend_runtime/`, never the old monolith.

## Benchmark methodology

`src/tools/benchmark/mtpg_p1_matrix.sh` encodes the fixed protocol: fresh server per run,
timeout-wrapped init, diagnostics before teardown, loud parse failures, carrier assertion,
enforced separate loadgen (hard `exit 2`, `--local-driver` taints every row), full per-row
confound context, SUT idle% only reported when the driver is remote. Prefer it over ad-hoc
scripts; if you must go ad-hoc, re-read the trap list above first.

## EC2 hygiene

Fresh key `xtc-<purpose>-<timestamp>`, Name tag == key name, `timeout` on every remote command.
Before terminating **verify `KeyName` == yours AND the `Name` tag == yours**. Then terminate,
verify terminated, delete the SG (retry ~25 s × 6 for `DependencyViolation`), delete the key,
`shred` the .pem, and sweep us-east-1/us-east-2/us-west-1/us-west-2/eu-west-1 for
`key-name=xtc-*`. The `bene` account is **shared** — never touch `fts-wand-*`, `solnix-*`,
`pgtv-team-*`, `numa-bench`/`bcs-jakub`, `osv-*`. Tear down even when the task fails.

## Repo hygiene

`commit.gpgsign` is true globally → set `git config commit.gpgsign false` repo-local, commit
with `--no-verify`. **Avoid backticks in `-m` messages** (bash command-substitutes them).
Never `rm -rf` (blocked) — `find <dir> -mindepth 1 -delete` then `rmdir`. Tab-heavy C files:
edit via python `str.replace` with `assert old in s`. **Commit incrementally** — several long
agent runs died before committing and lost hours.
