#!/usr/bin/env python3
#
# srclint - cheap, fast, pre-clang source lints for the thread-classification
# harness (F1.3 of docs/threading/F1_CLASSIFICATION_HARNESS.md).
#
# Unlike threadcheck.py (which drives the libclang-based pgguclifetimes /
# pg_static_vars checkers over the compile database), srclint is a pure
# text/regex scanner with no build dependency.  It runs in seconds on every
# PR and catches the bulk of thread-safety regressions before the heavier
# clang pass:
#
#   * s_globals  - a *new* bare mutable `static T x;` at file or function scope
#                  outside the allowlist, unannotated.  (Heuristic; the
#                  authoritative check is pgguclifetimes.)
#   * s_signals  - new raw signal()/sigaction()/sigprocmask() outside the
#                  port/signal layer.
#   * s_libc     - thread-unsafe libc calls (setlocale, strerror, getenv,
#                  setenv, strtok, rand, localtime, getopt) outside their
#                  thread-safe wrappers.
#
# Each finding is keyed as "relpath:symbol_or_call" (line-independent), so the
# baseline survives line-number churn and collapses duplicates.  The offender
# set is compared against a frozen baseline that may only shrink:
#
#   0  - no new offenders (set is <= baseline)
#   1  - new offenders appeared (CI failure); listed with file:line
#   2  - setup problem (source root not found)
#
# With --update-baseline the current set is written to the baseline file.
#
# Copyright (c) 2024, PostgreSQL Global Development Group
#
# src/tools/threadcheck/srclint.py

import argparse
import os
import re
import sys

# Repo-relative path substrings that are exempt from all lints: vendored
# third-party trees (own conventions) and the port/signal/locale layers that
# are *allowed* to call the raw primitives (they are the wrappers).
GLOBAL_EXEMPT = [
    "contrib/libxtc/",
    "src/backend/snowball/libstemmer",
    "src/include/snowball/libstemmer",
    "src/backend/regex/",
    "src/backend/utils/mb/Unicode/",
    "src/timezone/",
    "src/tools/",                  # the harness and other dev tooling
    "install/",                   # generated install prefix copy
    "tmp_install/",
]

# Files/dirs that legitimately own the raw signal primitives.
SIGNAL_EXEMPT = GLOBAL_EXEMPT + [
    "src/port/pqsignal.c",
    "src/backend/libpq/pqsignal.c",
    "src/backend/port/win32/signal.c",  # Windows signal emulation layer
    "src/backend/postmaster/",     # postmaster owns signal setup
    "src/backend/tcop/postgres.c", # backend signal setup
    "src/backend/utils/init/miscinit.c",
    "src/test/",
    "src/bin/",                    # frontend programs, single-threaded
    "src/interfaces/",
]

# Files/dirs that legitimately wrap the thread-unsafe libc calls.
LIBC_EXEMPT = GLOBAL_EXEMPT + [
    "src/port/",                   # pg_* wrappers live here
    "src/backend/utils/adt/pg_locale",
    "src/backend/commands/variable.c",
    "src/test/",
    "src/bin/",
    "src/interfaces/",
    "src/common/",
]

SIGNAL_RE = re.compile(r"\b(?P<call>signal|sigaction|sigprocmask)\s*\(")
LIBC_RE = re.compile(
    r"\b(?P<call>setlocale|strerror|getenv|setenv|putenv|strtok|"
    r"rand|srand|localtime|gmtime|asctime|ctime|getopt)\s*\(")
# Bare mutable file/function-scope static: `static <type> <name>` not const,
# not a function/forward decl, not an annotation macro.  Heuristic only.
GLOBAL_RE = re.compile(
    r"^\s*static\s+(?!const\b)(?!inline\b)"
    r"[A-Za-z_][\w\s\*]*?[\s\*]"
    r"(?P<name>[A-Za-z_]\w*)\s*(?:\[[^\]]*\])*\s*[=;]")

# Don't flag obvious non-mutable / annotated / control patterns.
GLOBAL_SKIP = re.compile(r"\b(const|PG_THREAD_LOCAL|pg_attribute_|"
                         r"PGDLLIMPORT|session_local|process_local|"
                         r"shared_global)\b")


def is_exempt(rel, exempt):
    return any(ex in rel for ex in exempt)


def iter_sources(source_root):
    for dirpath, dirs, files in os.walk(source_root):
        # prune .git and obvious build dirs early
        dirs[:] = [d for d in dirs
                   if d not in (".git", "install", "tmp_install")
                   and not d.startswith("build")]
        for fn in files:
            if fn.endswith((".c", ".h")):
                yield os.path.join(dirpath, fn)


def scan(source_root):
    """Return {check: {key: 'rel:line'}} for all three lints."""
    findings = {"s_globals": {}, "s_signals": {}, "s_libc": {}}
    src_prefix = os.path.realpath(source_root) + os.sep
    for path in iter_sources(source_root):
        rp = os.path.realpath(path)
        rel = rp[len(src_prefix):] if rp.startswith(src_prefix) \
            else os.path.relpath(rp, source_root)
        do_glob = not is_exempt(rel, GLOBAL_EXEMPT)
        do_sig = not is_exempt(rel, SIGNAL_EXEMPT)
        do_libc = not is_exempt(rel, LIBC_EXEMPT)
        if not (do_glob or do_sig or do_libc):
            continue
        try:
            with open(path, encoding="utf-8", errors="replace") as f:
                lines = f.readlines()
        except OSError:
            continue
        for lineno, line in enumerate(lines, 1):
            # strip trailing line comments cheaply (good enough for a grep-lint)
            code = line.split("//", 1)[0]
            if do_sig:
                m = SIGNAL_RE.search(code)
                if m:
                    key = "%s:%s" % (rel, m.group("call"))
                    findings["s_signals"].setdefault(
                        key, "%s:%d" % (rel, lineno))
            if do_libc:
                m = LIBC_RE.search(code)
                if m:
                    key = "%s:%s" % (rel, m.group("call"))
                    findings["s_libc"].setdefault(
                        key, "%s:%d" % (rel, lineno))
            if do_glob:
                m = GLOBAL_RE.match(code)
                if m and not GLOBAL_SKIP.search(code):
                    key = "%s:%s" % (rel, m.group("name"))
                    findings["s_globals"].setdefault(
                        key, "%s:%d" % (rel, lineno))
    return findings


def load_baseline(path):
    out = set()
    if not os.path.exists(path):
        return out
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                out.add(line)
    return out


def write_baseline(path, keys, total):
    with open(path, "w") as f:
        f.write(
            "# srclint baseline (F1.3): pre-existing thread-unsafe source\n"
            "# patterns. Keyed as '<check> relpath:symbol'. This list may only\n"
            "# shrink: CI fails if a new offender appears. Regenerate with\n"
            "# 'srclint --update-baseline'.\n"
            "# Total: %d\n" % total)
        for k in sorted(keys):
            f.write(k + "\n")


def main():
    ap = argparse.ArgumentParser(description="thread-safety source lints (F1.3)")
    ap.add_argument("source_root")
    ap.add_argument("--baseline", default=None,
                    help="baseline file (default: <source_root>/src/tools/"
                         "threadcheck/srclint-baseline.txt)")
    ap.add_argument("--update-baseline", action="store_true")
    args = ap.parse_args()

    if not os.path.isdir(args.source_root):
        sys.stderr.write("srclint: no such directory: %s\n" % args.source_root)
        return 2

    baseline_path = args.baseline or os.path.join(
        args.source_root, "src", "tools", "threadcheck", "srclint-baseline.txt")

    findings = scan(args.source_root)
    # Flatten to "<check> key" so all three lints share one baseline file.
    keys = set()
    where = {}
    for check, items in findings.items():
        for k, loc in items.items():
            flat = "%s %s" % (check, k)
            keys.add(flat)
            where[flat] = loc

    if args.update_baseline:
        write_baseline(baseline_path, keys, len(keys))
        sys.stderr.write("srclint: wrote baseline with %d entries to %s\n"
                         % (len(keys), baseline_path))
        return 0

    baseline = load_baseline(baseline_path)
    new = keys - baseline
    fixed = baseline - keys

    per = {c: len(v) for c, v in findings.items()}
    sys.stderr.write(
        "srclint: %d findings (s_globals %d, s_signals %d, s_libc %d); "
        "baseline %d; %d new, %d fixed\n"
        % (len(keys), per["s_globals"], per["s_signals"], per["s_libc"],
           len(baseline), len(new), len(fixed)))

    if new:
        sys.stderr.write("\nNew thread-unsafe source patterns "
                         "(fix or wrap, then justify):\n")
        for k in sorted(new):
            check, key = k.split(" ", 1)
            sys.stderr.write("  [%s] %s\n" % (check, where.get(k, key)))
        return 1

    if fixed:
        sys.stderr.write(
            "\n%d baseline entries are gone; shrink the baseline with "
            "'srclint --update-baseline'.\n" % len(fixed))

    return 0


if __name__ == "__main__":
    sys.exit(main())
