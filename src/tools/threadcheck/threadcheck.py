#!/usr/bin/env python3
#
# threadcheck - run the GUC/global lifetime-annotation checkers over the
# backend compilation database and enforce a ratcheting baseline.
#
# This is the F1.2 driver of the thread-classification harness (see
# docs/threading/F1_CLASSIFICATION_HARNESS.md).  It runs:
#
#   * pgguclifetimes  - file-scope globals must carry a lifetime annotation
#   * pg_static_vars  - function-scope statics (optional; built only when a
#                       C++ compiler is available)
#
# over the meson compile_commands.json, normalises the offenders to
# location-independent "path:symbol" keys (so the same global reported across
# the srv/shlib/static build variants collapses to one entry, and the key
# survives line-number churn), filters out vendored third-party trees that are
# not PostgreSQL core, and compares the result against a frozen baseline.
#
# Exit status:
#   0  - no new offenders (the set is <= the baseline)
#   1  - new offenders appeared (CI failure); they are listed with file:line
#   2  - setup problem (tools or compdb not found)
#
# With --update-baseline, the current offender set is written to the baseline
# file instead of being checked (the baseline only ever shrinks in normal use;
# this regenerates it deliberately).
#
# Copyright (c) 2024, PostgreSQL Global Development Group
#
# src/tools/threadcheck/threadcheck.py

import argparse
import os
import re
import subprocess
import sys

# Vendored / third-party subtrees that carry their own conventions and are not
# part of the PostgreSQL-core globals discipline.  Paths are matched as
# substrings of the repo-relative path.
DEFAULT_EXCLUDES = [
    "contrib/libxtc/",
    "src/backend/snowball/libstemmer",
    "src/include/snowball/libstemmer",
    "src/backend/regex/",          # Henry Spencer regex import
    "src/backend/utils/mb/Unicode/",
    "src/port/snprintf.c",
    "src/timezone/",               # IANA tz import
]

OFFENDER_RE = re.compile(
    r"^(?P<path>.+?):(?P<line>\d+):(?P<col>\d+): "
    r"(?P<sym>.+?) is missing a lifetime annotation\s*$"
)


def find_tool(build_root, name):
    cand = os.path.join(build_root, "src", "tools", "pgguclifetimes", name)
    return cand if os.path.exists(cand) else None


def run_checker(tool, build_root):
    """Run a checker over the compdb in build_root; return its stdout+stderr."""
    proc = subprocess.run(
        [tool, build_root],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
    )
    return proc.stdout or ""


def normalise(output, source_root, build_root, excludes):
    """Parse checker output into a set of 'relpath:symbol' keys plus a map of
    key -> first 'relpath:line' for human-readable reporting."""
    keys = set()
    where = {}
    src_prefix = os.path.realpath(source_root) + os.sep
    build_prefix = os.path.realpath(build_root) + os.sep
    for raw in output.splitlines():
        m = OFFENDER_RE.match(raw)
        if not m:
            continue
        path = m.group("path")
        rp = os.path.realpath(path)
        # Skip generated sources living under the build tree (fmgr tables,
        # parser output, etc.); we only classify checked-in source.
        if rp.startswith(build_prefix):
            continue
        if rp.startswith(src_prefix):
            rel = rp[len(src_prefix):]
        else:
            rel = os.path.relpath(rp, source_root)
        if any(ex in rel for ex in excludes):
            continue
        key = "%s:%s" % (rel, m.group("sym"))
        keys.add(key)
        where.setdefault(key, "%s:%s" % (rel, m.group("line")))
    return keys, where


def load_baseline(path):
    if not os.path.exists(path):
        return set()
    out = set()
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                out.add(line)
    return out


def write_baseline(path, keys, header):
    with open(path, "w") as f:
        f.write(header)
        for k in sorted(keys):
            f.write(k + "\n")


def main():
    ap = argparse.ArgumentParser(description="thread-classification harness")
    ap.add_argument("source_root")
    ap.add_argument("build_root")
    ap.add_argument("--baseline", default=None,
                    help="baseline file (default: <source_root>/src/tools/"
                         "threadcheck/baseline.txt)")
    ap.add_argument("--update-baseline", action="store_true",
                    help="rewrite the baseline from the current offenders")
    ap.add_argument("--exclude", action="append", default=[],
                    help="additional path substring to exclude")
    args = ap.parse_args()

    excludes = DEFAULT_EXCLUDES + args.exclude
    baseline_path = args.baseline or os.path.join(
        args.source_root, "src", "tools", "threadcheck", "baseline.txt")

    pgg = find_tool(args.build_root, "pgguclifetimes")
    if pgg is None:
        sys.stderr.write(
            "threadcheck: pgguclifetimes not built in %s\n"
            "  configure with -Dllvm=enabled (and, if the clang-c headers are "
            "in a split\n  output, -Dclang_c_includedir=DIR), then build the "
            "'pgguclifetimes' target.\n" % args.build_root)
        return 2

    compdb = os.path.join(args.build_root, "compile_commands.json")
    if not os.path.exists(compdb):
        sys.stderr.write("threadcheck: no compile_commands.json in %s\n"
                         % args.build_root)
        return 2

    output = run_checker(pgg, args.build_root)

    psv = find_tool(args.build_root, "pg_static_vars")
    if psv is not None:
        output += "\n" + run_checker(psv, args.build_root)

    keys, where = normalise(output, args.source_root, args.build_root, excludes)

    if args.update_baseline:
        header = (
            "# threadcheck baseline: PostgreSQL-core globals/statics still "
            "missing a\n# lifetime annotation. Keyed as 'relpath:symbol'. This "
            "list may only\n# shrink: CI fails if a new offender appears. "
            "Regenerate deliberately with\n# 'threadcheck --update-baseline'.\n"
            "# Total: %d\n" % len(keys))
        write_baseline(baseline_path, keys, header)
        sys.stderr.write("threadcheck: wrote baseline with %d entries to %s\n"
                         % (len(keys), baseline_path))
        return 0

    baseline = load_baseline(baseline_path)
    new = keys - baseline
    fixed = baseline - keys

    sys.stderr.write(
        "threadcheck: %d core offenders (baseline %d); %d new, %d fixed\n"
        % (len(keys), len(baseline), len(new), len(fixed)))

    if new:
        sys.stderr.write("\nNew unclassified globals/statics "
                         "(annotate or reclassify):\n")
        for k in sorted(new):
            sym = k.split(":", 1)[1]
            sys.stderr.write("  %s  (%s)\n" % (where.get(k, k), sym))
        return 1

    if fixed:
        sys.stderr.write(
            "\n%d baseline entries are now annotated; shrink the baseline with "
            "'threadcheck --update-baseline'.\n" % len(fixed))

    return 0


if __name__ == "__main__":
    sys.exit(main())
