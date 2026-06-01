#!/usr/bin/env python3
#
# run-cocci - apply or check the project's Coccinelle semantic patches.
#
# Coccinelle (https://coccinelle.gitlabpages.inria.fr/website/) lets us express
# *mechanical* C-to-C transforms as re-runnable ".cocci" semantic patches
# (SmPL).  The threading conversion contains many high-volume, mechanical
# sweeps -- call-site rewrites, signature changes, annotation seeding -- where a
# checked-in semantic patch is far better than a one-shot hand edit: it is
# self-documenting, re-runnable as the tree drifts, and eases tracking against
# origin/master (re-run the patch on a fresh merge instead of re-resolving
# conflicts by hand).
#
# Project convention (docs/threading): ship MECHANICAL transforms as .cocci
# patches in this directory; keep STRUCTURAL / creative edits hand-derived.
# Do NOT retrofit .cocci for transforms that have already landed.
#
# Usage:
#   run-cocci.py --list                 list available semantic patches
#   run-cocci.py [--apply] [PATCH...]   dry-run (default) or apply patches
#
# Without PATCH arguments, every *.cocci in this directory is processed.
# Default mode is a dry run: spatch computes the diff but does not touch the
# tree (exit 1 if any patch would change something -- useful as a CI "is the
# tree still in normal form?" check).  --apply writes the changes in place.
#
# Copyright (c) 2024, PostgreSQL Global Development Group
#
# src/tools/cocci/run-cocci.py

import argparse
import glob
import os
import shutil
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))


def find_spatch():
    return shutil.which("spatch")


def patch_target_dirs(source_root):
    # Limit spatch to the C sources we actually transform; skip vendored and
    # generated trees for speed and to avoid touching third-party code.
    return [os.path.join(source_root, "src", "backend"),
            os.path.join(source_root, "src", "include"),
            os.path.join(source_root, "src", "common"),
            os.path.join(source_root, "src", "bin")]


def run_one(spatch, cocci, source_root, apply):
    dirs = [d for d in patch_target_dirs(source_root) if os.path.isdir(d)]
    out_all = ""
    err_all = ""
    rc = 0
    # spatch's --dir takes a single directory, so iterate.
    for d in dirs:
        cmd = [spatch, "--sp-file", cocci, "--include-headers", "--dir", d]
        if apply:
            cmd[1:1] = ["--very-quiet", "--in-place"]
        else:
            # dry run: emit a unified diff without modifying any file
            cmd[1:1] = ["--quiet", "--show-diff"]
        proc = subprocess.run(cmd, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, universal_newlines=True)
        out_all += proc.stdout
        err_all += proc.stderr
        if proc.returncode != 0:
            rc = proc.returncode
    return rc, out_all, err_all


def main():
    ap = argparse.ArgumentParser(description="run project Coccinelle patches")
    ap.add_argument("patches", nargs="*",
                    help="specific .cocci files (default: all in this dir)")
    ap.add_argument("--source-root", default=os.path.abspath(
        os.path.join(HERE, "..", "..", "..")),
        help="repository root (default: inferred)")
    ap.add_argument("--apply", action="store_true",
                    help="modify files in place (default: dry-run diff)")
    ap.add_argument("--list", action="store_true",
                    help="list available semantic patches and exit")
    args = ap.parse_args()

    available = sorted(glob.glob(os.path.join(HERE, "*.cocci")))

    if args.list:
        if not available:
            print("(no .cocci patches in %s)" % HERE)
        for p in available:
            print(os.path.relpath(p, args.source_root))
        return 0

    spatch = find_spatch()
    if spatch is None:
        sys.stderr.write(
            "run-cocci: spatch (Coccinelle) not found on PATH.\n"
            "  Install Coccinelle to run the project's semantic patches.\n")
        return 2

    patches = [os.path.abspath(p) for p in args.patches] or available
    if not patches:
        sys.stderr.write("run-cocci: no .cocci patches found in %s\n" % HERE)
        return 2

    changed = False        # a normal-form patch would change the tree (gate)
    poc_changed = False    # an illustrative *_poc patch matched (informational)
    rc = 0
    for cocci in patches:
        name = os.path.relpath(cocci, args.source_root)
        # *_poc.cocci are illustrative (e.g. depend on an API not yet landed):
        # report their matches but never fail the gate on them.
        is_poc = os.path.basename(cocci).endswith("_poc.cocci")
        code, out, err = run_one(spatch, cocci, args.source_root, args.apply)
        if code != 0:
            sys.stderr.write("run-cocci: %s: spatch exited %d\n%s\n"
                             % (name, code, err))
            rc = 2
            continue
        if out.strip():
            if args.apply:
                sys.stderr.write("run-cocci: applied %s\n" % name)
                changed = True
            elif is_poc:
                poc_changed = True
                sys.stderr.write("run-cocci: %s (illustrative) matches:\n"
                                 % name)
                sys.stdout.write(out)
            else:
                changed = True
                sys.stderr.write("run-cocci: %s would change the tree:\n"
                                 % name)
                sys.stdout.write(out)
        else:
            sys.stderr.write("run-cocci: %s: no changes\n" % name)

    if rc:
        return rc
    if changed and not args.apply:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
