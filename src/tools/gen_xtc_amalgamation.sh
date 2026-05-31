#!/bin/sh
#-------------------------------------------------------------------------
#
# gen_xtc_amalgamation.sh
#		Generate the single-file xtc amalgamation from contrib/libxtc.
#
# xtc is vendored as a SQLite-style single-file amalgamation, NOT as a
# separately built libxtc.a.  The upstream sources are carried as a git
# submodule pinned to a known-good commit at contrib/libxtc; this script
# regenerates xtc.h + xtc.c from that submodule into a build-local output
# directory.  The generated files are NOT committed.
#
# Usage:
#   src/tools/gen_xtc_amalgamation.sh [OUTDIR]
#
# OUTDIR defaults to ./xtc-amalg relative to the current directory.
#
# Build inputs for the generated xtc.c (see F1.6 in
# docs/threading/F1_CLASSIFICATION_HARNESS.md):
#   - compile once with -std=c11 -D_GNU_SOURCE -I<OUTDIR>/include
#   - link with -pthread -ldl -lm   (epoll backend auto-selected)
#   - debug: add -DDEBUG -DXTC_RELATIVE_LOC=contrib/libxtc so #line
#     directives remap diagnostics back to the original xtc sources.
#
# NOTE: the amalgamation's internal "#define _GNU_SOURCE" lines land
# after the first system-header include, so they are ineffective on
# glibc; consumers MUST pass -D_GNU_SOURCE on the command line (matches
# upstream examples/06_sqlxtc AMALG_CFLAGS).
#
# Copyright (c) 2026, PostgreSQL Global Development Group
#
# src/tools/gen_xtc_amalgamation.sh
#
#-------------------------------------------------------------------------

set -eu

# Resolve the repo root from this script's location (src/tools/..).
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
top_srcdir=$(CDPATH= cd -- "$script_dir/../.." && pwd)

submodule_dir="$top_srcdir/contrib/libxtc"
mkamalg="$submodule_dir/dist/mkamalgamation.py"
outdir="${1:-$PWD/xtc-amalg}"

if [ ! -e "$mkamalg" ]; then
	echo "error: $mkamalg not found." >&2
	echo "       Did you run 'git submodule update --init contrib/libxtc'?" >&2
	exit 1
fi

# Refuse to run against a submodule that drifted off its pinned commit.
pinned=$(git -C "$top_srcdir" ls-tree HEAD contrib/libxtc 2>/dev/null \
	| awk '{print $3}')
actual=$(git -C "$submodule_dir" rev-parse HEAD 2>/dev/null || echo "")
if [ -n "$pinned" ] && [ -n "$actual" ] && [ "$pinned" != "$actual" ]; then
	echo "warning: contrib/libxtc HEAD ($actual)" >&2
	echo "         differs from the pinned commit ($pinned)." >&2
fi

python3 "$mkamalg" --root "$submodule_dir" --out "$outdir"

echo "xtc amalgamation written to $outdir (xtc.h, xtc.c, include/)" >&2
