#!/usr/bin/env python3
# ----------------------------------------------------------------------
# apply_annotations.py - derive global-variable lifetime annotations from
# a reference commit and apply them to the current (evolved) tree.
#
# Heikki Linnakangas' "Annotate all global variables" (3b43bbca2e0) marks
# every mutable global with exactly one lifetime keyword (session_local,
# pg_global, dynamic_singleton, static_singleton, *_guc, ...).  We do NOT
# cherry-pick it verbatim: 80/386 files conflict against our latch->interrupt
# and F3 work, and the project strategy is "re-derive against current master
# using his diff as reference, not verbatim".
#
# This tool treats that commit as an ORACLE.  It parses its diff into a map of
#     exact_unannotated_declaration_line  ->  annotated_declaration_line
# (the annotated line is always the old line with a lifetime keyword + space
# prepended), then applies each decision to our tree ONLY when the exact
# un-annotated line still exists verbatim and is not already annotated.
# Anything that evolved away, was removed, or we already annotated is skipped.
# Idempotent and re-runnable.
#
# Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
#
# src/tools/threadcheck/apply_annotations.py
# ----------------------------------------------------------------------

import argparse
import os
import re
import subprocess
import sys

# The lifetime keywords defined in src/include/postgres_ext.h.  A "+" line in
# the reference diff counts as an annotation only when it equals a paired "-"
# line with one of these keywords (plus following whitespace) prepended.
KEYWORDS = (
    "session_local",
    "pg_global",
    "dynamic_singleton",
    "static_singleton",
    "internal_guc",
    "postmaster_guc",
    "session_guc",
    "sighup_guc",
    "suset_guc",
    "userset_guc",
)
_KW_ALT = "|".join(KEYWORDS)
# A declaration is "already annotated" if any lifetime keyword appears as a
# standalone token anywhere before the type/name.
_HAS_KW_RE = re.compile(r"\b(" + _KW_ALT + r")\b")
# The reference inserts the keyword at one of three positions: bare at the
# start, right after `static`, or right after `extern [PGDLLIMPORT[_TLS]]`.
# Matching by "remove the keyword token and compare to the old line" handles
# all of them generically.
_INSERT_RE = re.compile(
    r"^(?P<prefix>(?:static\s+)?(?:extern\s+)?(?:PGDLLIMPORT(?:_TLS)?\s+)?)"
    r"(?P<kw>" + _KW_ALT + r")\s+(?P<rest>.*)$")


def _annotation_of(new_line, old_line):
    """If new_line == old_line with a single lifetime keyword inserted after an
    optional static/extern/PGDLLIMPORT prefix, return that keyword; else None."""
    m = _INSERT_RE.match(new_line)
    if not m:
        return None
    rebuilt = m.group("prefix") + m.group("rest")
    return m.group("kw") if rebuilt == old_line else None


# Extract the declared variable name from a declaration body (the text after
# any prefix+keyword).  We take the last identifier that precedes the first of
# '[', '=', ';' or end-of-decl; this is the declarator name for the simple
# file-scope forms the reference annotates (e.g. "int *foo = x;" -> foo).
_DECL_NAME_RE = re.compile(
    r"^[^=;]*?\b(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*(?:\[[^\]]*\]\s*)*(?:=|;)")


def _declared_name(decl_body):
    m = _DECL_NAME_RE.match(decl_body)
    return m.group("name") if m else None


def build_var_keywords(decisions):
    """From parsed decisions build {varname: keyword}.  When the reference gives
    a variable more than one keyword (it never should), the first wins and the
    conflict is reported by the caller via the returned conflicts set."""
    var_kw = {}
    conflicts = set()
    for pairs in decisions.values():
        for _old, new in pairs:
            m = _INSERT_RE.match(new)
            if not m:
                continue
            kw = m.group("kw")
            name = _declared_name(m.group("rest"))
            if not name:
                continue
            if name in var_kw and var_kw[name] != kw:
                conflicts.add(name)
            else:
                var_kw.setdefault(name, kw)
    return var_kw, conflicts


# A file-scope declaration line: starts at column 0 (no leading whitespace, so
# function locals and struct fields are excluded), optional static/extern/
# PGDLLIMPORT prefix, a type, then the variable name as a declarator.  We build
# this per-variable so the name is anchored.
def _file_scope_decl_re(varname):
    return re.compile(
        r"^(?P<prefix>(?:static\s+)?(?:extern\s+)?(?:PGDLLIMPORT(?:_TLS)?\s+)?)"
        r"(?P<type>[A-Za-z_].*?[\s*])"
        r"(?P<name>" + re.escape(varname) + r")\s*(?:\[[^\]]*\]\s*)*(?:=|;)")


def apply_consistency(path, var_kw):
    """Ensure every file-scope declaration of a reference-annotated variable
    carries its keyword.  Catches sites the exact pass missed because our tree's
    text drifted (whitespace / initializer / type spelling).  Idempotent."""
    with open(path, "r", encoding="utf-8", errors="surrogateescape") as fh:
        lines = fh.read().split("\n")

    applied = 0
    # Pre-filter: only variables whose name textually occurs in the file.
    blob = "\n".join(lines)
    candidates = {v: kw for v, kw in var_kw.items() if v in blob}
    res = {v: _file_scope_decl_re(v) for v in candidates}

    for i, line in enumerate(lines):
        if not line or line[0].isspace():
            continue                      # not column-0: skip locals/fields
        if _HAS_KW_RE.search(line):
            continue                      # already annotated
        for v, kw in candidates.items():
            m = res[v].match(line)
            if m and m.group("name") == v:
                lines[i] = (m.group("prefix") + kw + " "
                            + line[m.start("type"):])
                applied += 1
                break

    if applied:
        with open(path, "w", encoding="utf-8", errors="surrogateescape") as fh:
            fh.write("\n".join(lines))
    return applied


def parse_reference(repo, ref):
    """Return {relpath: [(old_line, new_line), ...]} from the reference diff.

    We pair each removed line with the immediately following added line within
    a hunk and keep it only when new == '<keyword> ' + old.
    """
    out = subprocess.run(
        ["git", "-C", repo, "show", "--no-color", "--format=", ref],
        capture_output=True, text=True, check=True).stdout

    decisions = {}
    cur = None
    pending_old = []          # removed lines awaiting their "+" partners
    for raw in out.splitlines():
        if raw.startswith("diff --git "):
            cur = None
            pending_old = []
            continue
        if raw.startswith("+++ b/"):
            cur = raw[6:]
            decisions.setdefault(cur, [])
            pending_old = []
            continue
        if cur is None:
            continue
        if raw.startswith("@@"):
            pending_old = []
            continue
        if raw.startswith("-") and not raw.startswith("---"):
            pending_old.append(raw[1:])
            continue
        if raw.startswith("+") and not raw.startswith("+++"):
            new = raw[1:]
            if pending_old:
                # The new line must equal a pending old line with exactly one
                # lifetime keyword inserted (bare / after static / after
                # extern[ PGDLLIMPORT]).  That old line is the un-annotated decl.
                for i, old in enumerate(pending_old):
                    if _annotation_of(new, old) is not None:
                        decisions[cur].append((old, new))
                        del pending_old[i]
                        break
            continue
        # context line ends the current removal run
        pending_old = []
    return {f: d for f, d in decisions.items() if d}


def apply_to_file(path, pairs):
    """Apply (old->new) line replacements to path. Returns (applied, skipped)."""
    with open(path, "r", encoding="utf-8", errors="surrogateescape") as fh:
        lines = fh.read().split("\n")

    # index un-annotated lines for exact match; skip lines already annotated.
    applied = skipped = 0
    by_old = {}
    for old, new in pairs:
        by_old.setdefault(old, new)

    for i, line in enumerate(lines):
        new = by_old.get(line)
        if new is None:
            continue
        # already annotated? (defensive: our own line equals the target)
        if line == new:
            continue
        # don't double-annotate: if the current line already starts with a
        # keyword, leave it (our latch work owns those).
        if _HAS_KW_RE.search(line):
            skipped += 1
            continue
        lines[i] = new
        applied += 1

    if applied:
        with open(path, "w", encoding="utf-8", errors="surrogateescape") as fh:
            fh.write("\n".join(lines))
    return applied, skipped


def main():
    ap = argparse.ArgumentParser(description="derive/apply lifetime annotations")
    ap.add_argument("repo", help="repository root")
    ap.add_argument("--ref", default="3b43bbca2e0",
                    help="reference commit (default: Heikki's annotate-all)")
    ap.add_argument("--dry-run", action="store_true",
                    help="report counts, do not write")
    ap.add_argument("--only", action="append", default=[],
                    help="restrict to relpath substring (repeatable)")
    ap.add_argument("--no-consistency", action="store_true",
                    help="skip the variable-keyed def/extern consistency pass")
    args = ap.parse_args()

    decisions = parse_reference(args.repo, args.ref)
    var_kw, conflicts = build_var_keywords(decisions)
    if conflicts:
        sys.stderr.write("apply_annotations: %d vars have conflicting keywords "
                         "in the reference (left to exact pass): %s\n"
                         % (len(conflicts), ", ".join(sorted(conflicts)[:10])))
        for c in conflicts:
            var_kw.pop(c, None)

    total_files = total_applied = total_skipped = missing_files = 0
    no_match_files = 0
    consistency_applied = 0
    for relpath, pairs in sorted(decisions.items()):
        if args.only and not any(s in relpath for s in args.only):
            continue
        abspath = os.path.join(args.repo, relpath)
        if not os.path.exists(abspath):
            missing_files += 1
            continue
        if args.dry_run:
            # count how many old-lines exist verbatim & unannotated
            with open(abspath, encoding="utf-8", errors="surrogateescape") as fh:
                content = set(fh.read().split("\n"))
            a = sum(1 for old, new in pairs
                    if old in content and not _HAS_KW_RE.search(old))
            if a:
                total_files += 1
                total_applied += a
                print(f"  {relpath}: {a}/{len(pairs)} applicable")
            else:
                no_match_files += 1
            continue
        applied, skipped = apply_to_file(abspath, pairs)
        if applied:
            total_files += 1
            total_applied += applied
        else:
            no_match_files += 1
        total_skipped += skipped
        if not args.no_consistency:
            consistency_applied += apply_consistency(abspath, var_kw)

    verb = "applicable" if args.dry_run else "applied"
    sys.stderr.write(
        f"apply_annotations: {total_applied} annotations {verb} across "
        f"{total_files} files; {total_skipped} already-annotated skipped; "
        f"{missing_files} ref files absent here; {no_match_files} files with "
        f"no applicable lines\n")
    if not args.dry_run and not args.no_consistency:
        sys.stderr.write(
            f"apply_annotations: consistency pass added {consistency_applied} "
            f"keyword(s) to drifted/lagging declarations\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
