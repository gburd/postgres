#!/usr/bin/env python3
"""Summarize Bitmapset census dumps (one TSV per backend) by call site.

Usage: census_sum.py <workload-dir> [<workload-dir> ...]

Set PGBIN to the postgres binary: backends rewrite their process title, so
dladdr reports the title rather than the path for the main executable.

Each dump row is: object, offset, fn, calls, members, words, max_words,
then (allocated, freed) byte pairs for the three representations bms, off,
and sbm (see bms_census.c).  Rows are merged across backends and their call
sites symbolized with addr2line.  Per workload this prints the bytes each
representation allocates, the producing call sites ranked by Bitmapset
bytes, the sites where sbm beats even an offset Bitmapset by 2x, and the
read-only functions ranked by words examined.
"""
import collections
import glob
import os
import subprocess
import sys

PRODUCERS = {
    "copy", "make_singleton", "union", "intersect", "difference",
    "offset_members", "add_member", "del_member", "add_members",
    "replace_members", "add_range", "int_members", "del_members", "join",
}
REPS = ("bms", "off", "sbm")
# Row layout after merging: calls, members, words, max_words, then
# alloc/freed for bms, off, sbm.
CALLS, MEMBERS, WORDS, MAXW = 0, 1, 2, 3
ALLOC = {"bms": 4, "off": 6, "sbm": 8}


def load(path):
    rows = collections.defaultdict(lambda: [0] * 10)
    for f in glob.glob(os.path.join(path, "*.tsv")):
        for line in open(f):
            obj, off, fn, *nums = line.rstrip("\n").split("\t")
            nums = [int(x) for x in nums]
            r = rows[(obj, off, fn)]
            for i, v in enumerate(nums):
                r[i] = max(r[i], v) if i == MAXW else r[i] + v
    return rows


def is_pie(path):
    out = subprocess.run(["readelf", "-hW", path], capture_output=True,
                         text=True).stdout
    return "DYN" in out


def symbolize(keys):
    pgbin = os.environ.get("PGBIN", "")
    by_obj = collections.defaultdict(set)
    for obj, off, _ in keys:
        by_obj[obj].add(off)
    names = {}
    for obj, offs in by_obj.items():
        offs = sorted(offs)
        path = obj if os.path.exists(obj) else pgbin
        if not path or not os.path.exists(path):
            for o in offs:
                names[(obj, o)] = (obj + "+" + o, "")
            continue
        # A non-PIE executable reports a zero base, so its "offset" is the
        # address minus nothing; rebase it.  The return address points just
        # past the call, so step back one byte to land on the call's line.
        base = 0x400000 if path == pgbin and not is_pie(path) else 0
        args = ["addr2line", "-f", "-C", "-e", path] + [
            hex(int(o, 16) + base - 1) for o in offs]
        lines = subprocess.run(args, capture_output=True,
                               text=True).stdout.splitlines()
        for i, o in enumerate(offs):
            func = lines[2 * i] if 2 * i < len(lines) else "?"
            loc = lines[2 * i + 1] if 2 * i + 1 < len(lines) else "?"
            loc = loc.split("/src/")[-1].split(" (")[0]
            names[(obj, o)] = (func, loc)
    return names


def site_line(v, fn, name):
    bb = v[ALLOC["bms"]]
    calls = max(v[CALLS], 1)
    return (f"{bb:>12,} {v[ALLOC['off']] / max(bb, 1):>7.2f} "
            f"{v[ALLOC['sbm']] / max(bb, 1):>7.2f} {v[CALLS]:>9,} "
            f"{v[MEMBERS] / calls:>8.1f} {v[WORDS] / calls:>6.1f} "
            f"{v[MAXW]:>6}  {fn}  {name[0]} {name[1]}")


def report(path):
    rows = load(path)
    names = symbolize(rows.keys())
    prod = {k: v for k, v in rows.items() if k[2] in PRODUCERS}
    reads = {k: v for k, v in rows.items()
             if k[2] not in PRODUCERS and k[2] != "free"}
    tot = {r: sum(v[ALLOC[r]] for v in prod.values()) for r in REPS}
    tb = max(tot["bms"], 1)
    print(f"=== {os.path.basename(path.rstrip('/'))}")
    print(f"set-producing calls {sum(v[CALLS] for v in prod.values()):,}; "
          f"bytes allocated: bms {tot['bms']:,}  off {tot['off']:,} "
          f"({tot['off'] / tb:.2f}x)  sbm {tot['sbm']:,} "
          f"({tot['sbm'] / tb:.2f}x)")
    hdr = (f"{'bms alloc':>12} {'off/bms':>7} {'sbm/bms':>7} {'calls':>9} "
           f"{'avg memb':>8} {'avg w':>6} {'max w':>6}  fn  site")
    print(hdr)
    ranked = sorted(prod.items(), key=lambda kv: -kv[1][ALLOC["bms"]])
    for (obj, off, fn), v in ranked[:20]:
        print(site_line(v, fn, names[(obj, off)]))
    special = [kv for kv in ranked
               if kv[1][ALLOC["sbm"]] * 2 <= kv[1][ALLOC["off"]]
               and kv[1][ALLOC["bms"]] >= 4096]
    print(f"-- sites where sbm allocates <= half of off ({len(special)}):")
    for (obj, off, fn), v in special[:15]:
        print(site_line(v, fn, names[(obj, off)]))
    print("-- read sites by words examined:")
    for (obj, off, fn), v in sorted(reads.items(),
                                    key=lambda kv: -kv[1][WORDS])[:12]:
        func, loc = names[(obj, off)]
        print(f"{v[WORDS]:>13,} {v[CALLS]:>10,} "
              f"{v[WORDS] / max(v[CALLS], 1):>6.1f}  {fn}  {func} {loc}")
    print()


for p in sys.argv[1:]:
    report(p)
