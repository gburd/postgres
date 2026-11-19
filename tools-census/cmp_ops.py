#!/usr/bin/env python3
"""Compare two ops_bench.sh outputs (base, new): per (shape, op) ns ratios.

Usage: cmp_ops.py <base.csv> <new.csv> [<base2.csv> <new2.csv> ...]

Pairs are (base, new).  With several pairs, the per-pair speedups are listed
so the spread across runs is visible; the summary column is their median.
"""
import statistics
import sys


def sections(path):
    out = {}
    cur = None
    for line in open(path):
        line = line.rstrip("\n")
        if line.startswith("# "):
            cur = line[2:]
            out[cur] = []
            continue
        if not line or line.startswith("(") or cur is None:
            continue
        out[cur].append(line)
    return out


def ops(path):
    rows = {}
    s = sections(path)["ops"]
    hdr = s[0].split(",")
    for line in s[1:]:
        f = dict(zip(hdr, line.split(",")))
        rows[(f["label"], f["op"])] = (float(f["ns"]), int(f["chunks"]))
    return rows


def shapes(path):
    rows = {}
    s = sections(path)["shapes"]
    for line in s[1:]:
        p = line.split(",")
        tail = p[-5:]
        rows[(p[0], ",".join(p[1:-5]))] = float(tail[1])
    return rows


def intset(path):
    rows = {}
    s = sections(path)["intset"]
    hdr = s[0].split(",")
    for line in s[1:]:
        f = dict(zip(hdr, line.split(",")))
        if f["structure"] == "intset":
            continue
        rows[(f["label"], f["structure"])] = (
            float(f["add_ns"]), float(f["iterate_ns"]), float(f["probe_ns"]))
    return rows


def main(args):
    pairs = [(args[i], args[i + 1]) for i in range(0, len(args), 2)]
    o = [(ops(b), ops(n)) for b, n in pairs]
    print(f"{'shape':22} {'op':14} {'chunks':>6} {'base ns':>12} "
          f"{'new ns':>11} {'speedup':>8}  per-run")
    for key in o[0][0]:
        sp = [b[key][0] / n[key][0] for b, n in o if n[key][0] > 0]
        b0, n0 = o[0][0][key][0], o[0][1][key][0]
        print(f"{key[0]:22} {key[1]:14} {o[0][0][key][1]:>6} {b0:>12.1f} "
              f"{n0:>11.1f} {statistics.median(sp):>7.2f}x  "
              + " ".join(f"{x:.2f}" for x in sp))
    sh = [(shapes(b), shapes(n)) for b, n in pairs]
    print(f"\n{'shape':10} {'op':16} {'base sbm ns':>11} {'new sbm ns':>10} "
          f"{'speedup':>8}")
    for key in sh[0][0]:
        sp = [b[key] / n[key] for b, n in sh if n[key] > 0]
        print(f"{key[0]:10} {key[1]:16} {sh[0][0][key]:>11.1f} "
              f"{sh[0][1][key]:>10.1f} {statistics.median(sp):>7.2f}x")
    it = [(intset(b), intset(n)) for b, n in pairs]
    print(f"\n{'intset shape':20} {'structure':12} {'add':>14} {'iter':>14} "
          f"{'probe':>16}")
    for key in it[0][0]:
        b, n = it[0][0][key], it[0][1][key]
        print(f"{key[0]:20} {key[1]:12} "
              + " ".join(f"{b[i]:>7.1f}->{n[i]:<7.1f}" for i in range(3)))


main(sys.argv[1:])
