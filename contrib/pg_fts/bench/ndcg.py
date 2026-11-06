#!/usr/bin/env python3
"""contrib/pg_fts/bench/ndcg.py

Relevance scoring for the BM25 benchmark: run each BEIR/MS-MARCO query through a
system and score NDCG@10 / Recall@100 / MRR against qrels.

Expects three inputs:
  --queries queries.tsv   (query_id<TAB>text)
  --qrels   qrels.tsv     (query_id<TAB>doc_id<TAB>relevance)
  --system  {pgfts|tsvector|pgsearch}

and a running PostgreSQL with docs(id,title,body) + the indexes from load.sh.

Prints NDCG@10, Recall@100, MRR (means over queries).  This is the accuracy
axis the competition reports (BEIR paper, bm25s); it lets us show pg_fts ranks
as well as Lucene/Elasticsearch and clearly better than tsvector ts_rank.
"""
import argparse
import math
import sys
from collections import defaultdict

import psycopg2  # pip install psycopg2-binary


def load_tsv(path, n):
    rows = []
    with open(path) as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) >= n:
                rows.append(parts)
    return rows


def query_sql(system, text):
    t = text.replace("'", "''")
    if system == "pgfts":
        # OR the terms; rank by BM25 distance (ascending = most relevant)
        q = " | ".join(t.split())
        return (f"SELECT id FROM docs WHERE d @@@ '{q}'::ftsquery "
                f"ORDER BY d <=> '{q}'::ftsquery LIMIT 100")
    if system == "tsvector":
        q = " | ".join(t.split())
        return (f"SELECT id FROM docs WHERE tsv @@ to_tsquery('english','{q}') "
                f"ORDER BY ts_rank(tsv, to_tsquery('english','{q}')) DESC LIMIT 100")
    if system == "pgsearch":
        return (f"SELECT id FROM docs WHERE body @@@ '{t}' "
                f"ORDER BY paradedb.score(id) DESC LIMIT 100")
    raise SystemExit(f"unknown system {system}")


def dcg(rels):
    return sum(r / math.log2(i + 2) for i, r in enumerate(rels))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--queries", required=True)
    ap.add_argument("--qrels", required=True)
    ap.add_argument("--system", required=True)
    ap.add_argument("--dsn", default="dbname=bench")
    args = ap.parse_args()

    queries = {qid: text for qid, text in load_tsv(args.queries, 2)}
    rel = defaultdict(dict)
    for qid, did, r in load_tsv(args.qrels, 3):
        rel[qid][did] = float(r)

    conn = psycopg2.connect(args.dsn)
    cur = conn.cursor()

    ndcgs, recalls, mrrs = [], [], []
    for qid, text in queries.items():
        if qid not in rel:
            continue
        cur.execute(query_sql(args.system, text))
        ranked = [str(row[0]) for row in cur.fetchall()]
        gains = [rel[qid].get(d, 0.0) for d in ranked]

        ideal = sorted(rel[qid].values(), reverse=True)
        idcg = dcg(ideal[:10]) or 1.0
        ndcgs.append(dcg(gains[:10]) / idcg)

        nrel = sum(1 for v in rel[qid].values() if v > 0)
        hit = sum(1 for g in gains[:100] if g > 0)
        recalls.append(hit / nrel if nrel else 0.0)

        mrr = 0.0
        for i, g in enumerate(gains):
            if g > 0:
                mrr = 1.0 / (i + 1)
                break
        mrrs.append(mrr)

    n = len(ndcgs)
    print(f"system={args.system} queries={n}")
    print(f"NDCG@10   = {sum(ndcgs)/n:.4f}")
    print(f"Recall@100= {sum(recalls)/n:.4f}")
    print(f"MRR       = {sum(mrrs)/n:.4f}")


if __name__ == "__main__":
    main()
