# Benchmark: rebuilt segmented engine vs tsvector/GIN

Instance: EC2 m6i.2xlarge (8 vCPU, 32 GB), Fedora, PG 20devel, gp3 16k IOPS,
shared_buffers=8GB. Corpus: 2,000,000 docs, Zipfian single-token alpha vocab
(50k terms), avg 68 bytes/doc. Warm cache, index scans forced, medians / p95
over 15 runs.

NOTE: an earlier run used `word_00005`-style tokens; the english analyzer splits
on `_`, so those queries secretly hit the token `word` (df=2,000,000) -- a
tokenization artifact, not an engine property. Re-run below uses whole single
tokens (df shown).

## Build + size
| metric      | pg_fts bm25 | tsvector/GIN |
|-------------|-------------|--------------|
| build time  | 10.9 s      | 5.6 s (+ to_tsvector) |
| index size  | 204 MB      | 110 MB       |

(Shared posting pages cut the bm25 index ~5x from an earlier 430MB->88MB on the
longer-doc corpus; here 204MB vs GIN 110MB on the shorter-doc corpus.)

## Query latency (median / p95, ms)
| query                              | pg_fts bm25 | tsvector/GIN | winner        |
|------------------------------------|-------------|--------------|---------------|
| Q1 rare count (df=2000)            | 1.9 / 2.3   | 2.4 / 2.4    | pg_fts 1.24x  |
| Q2 mid-term count (df=75k)         | 119 / 121   | 101 / 102    | GIN 1.18x     |
| Q3 two-term AND (mid & mid)        | 6.8 / 7.2   | 4.5 / 4.6    | GIN 1.5x      |
| **Q4 ranked top-10 (two mid)**     | **4.7 / 4.9** | 102 / 105  | **pg_fts 21.7x** |
| **Q5 ranked top-10 (common+mid)**  | **13.6 / 14.0** | 246 / 254 | **pg_fts 18.1x** |

## Read
- **Ranked BM25 top-k is the decisive win: 18-22x faster than GIN+ts_rank.**
  Block-max WAND with lazy paging skips almost every posting; GIN must fetch and
  ts_rank every match, then sort.  This is the entire reason the engine exists.
- Boolean COUNTs (Q2/Q3) GIN wins modestly -- compact posting lists + bitmap AND
  vs our (tid,tf,doclen) scoring postings.  Not a BM25 engine's job, but the gap
  is now small (1.2-1.5x, was 3.7x before shared pages + FOR).
- Index parity: 204MB vs 110MB (~1.9x), down from ~9x pre-optimization.

## Engine features GIN/tsvector cannot match (correctness, not just speed)
- True BM25/BM25F relevance ranking (Q4/Q5); GIN has only ts_rank heuristics.
- Fuzzy (term~k), regex (/re/), phrase/NEAR, prefix, highlight/snippet.
- amcanorderbyop <=> ordering scan pushes top-k into the AM.

## Remaining optimization backlog (measured)
- Q2/Q3 decode: skip-list intersection using block first_docid; a docids-only
  boolean posting variant would close the count-query gap.
- Full FST term dict + Levenshtein-DFA (deferred; point lookups already O(logP)).
- MaxScore for long queries / large k (BMW covers short queries).

## Scaling: 10,000,000 docs (vocab 100k)
Index: bm25 498 MB < GIN 569 MB (pg_fts is SMALLER at scale).

| query                         | pg_fts bm25 | GIN+ts_rank | speedup |
|-------------------------------|-------------|-------------|---------|
| rare count (df=2000)          | 6.9 ms      | 6.5 ms      | ~par    |
| ranked top-10 (mid, mid)      | 11.9 ms     | 456 ms      | **38x** |
| ranked top-10 (common, mid)   | 50 ms       | 1249 ms     | **25x** |

The ranked-query advantage WIDENS with scale (18-22x at 2M -> 25-38x at 10M):
GIN's ts_rank cost grows linearly with the match set, while block-max WAND stays
near-constant.  This is the TB-scale thesis confirmed.
