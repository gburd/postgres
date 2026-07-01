# Benchmark: segmented engine (rebuild steps 1-7) vs tsvector/GIN

Instance: EC2 m6i.2xlarge (8 vCPU, 32 GB), Fedora, PG 20devel, gp3 16k IOPS.
shared_buffers=8GB. Corpus: 2,000,000 docs, Zipfian vocab=50k, avgdl=23 words,
322 MB heap. Warm cache. Medians over 15 runs, index scans forced.

## Index build + size
| metric            | pg_fts bm25 | tsvector/GIN |
|-------------------|-------------|--------------|
| build time        | 9.6 s       | 6.0 s (+ 51 s to_tsvector) |
| index size        | 430 MB      | 115 MB       |
| segments          | 1           | -            |

## Query latency (median / p95, ms)
| query                          | pg_fts bm25   | tsvector/GIN  | winner       |
|--------------------------------|---------------|---------------|--------------|
| Q1 rare term count (df=2000)   | 1.57 / 1.87   | 1.97 / 2.08   | pg_fts 1.25x |
| Q2 common term count           | 436 / 439     | 298 / 302     | GIN 1.46x    |
| Q3 two-term AND count          | 79 / 80       | 22 / 22       | GIN 3.7x     |
| Q4 top-10 ranked (BM25 vs rank)| 35 / 35       | 136 / 143     | pg_fts 3.9x  |

## Read
- **Ranked top-k (Q4) is pg_fts's flagship win: 3.9x** -- block-max WAND skips
  most postings; GIN must fetch every match and sort by ts_rank.
- **Boolean counts (Q2/Q3) GIN wins** -- pg_fts decodes full (tid,tf,doclen)
  postings + TID-set intersection; GIN has compact lists + bitmap AND. The tf/
  doclen payload we carry for scoring is dead weight for a pure boolean count.
- **Index 3.7x larger** for the same reason (scoring payload per posting).

## Next optimization targets (measured, not guessed)
1. FOR/PFOR bit-pack the intra-block posting payload -> shrink index toward GIN
   and cut Q2/Q3 decode cost (the deferred step-3 intra-block encoding).
2. Skip-list intersection for AND: use block first_docid to skip during TID-set
   AND instead of decoding both lists fully (Q3).
3. Consider a boolean-only posting variant (docids only, no tf/doclen) or column
   to serve non-ranked matches from a compact list.
