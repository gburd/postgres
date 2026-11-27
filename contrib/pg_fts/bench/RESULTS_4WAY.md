# pg_fts 4-way competitive benchmark — 2.19M Wikipedia

Real corpus: `wikimedia/wikipedia` 20231101.en, first ~2.19M articles (`body`
column), loaded identically into four databases.

## Environment
- EC2 r7i.4xlarge (16 vCPU Sapphire Rapids, 123 GB), Fedora 43.
- PostgreSQL **17.10** built from source (`--without-icu`, -O2).
- `shared_buffers=32GB`, `work_mem=256MB`, `jit=off`, autovacuum off during runs.
- Engines:
  - **pg_fts 1.19** (this repo, with the parallel-build regression fix) — `bm25` AM.
  - **ParadeDB pg_search 0.24.1** (Tantivy) — `USING bm25 (id,title,body) WITH (key_field='id')`.
  - **VectorChord-bm25** (HEAD, tsvector-based block-WeakAND) + pg_tokenizer.
  - **tsvector + GIN** + `ts_rank` (stock PostgreSQL FTS).
- Latency: median of 9, warm cache, uncontended (one engine at a time).

## Index size + build
| engine    | index size | notes |
|-----------|-----------:|-------|
| pg_fts    | 12 GB*     | *logical ~3.6 GB; physical bloated by unreclaimed post-merge pages (drops after REINDEX/VACUUM churn) |
| pg_search | 4103 MB    | |
| GIN       | 1550 MB    | tsvector + GIN |
| vchord    | 1367 MB    | tsvector + bm25 index |

Analyzer note: pg_fts and GIN use the same PostgreSQL `english` Snowball
config and return identical match counts (e.g. year 735,658; slovakia 10,889).
pg_search uses Tantivy `en_stem` (year 495,569 — different tokenizer, expected).
vchord uses the same `to_tsvector('english',...)` as GIN.

## Counts (ms)
| query        | pg_fts | pg_fts `fts_count` | pg_search | GIN   |
|--------------|-------:|-------------------:|----------:|------:|
| rare (10.9k) | 14.3   | **9.8**            | 32.1      | 14.1  |
| common (736k)| 371.8  | 305.2              | **123.4** | 496.7 |

## Ranked top-10 (ms)
| query      | pg_fts   | pg_search | GIN   | vchord   |
|------------|---------:|----------:|------:|---------:|
| rare&mid   | 22.8     | 27.5      | 32.2  | **7.4**  |
| common&mid | **19.3** | 27.5      | 127.2 | 13.8     |

## Ranked top-100 (common term) (ms)
| query          | pg_fts | pg_search | GIN     | vchord   |
|----------------|-------:|----------:|--------:|---------:|
| top-100 common | 74.4   | 27.7      | 3202.9  | **24.7** |

## Reading the results
- **vchord (current HEAD) wins ranked retrieval** — its tsvector-based
  block-WeakAND index is fast and small.  (This differs from the earlier run
  where vchord used a slow BERT tokenizer; the tsvector path is the fair, fast
  config.)
- **pg_fts wins ranked common&mid** (19.3 ms) and is competitive on rare&mid;
  it **crushes GIN+ts_rank** on ranked queries — up to **43x** on top-100
  common (74 ms vs 3203 ms), because ts_rank must sort every match.
- **pg_fts `fts_count` wins selective counts** (rare 9.8 ms).  On a common
  term the count is dominated by the current physical index bloat (12 GB);
  pg_search's columnar count wins there (123 ms).
- pg_fts and vchord are the only two that keep ranked common-term latency
  under ~75 ms; GIN is 1-2 orders of magnitude slower on ranked work.

## Where pg_fts should improve (unchanged conclusion)
The ranked-retrieval gap vs vchord/pg_search is a **posting-codec** matter
(compact columnar + rank/select skip), not an index-structure or sparsemap
matter — see NOTE_IMPACT_ORDERING.md and DEFERRED.md.  Physical index bloat
after merge (12 GB vs ~3.6 GB logical) is the other actionable item: recycle
merged source pages eagerly rather than leaving them for later reuse.
