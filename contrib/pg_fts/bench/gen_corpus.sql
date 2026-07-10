-- contrib/pg_fts/bench/gen_corpus.sql
-- Generate a reproducible corpus with a Zipfian-ish vocabulary so IDF/BM25 are
-- meaningful (rare terms carry signal, common terms don't).  :ndocs and :vocab
-- are psql vars.  Produces docs(id, body) with ~12-word bodies drawn from a
-- vocabulary where word K has frequency ~ 1/K.
\set ndocs :ndocs
\set vocab :vocab
DROP TABLE IF EXISTS docs;
CREATE TABLE docs (id bigint, body text);

-- vocabulary: word_00001 .. word_NNNNN ; sampling weight 1/rank via a
-- transformed uniform (floor(vocab * u^3)+1) gives a heavy-tailed pick.
INSERT INTO docs
SELECT g,
       (SELECT string_agg('word_' || lpad(
                 (floor(:vocab * power(random(),3))::int + 1)::text, 5, '0'), ' ')
        FROM generate_series(1, 8 + (g % 8)))    -- 8..15 words per doc
FROM generate_series(1, :ndocs) g;

-- salt a known rare marker into 0.1% of docs so we have a controllable
-- high-IDF query with a known ground-truth match set (for NDCG).
UPDATE docs SET body = body || ' zzqrare'
WHERE id % 1000 = 0;

SELECT count(*) AS ndocs, avg(length(body))::int AS avg_bytes FROM docs;
