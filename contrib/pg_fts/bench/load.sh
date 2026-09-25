#!/bin/bash
# contrib/pg_fts/bench/load.sh
# Load a corpus into docs(id,title,body) and build the three indexes, recording
# build time, on-disk size, and (best-effort) peak RSS for each.
#
# Usage: bash load.sh /path/to/corpus.tsv   (id<TAB>title<TAB>body per line)
set -euo pipefail

PGBIN="${PGBIN:-/postgres/pgfts/bin}"
DB=bench
CORPUS="${1:?usage: load.sh corpus.tsv}"
RESULTS="${RESULTS:-/pgfts_bench/results}"
mkdir -p "$RESULTS"
psql() { "$PGBIN/psql" -X -q -d "$DB" "$@"; }

"$PGBIN/createdb" "$DB" 2>/dev/null || true
psql -c "CREATE EXTENSION IF NOT EXISTS pg_fts;"
psql -c "DROP TABLE IF EXISTS docs;"
psql -c "CREATE TABLE docs (id bigint, title text, body text);"

echo "[load] copying corpus..."
psql -c "\\copy docs (id, title, body) FROM '$CORPUS' WITH (FORMAT csv, DELIMITER E'\\t')"
psql -c "SELECT count(*) AS ndocs FROM docs;"

timed_build() {   # name, sql
    local name="$1" sql="$2" t0 t1
    t0=$(date +%s.%N)
    psql -c "$sql"
    t1=$(date +%s.%N)
    echo "$name build: $(echo "$t1 - $t0" | bc) s"
}

echo "build,seconds,size" > "$RESULTS/indexes.csv"

# --- pg_fts bm25 ---
t0=$(date +%s.%N)
psql -c "CREATE INDEX docs_bm25 ON docs USING bm25 (to_ftsdoc('english', body));"
t1=$(date +%s.%N)
sz=$(psql -tAc "SELECT pg_size_pretty(pg_relation_size('docs_bm25'));")
echo "pg_fts,$(echo "$t1-$t0"|bc),$sz" >> "$RESULTS/indexes.csv"

# --- tsvector + GIN ---
psql -c "ALTER TABLE docs ADD COLUMN tsv tsvector;"
t0=$(date +%s.%N)
psql -c "UPDATE docs SET tsv = to_tsvector('english', body);"
psql -c "CREATE INDEX docs_gin ON docs USING gin (tsv);"
t1=$(date +%s.%N)
sz=$(psql -tAc "SELECT pg_size_pretty(pg_relation_size('docs_gin'));")
echo "tsvector_gin,$(echo "$t1-$t0"|bc),$sz" >> "$RESULTS/indexes.csv"

# --- ParadeDB pg_search (optional) ---
if psql -tAc "SELECT 1 FROM pg_available_extensions WHERE name='pg_search'" | grep -q 1; then
    psql -c "CREATE EXTENSION IF NOT EXISTS pg_search;"
    t0=$(date +%s.%N)
    psql -c "CREATE INDEX docs_search ON docs USING bm25 (id, body) WITH (key_field='id');"
    t1=$(date +%s.%N)
    sz=$(psql -tAc "SELECT pg_size_pretty(pg_relation_size('docs_search'));")
    echo "pg_search,$(echo "$t1-$t0"|bc),$sz" >> "$RESULTS/indexes.csv"
fi

psql -c "VACUUM ANALYZE docs;"
echo "=== index build results ==="; cat "$RESULTS/indexes.csv"
