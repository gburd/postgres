#!/bin/bash
# contrib/pg_fts/bench/phase_latency.sh
# Runs on a benchmark instance built from the pg_fts AMI.  Generates a corpus,
# builds pg_fts (bm25) and tsvector+GIN indexes, then A/B measures ranked-query
# latency (p50/p99) per query class.  Writes /home/fedora/results/*.csv.
#
# Env: NDOCS (default 1000000), VOCAB (default 50000), NQ (queries/class, 300).
set -euo pipefail
PGB=$HOME/pgfts/bin
PGDATA=$HOME/pgdata
NDOCS="${NDOCS:-1000000}"; VOCAB="${VOCAB:-50000}"; NQ="${NQ:-300}"
R=$HOME/results; mkdir -p "$R"
export PGDATA

start(){ $PGB/pg_ctl -D "$PGDATA" -l "$PGDATA/log" start -w >/dev/null 2>&1 || true; sleep 2; }
psql(){ $PGB/psql -U postgres -X -q "$@"; }

start
psql -c "DROP DATABASE IF EXISTS bench" 2>/dev/null || true
psql -c "CREATE DATABASE bench"
PSQLB(){ $PGB/psql -U postgres -X -q -d bench "$@"; }

echo "[$(date +%T)] generating corpus NDOCS=$NDOCS VOCAB=$VOCAB"
PSQLB -v ndocs="$NDOCS" -v vocab="$VOCAB" -f $HOME/gen_corpus.sql

echo "[$(date +%T)] CREATE EXTENSION + build indexes"
PSQLB -c "CREATE EXTENSION pg_fts"

# pg_fts bm25
t0=$(date +%s.%N)
PSQLB -c "CREATE INDEX docs_bm25 ON docs USING bm25 (to_ftsdoc('english'::regconfig, body))"
t1=$(date +%s.%N)
bm25_build=$(awk "BEGIN{print $t1-$t0}"); bm25_sz=$(PSQLB -tAc "SELECT pg_relation_size('docs_bm25')")

# tsvector + GIN
PSQLB -c "ALTER TABLE docs ADD COLUMN tsv tsvector"
t0=$(date +%s.%N)
PSQLB -c "UPDATE docs SET tsv = to_tsvector('english', body)"
PSQLB -c "CREATE INDEX docs_gin ON docs USING gin (tsv)"
t1=$(date +%s.%N)
gin_build=$(awk "BEGIN{print $t1-$t0}"); gin_sz=$(PSQLB -tAc "SELECT pg_relation_size('docs_gin')")
PSQLB -c "VACUUM ANALYZE docs"

echo "system,build_s,index_bytes" > "$R/indexes.csv"
echo "pg_fts,$bm25_build,$bm25_sz" >> "$R/indexes.csv"
echo "tsvector_gin,$gin_build,$gin_sz" >> "$R/indexes.csv"
cat "$R/indexes.csv"

# --- latency: measure per-query wall time with \timing, capture p50/p99 ---
# query terms: sample real vocabulary words (exist in corpus) + the rare marker
mapfile -t TERMS < <(PSQLB -tAc "
  SELECT 'word_'||lpad((floor($VOCAB*power(random(),3))::int+1)::text,5,'0')
  FROM generate_series(1,$NQ)")
TERMS+=("zzqrare")

runclass(){  # name, sql_template(%Q)
  local name="$1" tmpl="$2" q sql
  local out="$R/lat_${name}.txt"; : > "$out"
  for q in "${TERMS[@]}"; do
    sql="${tmpl//%Q/$q}"
    # \timing prints "Time: N.NNN ms"; grab it
    local ms
    ms=$(PSQLB -c "\\timing on" -c "$sql" 2>/dev/null | grep -oP 'Time: \K[0-9.]+' | head -1)
    [ -n "$ms" ] && echo "$ms" >> "$out"
  done
  # p50/p99
  sort -n "$out" > "$out.s"
  local n=$(wc -l < "$out.s")
  local p50=$(sed -n "$(( (n+1)/2 ))p" "$out.s")
  local p99=$(sed -n "$(( (n*99+99)/100 ))p" "$out.s")
  echo "$name,$n,$p50,$p99"
}

echo "class,n,p50_ms,p99_ms" > "$R/latency.csv"
# pg_fts ranked top-10 (single term)
runclass pgfts_term "SELECT id FROM docs WHERE to_ftsdoc('english'::regconfig,body) @@@ to_ftsquery('english'::regconfig,'%Q') ORDER BY to_ftsdoc('english'::regconfig,body) <=> to_ftsquery('english'::regconfig,'%Q') LIMIT 10" >> "$R/latency.csv"
# tsvector ranked top-10 (single term)
runclass tsv_term "SELECT id FROM docs WHERE tsv @@ to_tsquery('english','%Q') ORDER BY ts_rank(tsv,to_tsquery('english','%Q')) DESC LIMIT 10" >> "$R/latency.csv"

echo "=== latency.csv ==="; cat "$R/latency.csv"
echo "[$(date +%T)] DONE"
