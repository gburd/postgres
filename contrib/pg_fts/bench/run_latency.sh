#!/bin/bash
# contrib/pg_fts/bench/run_latency.sh
# Per-class BM25 query-latency A/B benchmark: pg_fts vs tsvector+GIN (and
# pg_search if installed).  Adapted from Jim's run_pgbench_ab.sh: A/B
# alternation, 3 runs, medians, drop_caches between runs, CSV out.
#
# Prereqs: a database `bench` with table docs(id,title,body); indexes built by
# load.sh (docs_bm25, docs_gin, optionally docs_search); pg_fts installed.
# A query file bench/queries.txt with one search term/phrase per line.
set -euo pipefail

PGBIN="${PGBIN:-/postgres/pgfts/bin}"
PGDATA="${PGDATA:-/pgdata/main}"
DB=bench
RUNS="${RUNS:-3}"
NQUERIES="${NQUERIES:-200}"     # queries per class per run
RESULTS="${RESULTS:-/pgfts_bench/results}"
QUERIES="${QUERIES:-bench/queries.txt}"
mkdir -p "$RESULTS"

drop_caches() { sync; echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null; sleep 1; }
psql() { "$PGBIN/psql" -X -q -d "$DB" "$@"; }

# time N queries of one class against one system; print total ms
time_class() {
    local sql_template="$1"     # contains %Q for the query text
    local n=0 start end
    start=$(date +%s%N)
    while IFS= read -r q && [ "$n" -lt "$NQUERIES" ]; do
        local sql="${sql_template//%Q/$q}"
        psql -c "$sql" >/dev/null
        n=$((n+1))
    done < "$QUERIES"
    end=$(date +%s%N)
    echo "scale=3; ($end - $start)/1000000/$n" | bc   # avg ms/query
}

echo "system,class,run,avg_ms" > "$RESULTS/latency.csv"

# Query templates per system and class.  Add pg_search rows when installed.
run_system() {
    local sys="$1"
    declare -A T
    if [ "$sys" = "pgfts" ]; then
        T[term]="SELECT id FROM docs WHERE d @@@ '%Q'::ftsquery ORDER BY d <=> '%Q'::ftsquery LIMIT 10"
        T[and]="SELECT id FROM docs WHERE d @@@ '%Q'::ftsquery ORDER BY d <=> '%Q'::ftsquery LIMIT 10"
        T[rank]="SELECT id FROM docs WHERE d @@@ '%Q'::ftsquery ORDER BY d <=> '%Q'::ftsquery LIMIT 10"
    elif [ "$sys" = "tsvector" ]; then
        T[term]="SELECT id FROM docs WHERE tsv @@ to_tsquery('english','%Q') ORDER BY ts_rank(tsv, to_tsquery('english','%Q')) DESC LIMIT 10"
        T[and]="$T[term]"
        T[rank]="$T[term]"
    fi
    for class in term and rank; do
        for run in $(seq 1 "$RUNS"); do
            drop_caches
            local ms
            ms=$(time_class "${T[$class]}")
            echo "$sys,$class,$run,$ms" >> "$RESULTS/latency.csv"
            echo "[$(date +%H:%M:%S)] $sys $class run $run: $ms ms/query"
        done
    done
}

# A/B alternate the systems per class/run for drift control
run_system pgfts
run_system tsvector
# run_system pgsearch   # enable when pg_search is installed

echo "=== latency.csv ==="; cat "$RESULTS/latency.csv"

# medians per (system,class)
echo; echo "=== medians (ms/query) ==="
awk -F, 'NR>1{k=$1","$2; v[k]=v[k]" "$4}
END{for(k in v){n=split(v[k],a," ");
  for(i=1;i<n;i++)for(j=i+1;j<=n;j++)if(a[j]<a[i]){t=a[i];a[i]=a[j];a[j]=t}
  print k","a[int((n+1)/2)]}}' "$RESULTS/latency.csv" | sort
