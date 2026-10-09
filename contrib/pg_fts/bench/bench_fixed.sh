#!/bin/bash
PSQL="$HOME/pgi/bin/psql -U postgres -X -t -A"
RARE=alcohol; MID=slovakia; MID2=hungary; COMMON=year; COMMON2=world
med() { local q="$1" n=9 t times=(); for i in $(seq 1 $n); do
  t=$($PSQL -c "\timing on" -c "$q" 2>&1 | grep -oE "Time: [0-9.]+" | tail -1 | grep -oE "[0-9.]+"); times+=("$t"); done
  printf "%s\n" "${times[@]}" | sort -n | awk "{a[NR]=\$1} END{print a[int(NR/2)+1]}"; }
F="to_ftsdoc('english',body)"
echo "counts: $($PSQL -c "SELECT 'rare='||count(*) FROM docs2 WHERE tsv@@to_tsquery('english','$RARE')")  $($PSQL -c "SELECT 'common='||count(*) FROM docs2 WHERE tsv@@to_tsquery('english','$COMMON')")"
echo "Q1 rare-count      bm25=$(med "SELECT count(*) FROM docs2 WHERE $F @@@ to_ftsquery('english','$RARE')")  gin=$(med "SELECT count(*) FROM docs2 WHERE tsv @@ to_tsquery('english','$RARE')")"
echo "Q2 mid-count       bm25=$(med "SELECT count(*) FROM docs2 WHERE $F @@@ to_ftsquery('english','$MID')")  fts_count=$(med "SELECT fts_count('docs2_bm25',to_ftsquery('english','$MID'))")  gin=$(med "SELECT count(*) FROM docs2 WHERE tsv @@ to_tsquery('english','$MID')")"
echo "Q3 AND-count       bm25=$(med "SELECT count(*) FROM docs2 WHERE $F @@@ to_ftsquery('english','$MID & $MID2')")  gin=$(med "SELECT count(*) FROM docs2 WHERE tsv @@ to_tsquery('english','$MID & $MID2')")"
echo "Q4 rank10 mid&mid  bm25=$(med "SELECT id FROM docs2 WHERE $F @@@ to_ftsquery('english','$MID & $MID2') ORDER BY $F <=> to_ftsquery('english','$MID & $MID2') LIMIT 10")  gin=$(med "SELECT id FROM docs2 WHERE tsv @@ to_tsquery('english','$MID & $MID2') ORDER BY ts_rank(tsv,to_tsquery('english','$MID & $MID2')) DESC LIMIT 10")"
echo "Q5 rank10 comm&mid bm25=$(med "SELECT id FROM docs2 WHERE $F @@@ to_ftsquery('english','$COMMON & $MID') ORDER BY $F <=> to_ftsquery('english','$COMMON & $MID') LIMIT 10")  gin=$(med "SELECT id FROM docs2 WHERE tsv @@ to_tsquery('english','$COMMON & $MID') ORDER BY ts_rank(tsv,to_tsquery('english','$COMMON & $MID')) DESC LIMIT 10")"
echo "Q6 rank100 common  bm25=$(med "SELECT id FROM docs2 WHERE $F @@@ to_ftsquery('english','$COMMON') ORDER BY $F <=> to_ftsquery('english','$COMMON') LIMIT 100")  gin=$(med "SELECT id FROM docs2 WHERE tsv @@ to_tsquery('english','$COMMON') ORDER BY ts_rank(tsv,to_tsquery('english','$COMMON')) DESC LIMIT 100")"
echo "Q7 rank10 2common  bm25=$(med "SELECT id FROM docs2 WHERE $F @@@ to_ftsquery('english','$COMMON & $COMMON2') ORDER BY $F <=> to_ftsquery('english','$COMMON & $COMMON2') LIMIT 10")  gin=$(med "SELECT id FROM docs2 WHERE tsv @@ to_tsquery('english','$COMMON & $COMMON2') ORDER BY ts_rank(tsv,to_tsquery('english','$COMMON & $COMMON2')) DESC LIMIT 10")"
