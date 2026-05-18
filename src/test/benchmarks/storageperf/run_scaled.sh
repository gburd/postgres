#!/usr/bin/env bash
#
# run_scaled.sh -- proper RECNO-vs-HEAP benchmark harness.
#
# Spins a throwaway cluster on a real (non-tmpfs) disk under a caller-supplied
# data dir, with a deliberately SMALL shared_buffers so we can exercise both a
# working set that fits in buffers and one that overflows them.  Runs:
#   1. the storage-model + compression-matrix SQL (architectural footprint)
#   2. recno_vs_heap_scaled.sql at two regimes: 'fits' and 'exceeds'
#
# A server restart is issued before the 'exceeds' cold scan so shared_buffers
# starts empty (the OS / ARC page cache cannot be dropped without root, so the
# 'exceeds' scan measures buffer-manager eviction + re-read from OS cache, which
# is the honest within-privilege "does not fit in buffers" signal; it is NOT a
# cold-disk number).
#
# Usage: run_scaled.sh <bindir> <datadir-parent> <port> <shared_buffers> \
#                       <fits_scale> <exceeds_scale> <valbytes> <sqldir> <outfile>
#
set -euo pipefail

BINDIR=$1
DATAPARENT=$2
PORT=$3
SHB=$4
FITS_SCALE=$5
EXCEEDS_SCALE=$6
VALBYTES=$7
SQLDIR=$8
OUT=$9

PGDATA="$DATAPARENT/data"
export PGDATA
PSQL="$BINDIR/psql -X -p $PORT -U postgres -d bench -v ON_ERROR_STOP=1"

log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$OUT"; }

start_server() {
  "$BINDIR/pg_ctl" -D "$PGDATA" -w -o "-p $PORT" -l "$DATAPARENT/server.log" start
}
stop_server() {
  "$BINDIR/pg_ctl" -D "$PGDATA" -w -m fast stop || true
}

: > "$OUT"
log "=== HOST $(hostname) ==="
log "bindir=$BINDIR shared_buffers=$SHB fits=$FITS_SCALE exceeds=$EXCEEDS_SCALE valbytes=$VALBYTES"
"$BINDIR/postgres" --version | tee -a "$OUT"
log "datadir=$PGDATA (df below; must NOT be tmpfs)"
df -hT "$DATAPARENT" 2>/dev/null | tee -a "$OUT" || df -h "$DATAPARENT" | tee -a "$OUT"

# Fresh cluster. Stop any stale postmaster still holding a previous run's
# data dir / port before we wipe and re-init (a leftover postmaster would
# otherwise keep the port bound and make the new server fail to start).
if [ -d "$PGDATA" ]; then
  "$BINDIR/pg_ctl" -D "$PGDATA" -m immediate stop >/dev/null 2>&1 || true
  rm -r "$PGDATA"
fi
"$BINDIR/initdb" -D "$PGDATA" -U postgres --no-sync >/dev/null
{
  echo "shared_buffers = $SHB"
  echo "max_wal_size = 8GB"
  echo "checkpoint_timeout = 30min"
  echo "fsync = on"
  echo "work_mem = 64MB"
  echo "maintenance_work_mem = 512MB"
  echo "track_io_timing = on"
  echo "port = $PORT"
} >> "$PGDATA/postgresql.conf"

start_server
"$BINDIR/createdb" -p "$PORT" -U postgres bench

log ""
log "########## ARCHITECTURAL FOOTPRINT + COMPRESSION (small, exact) ##########"
$PSQL -f "$SQLDIR/toast_overflow_nocomp.sql"   2>&1 | tee -a "$OUT"
$PSQL -f "$SQLDIR/compression_matrix.sql"       2>&1 | tee -a "$OUT"

log ""
log "########## SCALED REGIME: fits (working set < shared_buffers) ##########"
$PSQL -v scale="$FITS_SCALE" -v label="fits" -v valbytes="$VALBYTES" \
      -f "$SQLDIR/recno_vs_heap_scaled.sql" 2>&1 | tee -a "$OUT"

# Restart to empty shared_buffers before the large regime's cold scan.
log "--- restart (clears shared_buffers) ---"
stop_server
start_server

log ""
log "########## SCALED REGIME: exceeds (working set >> shared_buffers) ##########"
$PSQL -v scale="$EXCEEDS_SCALE" -v label="exceeds" -v valbytes="$VALBYTES" \
      -f "$SQLDIR/recno_vs_heap_scaled.sql" 2>&1 | tee -a "$OUT"

stop_server
log "=== DONE on $(hostname) ==="
