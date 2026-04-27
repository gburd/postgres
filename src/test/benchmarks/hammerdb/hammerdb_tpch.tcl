#!/usr/bin/env tclsh
#
# hammerdb_tpch.tcl -- HammerDB TPC-H configuration template for PostgreSQL.
#
# TPC-H exercises analytical / decision-support queries that stress the
# buffer pool differently from OLTP (TPC-C): large sequential scans,
# hash joins, sort spills, and minimal writes.
#
# Usage (standalone):
#   cd $HAMMERDB_HOME
#   ./hammerdbcli auto /path/to/hammerdb_tpch.tcl
#
# Environment variables (override defaults):
#   PG_HOST              PostgreSQL host (default: localhost)
#   PG_PORT              PostgreSQL port (default: 5432)
#   PG_USER              PostgreSQL superuser (default: postgres)
#   PG_DATABASE          Target database (default: hammerdb_tpch)
#   PG_SCALE_FACTOR      TPC-H scale factor (default: 1)
#   PG_TPCH_THREADS      Threads for query execution (default: 4)
#   PG_TOTAL_QUERYSETS    Number of complete query set runs (default: 3)
#   PG_BUILD_SCHEMA      Whether to build schema: 1=yes 0=no (default: 1)
#
# Scale factor guide:
#   SF=1  => ~1GB raw data  (small, good for dev testing)
#   SF=10 => ~10GB raw data (medium, exceeds typical shared_buffers)
#   SF=100 => ~100GB raw data (large, production-like pressure)

puts "=== HammerDB TPC-H for PostgreSQL ==="

# Database type
dbset db pg
dbset bm TPC-H

# Connection settings
set pg_host [expr {[info exists ::env(PG_HOST)] ? $::env(PG_HOST) : "localhost"}]
set pg_port [expr {[info exists ::env(PG_PORT)] ? $::env(PG_PORT) : "5432"}]
set pg_user [expr {[info exists ::env(PG_USER)] ? $::env(PG_USER) : "postgres"}]
set pg_database [expr {[info exists ::env(PG_DATABASE)] ? $::env(PG_DATABASE) : "hammerdb_tpch"}]
set pg_scale [expr {[info exists ::env(PG_SCALE_FACTOR)] ? $::env(PG_SCALE_FACTOR) : 1}]
set pg_threads [expr {[info exists ::env(PG_TPCH_THREADS)] ? $::env(PG_TPCH_THREADS) : 4}]
set pg_querysets [expr {[info exists ::env(PG_TOTAL_QUERYSETS)] ? $::env(PG_TOTAL_QUERYSETS) : 3}]
set pg_build [expr {[info exists ::env(PG_BUILD_SCHEMA)] ? $::env(PG_BUILD_SCHEMA) : 1}]

puts "Host: $pg_host:$pg_port"
puts "Database: $pg_database"
puts "User: $pg_user"
puts "Scale factor: $pg_scale"
puts "Threads: $pg_threads"
puts "Query sets: $pg_querysets"

# Configure connection
diset connection pg_host $pg_host
diset connection pg_port $pg_port
diset connection pg_sslmode disable

# Configure TPC-H
diset tpch pg_tpch_superuser $pg_user
diset tpch pg_tpch_superuserpass ""
diset tpch pg_tpch_defaultdbase postgres
diset tpch pg_tpch_dbase $pg_database
diset tpch pg_scale_fact $pg_scale
diset tpch pg_num_tpch_threads $pg_threads
diset tpch pg_tpch_driver timed
diset tpch pg_total_querysets $pg_querysets
diset tpch pg_raise_query_error false
diset tpch pg_verbose false

# Build schema if requested
if {$pg_build} {
    puts ""
    puts "Building TPC-H schema (scale factor $pg_scale)..."
    buildschema
    waittocomplete
    puts "Schema build complete."
}

# Run the benchmark
puts ""
puts "Starting TPC-H benchmark..."
puts "  Threads: $pg_threads"
puts "  Query sets: $pg_querysets"

loadscript
vuset vu $pg_threads
vucreate
vurun
waittocomplete

puts ""
puts "=== TPC-H Benchmark Complete ==="
