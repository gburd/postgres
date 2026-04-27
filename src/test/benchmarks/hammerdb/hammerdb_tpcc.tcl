#!/usr/bin/env tclsh
#
# hammerdb_tpcc.tcl -- HammerDB TPC-C configuration template for PostgreSQL.
#
# This template can be used standalone or is generated dynamically by
# run_hammerdb.sh with strategy-specific settings.
#
# Usage (standalone):
#   cd $HAMMERDB_HOME
#   ./hammerdbcli auto /path/to/hammerdb_tpcc.tcl
#
# Environment variables (override defaults):
#   PG_HOST          PostgreSQL host (default: localhost)
#   PG_PORT          PostgreSQL port (default: 5432)
#   PG_USER          PostgreSQL superuser (default: postgres)
#   PG_DATABASE      Target database (default: hammerdb)
#   PG_WAREHOUSES    Number of warehouses (default: 10)
#   PG_VU_COUNT      Virtual users for test (default: 8)
#   PG_BUILD_VU      Virtual users for build (default: 4)
#   PG_RAMPUP_MIN    Ramp-up time in minutes (default: 2)
#   PG_DURATION_MIN  Test duration in minutes (default: 30)
#   PG_BUILD_SCHEMA  Whether to build schema: 1=yes 0=no (default: 1)
#
# After running, look for "NOPM" and "TPM" in output for results.

puts "=== HammerDB TPC-C for PostgreSQL ==="

# Database type
dbset db pg
dbset bm TPC-C

# Connection settings
set pg_host [expr {[info exists ::env(PG_HOST)] ? $::env(PG_HOST) : "localhost"}]
set pg_port [expr {[info exists ::env(PG_PORT)] ? $::env(PG_PORT) : "5432"}]
set pg_user [expr {[info exists ::env(PG_USER)] ? $::env(PG_USER) : "postgres"}]
set pg_database [expr {[info exists ::env(PG_DATABASE)] ? $::env(PG_DATABASE) : "hammerdb"}]
set pg_warehouses [expr {[info exists ::env(PG_WAREHOUSES)] ? $::env(PG_WAREHOUSES) : 10}]
set pg_vu_count [expr {[info exists ::env(PG_VU_COUNT)] ? $::env(PG_VU_COUNT) : 8}]
set pg_build_vu [expr {[info exists ::env(PG_BUILD_VU)] ? $::env(PG_BUILD_VU) : 4}]
set pg_rampup [expr {[info exists ::env(PG_RAMPUP_MIN)] ? $::env(PG_RAMPUP_MIN) : 2}]
set pg_duration [expr {[info exists ::env(PG_DURATION_MIN)] ? $::env(PG_DURATION_MIN) : 30}]
set pg_build [expr {[info exists ::env(PG_BUILD_SCHEMA)] ? $::env(PG_BUILD_SCHEMA) : 1}]

puts "Host: $pg_host:$pg_port"
puts "Database: $pg_database"
puts "User: $pg_user"
puts "Warehouses: $pg_warehouses"
puts "Virtual users: $pg_vu_count"
puts "Duration: ${pg_duration}min"

# Configure connection
diset connection pg_host $pg_host
diset connection pg_port $pg_port
diset connection pg_sslmode disable

# Configure TPC-C
diset tpcc pg_count_ware $pg_warehouses
diset tpcc pg_num_vu $pg_build_vu
diset tpcc pg_superuser $pg_user
diset tpcc pg_superuserpass ""
diset tpcc pg_defaultdbase postgres
diset tpcc pg_dbase $pg_database
diset tpcc pg_driver timed
diset tpcc pg_rampup $pg_rampup
diset tpcc pg_duration $pg_duration
diset tpcc pg_timeprofile true
diset tpcc pg_allwarehouse true
diset tpcc pg_storedprocs false

# Build schema if requested
if {$pg_build} {
    puts ""
    puts "Building TPC-C schema ($pg_warehouses warehouses)..."
    buildschema
    waittocomplete
    puts "Schema build complete."
}

# Run the benchmark
puts ""
puts "Starting TPC-C benchmark..."
puts "  Virtual users: $pg_vu_count"
puts "  Ramp-up: ${pg_rampup}min"
puts "  Duration: ${pg_duration}min"

loadscript
vuset vu $pg_vu_count
vucreate
vurun

# Wait for completion (duration + rampup + margin)
set total_seconds [expr {($pg_duration + $pg_rampup + 1) * 60}]
runtimer $total_seconds
vudestroy
waittocomplete

puts ""
puts "=== TPC-C Benchmark Complete ==="
