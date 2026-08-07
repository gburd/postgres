/* contrib/upsert/upsert--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION upsert" to load this file. \quit

-- The upsert extension's work happens in shared_preload_libraries via
-- _PG_init(): it registers the UPSERT keyword and the upsert_stmt
-- production with the parser_extension.h API.  There are no SQL-level
-- objects; CREATE EXTENSION exists only so the feature can be enabled
-- per-database in the standard way.
