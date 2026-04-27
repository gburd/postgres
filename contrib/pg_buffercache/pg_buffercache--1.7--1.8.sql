/* contrib/pg_buffercache/pg_buffercache--1.7--1.8.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pg_buffercache UPDATE TO '1.8'" to load this file. \quit

-- Version 1.8 adds dynamic buffer pool support to all functions.
-- No schema changes are needed; the existing functions now iterate
-- over dynamic pool buffers in addition to the default pool.
