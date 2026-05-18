--
-- Compression matrix: RECNO (inline overflow + pluggable compression)
-- vs HEAP (out-of-line TOAST).
--
-- Sweeps every supported RECNO compression configuration and the two HEAP
-- TOAST compressors across three payload shapes, reporting stored bytes,
-- compression ratio, INSERT time, and full-scan retrieval time per cell.
-- Measure-after-only (no before baseline).
--
-- RECNO compression GUCs (all USERSET):
--   recno_compression_algorithm = auto | lz4 | zstd | none
--   recno_compression_level     = 1..22  (ZSTD honors it; LZ4 non-dict path
--                                          uses LZ4_compress_default, so level
--                                          is a no-op for plain LZ4)
--   recno_analyze_refresh_dict  = on     (ANALYZE trains+activates a dict for
--                                          subsequent writes only)
-- HEAP TOAST compressor selected via default_toast_compression (pglz | lz4).
--
-- The dict-on cells exercise the trained-dictionary path: load a training
-- sample -> ANALYZE (trains + stores the recnodict fork) -> TRUNCATE -> reload
-- the measured set -> measure. The dictionary only affects writes made after
-- training, which is why the reload is required.
--

\timing on

-- ----------------------------------------------------------------
-- Results accumulator + per-cell benchmark helper
-- ----------------------------------------------------------------
DROP TABLE IF EXISTS comp_results;
CREATE TABLE comp_results (
    payload     text,
    am          text,
    config      text,
    rows        int,
    stored_bytes bigint,
    insert_ms   numeric,
    scan_ms     numeric
);

-- Build payload expression for a given kind, keyed off generate_series alias g.
-- compressible : same long phrase every row (best case for any compressor)
-- mixed        : md5-seeded, per-row distinct, semi-compressible
-- random_hex   : many random-seeded md5 hashes concatenated per row. The inner
--                series references g (correlated) and random() (volatile) so
--                each row is distinct. This is hex text (4 bits/char), so it is
--                near-incompressible for byte-oriented compressors but still
--                slightly shrinkable by an entropy coder -- a realistic
--                worst-ish case, not a true random-bytes floor. Uses no
--                extension (avoids pgcrypto gen_random_bytes).
CREATE OR REPLACE FUNCTION comp_payload_expr(p_kind text, p_valbytes int)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE p_kind
    WHEN 'compressible'   THEN format('repeat(''The quick brown fox jumped. '', %s)',
                                       GREATEST(p_valbytes / 28, 1))
    WHEN 'mixed'          THEN format('repeat(md5(g::text), %s)',
                                       GREATEST(p_valbytes / 32, 1))
    WHEN 'random_hex'     THEN format(
        '(SELECT string_agg(md5((g * 100000 + s)::text || random()::text), '''') '
        'FROM generate_series(1, %s) s)',
        GREATEST(p_valbytes / 32, 1))
  END
$$;

-- Run one cell: caller has already SET the relevant GUCs.  Builds a table with
-- the requested access method, optionally trains a dictionary, loads the
-- measured set, and records size + insert/scan timings into comp_results.
CREATE OR REPLACE FUNCTION comp_bench_cell(p_payload text, p_am text,
                                           p_config text, p_dict bool,
                                           p_rows int, p_valbytes int)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    expr     text := comp_payload_expr(p_payload, p_valbytes);
    t0       timestamptz;
    t1       timestamptz;
    ins_ms   numeric;
    scn_ms   numeric;
    nbytes   bigint;
    dummy    bigint;
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS comp_cell';
    EXECUTE format('CREATE TABLE comp_cell (id bigserial, data text) USING %I', p_am);

    -- Dictionary training pass (recno, dict-on only).
    IF p_dict THEN
        EXECUTE format(
            'INSERT INTO comp_cell (data) SELECT %s FROM generate_series(1, %s) g',
            expr, GREATEST(p_rows / 5, 1000));
        EXECUTE 'ANALYZE comp_cell';
        EXECUTE 'TRUNCATE comp_cell';
    END IF;

    t0 := clock_timestamp();
    EXECUTE format(
        'INSERT INTO comp_cell (data) SELECT %s FROM generate_series(1, %s) g',
        expr, p_rows);
    t1 := clock_timestamp();
    ins_ms := round(extract(epoch FROM (t1 - t0)) * 1000, 1);

    t0 := clock_timestamp();
    EXECUTE 'SELECT sum(length(data)) FROM comp_cell' INTO dummy;
    t1 := clock_timestamp();
    scn_ms := round(extract(epoch FROM (t1 - t0)) * 1000, 1);

    nbytes := pg_total_relation_size('comp_cell');

    INSERT INTO comp_results
        VALUES (p_payload, p_am, p_config, p_rows, nbytes, ins_ms, scn_ms);

    EXECUTE 'DROP TABLE comp_cell';
END
$$;

-- ----------------------------------------------------------------
-- Drive the matrix.  Parameters held constant across cells.
-- 4000-byte values guarantee TOAST (heap) / overflow (recno) engagement.
-- ----------------------------------------------------------------
\set NROWS 5000
\set VALB  4000

\echo '=== Running compression matrix (this takes a few minutes) ==='

DO $driver$
DECLARE
    payload text;
    payloads text[] := ARRAY['compressible', 'mixed', 'random_hex'];
    nrows int := 5000;
    valb  int := 4000;
BEGIN
    FOREACH payload IN ARRAY payloads LOOP
        -- RECNO: none
        SET recno_enable_compression = on;
        SET recno_compression_algorithm = 'none';
        PERFORM comp_bench_cell(payload, 'recno', 'recno none', false, nrows, valb);

        -- RECNO: lz4 (level is a no-op on the non-dict path) dict off/on
        SET recno_compression_algorithm = 'lz4';
        SET recno_compression_level = 3;
        PERFORM comp_bench_cell(payload, 'recno', 'recno lz4 dict-off', false, nrows, valb);
        PERFORM comp_bench_cell(payload, 'recno', 'recno lz4 dict-on',  true,  nrows, valb);

        -- RECNO: zstd at min / default / max, dict off
        SET recno_compression_algorithm = 'zstd';
        SET recno_compression_level = 1;
        PERFORM comp_bench_cell(payload, 'recno', 'recno zstd L1 dict-off', false, nrows, valb);
        SET recno_compression_level = 3;
        PERFORM comp_bench_cell(payload, 'recno', 'recno zstd L3 dict-off', false, nrows, valb);
        SET recno_compression_level = 22;
        PERFORM comp_bench_cell(payload, 'recno', 'recno zstd L22 dict-off', false, nrows, valb);

        -- RECNO: zstd default level, dict on
        SET recno_compression_level = 3;
        PERFORM comp_bench_cell(payload, 'recno', 'recno zstd L3 dict-on', true, nrows, valb);

        -- RECNO: auto (resolver picks algorithm), default level
        SET recno_compression_algorithm = 'auto';
        SET recno_compression_level = 3;
        PERFORM comp_bench_cell(payload, 'recno', 'recno auto dict-off', false, nrows, valb);

        -- HEAP baselines: TOAST pglz and lz4
        SET default_toast_compression = 'pglz';
        PERFORM comp_bench_cell(payload, 'heap', 'heap toast pglz', false, nrows, valb);
        SET default_toast_compression = 'lz4';
        PERFORM comp_bench_cell(payload, 'heap', 'heap toast lz4', false, nrows, valb);
    END LOOP;
END
$driver$;

RESET recno_enable_compression;
RESET recno_compression_algorithm;
RESET recno_compression_level;
RESET default_toast_compression;

-- ----------------------------------------------------------------
-- Report: ratio anchored to the uncompressed RECNO-none cell of the
-- same payload (the raw inline-stored size).
-- ----------------------------------------------------------------
\echo ''
\echo '=== Compression matrix results ==='
\echo '(ratio = stored_bytes(recno none, same payload) / stored_bytes; >1 = smaller)'

SELECT
    r.payload,
    r.config,
    pg_size_pretty(r.stored_bytes)                              AS size,
    round(base.stored_bytes::numeric
          / GREATEST(r.stored_bytes, 1), 2)                     AS ratio,
    r.insert_ms,
    r.scan_ms
FROM comp_results r
JOIN comp_results base
  ON base.payload = r.payload AND base.config = 'recno none'
ORDER BY
    array_position(ARRAY['compressible','mixed','random_hex'], r.payload),
    r.stored_bytes;

DROP FUNCTION comp_bench_cell(text, text, text, bool, int, int);
DROP FUNCTION comp_payload_expr(text, int);
DROP TABLE comp_results;

\timing off
