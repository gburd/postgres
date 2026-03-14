# Orvos Compression Enhancement Report

**Version**: 2.0 (Framework -- awaiting benchmark data)
**Date**: 2026-03-14
**Status**: Template ready; `[TBD]` placeholders to be filled from benchmark suite output
**Prerequisite**: Task #10 (test suite) must complete before running benchmarks

---

## 1  Executive Summary

Seven type-specific compression techniques were implemented on top of the
existing general-purpose compression layer (zstd / LZ4 / pglz), bringing
Orvos into parity with the compression stacks of DuckDB, Apache Parquet,
and Apache Arrow.

| # | Technique                  | Task | Flag Constant                    | Target Gain                 |
|---|----------------------------|------|----------------------------------|-----------------------------|
| 1 | Boolean bit-packing        | #2   | `OVBT_ATTR_BITPACKED` (0x0010)  | 8x for boolean columns      |
| 2 | Enhanced NULL handling      | #3   | `NO_NULLS`/`SPARSE`/`RLE`       | 100% (no NULLs) to 8-16x   |
| 3 | Dictionary encoding         | #4   | `OVBT_ATTR_FORMAT_DICT` (0x0100)| 10-100x low-cardinality     |
| 4 | UUID fixed-binary           | #5   | `OVBT_ATTR_FORMAT_FIXED_BIN`    | 6-31% space savings         |
| 5 | Frame of Reference (FOR)    | #6   | `OVBT_ATTR_FORMAT_FOR` (0x0008) | 2-8x sequential/clustered   |
| 6 | FSST string compression     | #7   | `OVBT_ATTR_FORMAT_FSST` (0x0400)| 30-60% additional on strings|
| 7 | Native varlena format       | #8   | `OVBT_ATTR_FORMAT_NATIVE_VARLENA`| 15-30% faster string I/O   |

### Key Findings

> **[TBD -- populate after running benchmark suite]**
>
> - Overall compression improvement: **[TBD]** x average across mixed workloads
> - Best single-technique gain: **[TBD]** (technique: **[TBD]**)
> - Query performance on analytical scans: **[TBD]** % change
> - Top 3 recommendations:
>   1. **[TBD]**
>   2. **[TBD]**
>   3. **[TBD]**

### Compression Pipeline Architecture

Encodings compose in layers -- each auto-selects based on data characteristics:

```
User Data
    |
[Type-Specific Encoding]      <-- Boolean bit-packing, FOR, Dictionary, UUID fixed-binary
    |
[NULL Optimization]           <-- NO_NULLS, SPARSE_NULLS, RLE_NULLS
    |
[FSST Pre-compression]        <-- String-specific symbol table compression
    |
[Simple8b Integer Encoding]   <-- TID delta compression (existing)
    |
[Generic Compression]         <-- zstd / lz4 / pglz
    |
Stored on Disk
```

---

## 2  Methodology

### 2.1  Benchmark Suite

All measurements use the shell-based benchmark suite in `benchmarks/`
(Task #1).  Eight workload scripts exercise different access patterns.
The type-specific compression benchmark (`workload_type_compression.sh`)
directly targets each new encoding.

| Script                          | Focus                                    |
|---------------------------------|------------------------------------------|
| `simple_comparison.sh`          | Baseline HEAP vs Orvos                   |
| `workload_analytical.sh`        | TPC-H-like OLAP queries                 |
| `workload_compression.sh`       | General compression effectiveness         |
| `workload_oltp.sh`              | Single-row OLTP operations               |
| `workload_index.sh`             | B-tree index build and scan              |
| `workload_update_delete.sh`     | DML operations and VACUUM                |
| `workload_mixed.sh`             | Realistic mixed workload (70/20/8/2)     |
| `workload_type_compression.sh`  | Type-specific encoding benchmarks        |

Run all:

```bash
cd benchmarks/
./run_benchmarks.sh benchmark_db
# Results saved to results_YYYYMMDD_HHMMSS/
```

### 2.2  Test Matrix

**Table shapes:**

| Shape  | Columns | Rationale                            |
|--------|---------|--------------------------------------|
| Narrow | 3-5     | Heavy read; column projection minimal |
| Medium | 10-30   | Typical OLAP fact table               |
| Wide   | 50-120  | Extreme projection benefit            |

**Data types and encodings exercised:**

| Data Type / Pattern       | Encoding Triggered              | Flag                              |
|---------------------------|---------------------------------|-----------------------------------|
| `BOOLEAN`                 | Bit-packing (8 per byte)        | `OVBT_ATTR_BITPACKED` (0x0010)   |
| Low-cardinality `VARCHAR` | Dictionary encoding             | `OVBT_ATTR_FORMAT_DICT` (0x0100) |
| Narrow-range `INT/BIGINT` | Frame of Reference              | `OVBT_ATTR_FORMAT_FOR` (0x0008)  |
| `UUID`                    | Fixed 16-byte binary            | `OVBT_ATTR_FORMAT_FIXED_BIN`     |
| String columns            | FSST pre-filter + zstd          | `OVBT_ATTR_FORMAT_FSST` (0x0400) |
| Short `TEXT/VARCHAR`       | Native varlena passthrough      | `OVBT_ATTR_FORMAT_NATIVE_VARLENA`|
| No NULLs                  | Bitmap omitted                  | `OVBT_ATTR_NO_NULLS` (0x0020)   |
| <5% NULLs                 | Sparse (offset, count) pairs    | `OVBT_ATTR_SPARSE_NULLS` (0x0040)|
| Sequential NULL runs       | Run-length encoding             | `OVBT_ATTR_RLE_NULLS` (0x0080)  |

**Data distributions:**

- Random (uniform), Clustered (80/20), Correlated, Low cardinality (4-10
  distinct values), High NULL density (50%, 95%, RLE pattern).

**Table sizes:**

| Label  | Row Count | Purpose                      |
|--------|-----------|------------------------------|
| Small  | 1K-10K    | Overhead-dominated           |
| Medium | 100K-1M   | Typical production           |
| Large  | 10M-100M  | Compression-critical regime  |

### 2.3  Query Patterns

| Pattern             | SQL Example                                          |
|---------------------|------------------------------------------------------|
| Full table scan     | `SELECT * FROM t`                                    |
| Column projection   | `SELECT col1, col5 FROM t`                           |
| Filtered scan       | `SELECT ... WHERE col = value`                       |
| Aggregation         | `SELECT COUNT(*), SUM(col) FROM t`                   |
| GROUP BY            | `SELECT cat, COUNT(*) FROM t GROUP BY cat`           |
| Index scan          | `SELECT ... WHERE id = ?` (B-tree)                   |
| Bitmap scan         | `SELECT ... WHERE col IN (...)`                      |

### 2.4  Measurement Methodology

- Each query executed 3 times; median reported.
- `EXPLAIN (ANALYZE, BUFFERS)` captures execution time, buffer hits/reads.
- Storage via `pg_relation_size()` after `ANALYZE`.
- Per-column ratios from `pg_ov_btree_pages()`.
- Planner statistics verified via `pg_statistic` (`stakind = 10001`).
- Background autovacuum disabled during runs.

### 2.5  Hardware and Configuration

> **[TBD -- fill in from actual test environment]**
>
> - CPU: **[TBD]**
> - RAM: **[TBD]**
> - Storage: **[TBD]** (SSD / NVMe / HDD)
> - PostgreSQL version: **[TBD]** (commit **[TBD]**)
> - `shared_buffers`: **[TBD]**
> - `work_mem`: **[TBD]**
> - `effective_cache_size`: **[TBD]**
> - Compression algorithm: zstd (build-time default)

---

## 3  Compression Results by Technique

### 3.1  Boolean Bit-Packing

**Implementation**: `src/backend/access/orvos/orvos_attitem.c`
**Flag**: `OVBT_ATTR_BITPACKED` (0x0010)

Values packed 8 per byte.  NULL bitmap remains separate.  On read, the
packed byte is expanded back to individual `Datum` booleans.

| Metric                    | Target       | Actual       |
|---------------------------|-------------|-------------|
| Compression ratio (bools) | 8x           | **[TBD]**   |
| Whole-table ratio (8 bool + 1 INT) | 4-6x | **[TBD]** |
| COUNT WHERE multi-flag    | < 1.2x HEAP  | **[TBD]**   |
| Decompression overhead    | < 5%         | **[TBD]**   |

**Best use case**: Tables with many boolean flags (feature toggles, status
bits, permission matrices).

**Limitations**: Only applies to `BOOLOID`.  Not combined with dictionary
or FOR encoding.

**Benchmark data** (`workload_type_compression.sh`, Test 1):

```
[TBD -- paste storage comparison output here]
```

```
[TBD -- paste EXPLAIN (ANALYZE, BUFFERS) output for HEAP and Orvos here]
```

---

### 3.2  Enhanced NULL Handling

**Implementation**: `orvos_attitem.c` -- `ov_choose_null_encoding()` selects
among four strategies based on NULL density and distribution.

| Strategy          | Flag                       | When Selected                     |
|-------------------|----------------------------|-----------------------------------|
| Bitmap omitted    | `OVBT_ATTR_NO_NULLS`      | Zero NULLs in the item            |
| Sparse encoding   | `OVBT_ATTR_SPARSE_NULLS`  | < 5% NULLs, sparse saves space    |
| RLE encoding      | `OVBT_ATTR_RLE_NULLS`     | Runs of 8+ consecutive NULLs      |
| Standard bitmap   | `OVBT_HAS_NULLS`          | All other patterns                |

Data structures:

- `OVSparseNullEntry` (4 B): `{uint16 sn_position, uint16 sn_count}`.
- `OVRleNullEntry` (2 B): high bit = is_null, low 15 bits = run length.

| Metric                            | Target  | Actual    |
|-----------------------------------|---------|-----------|
| No NULLs (bitmap savings)         | 100%    | **[TBD]** |
| Sparse (5% NULLs)                 | 90%+    | **[TBD]** |
| Dense (50% NULLs)                 | Baseline| **[TBD]** |
| RLE (alternating blocks of 100)   | 8-16x   | **[TBD]** |
| Dense (95% NULLs)                 | Baseline| **[TBD]** |

**Benchmark data** (`workload_type_compression.sh`, Test 5):

```
[TBD -- paste storage comparison output here]
```

```
[TBD -- paste EXPLAIN (ANALYZE, BUFFERS) output here]
```

---

### 3.3  Dictionary Encoding

**Implementation**: `src/backend/access/orvos/orvos_dict.c` /
`src/include/access/orvos_dict.h`
**Flag**: `OVBT_ATTR_FORMAT_DICT` (0x0100)

Replaces repeated values with `uint16` indices into a dictionary of
distinct values.

Key parameters:

| Parameter                         | Value    | Description                      |
|----------------------------------|----------|----------------------------------|
| `OV_DICT_CARDINALITY_THRESHOLD`  | 0.01     | Max distinct/total ratio         |
| `OV_DICT_MAX_ENTRIES`            | 65534    | Max dictionary entries (uint16)  |
| `OV_DICT_NULL_INDEX`             | 0xFFFF   | Reserved NULL marker             |
| `OV_DICT_MAX_TOTAL_SIZE`         | 64 KB    | Max total dictionary data        |

On-disk layout:

```
[OVDictHeader: 8 bytes]
  uint16  num_entries
  uint16  entry_size        (0 = variable-length)
  uint32  total_data_size
[offsets: uint32 * num_entries]
[values data: total_data_size bytes]
[indices: uint16 * num_elements]
```

Decision logic (`ov_dict_should_encode()`):
1. At least 16 items.
2. Skip byval types <= 2 bytes.
3. Cardinality < 1% (with floor of 4 distinct values).
4. Encoded size < raw data size.

| Metric                          | Target      | Actual    |
|---------------------------------|-------------|-----------|
| Low-card VARCHAR (4-6 distinct) | 10-100x     | **[TBD]** |
| Whole-table ratio               | 3-8x        | **[TBD]** |
| GROUP BY on dict column         | 1.5-3x faster | **[TBD]** |
| Decompression overhead          | < 10%       | **[TBD]** |

**Benchmark data** (`workload_type_compression.sh`, Test 2):

```
[TBD -- paste storage comparison and GROUP BY query output here]
```

Per-column ratios from `pg_ov_btree_pages('dict_orvos')`:

```
[TBD -- paste per-column output here]
```

---

### 3.4  UUID Fixed-Binary Storage

**Implementation**: `orvos_attitem.c`
**Flag**: `OVBT_ATTR_FORMAT_FIXED_BIN` (0x0200)

UUIDs stored as 16-byte fixed binary, bypassing the variable-length
varlena header.  Reconstructed on read.

| Metric                    | Target     | Actual    |
|---------------------------|-----------|-----------|
| Per-UUID space savings    | 6-31%     | **[TBD]** |
| Whole-table ratio (2 UUID cols) | 1.5-3x | **[TBD]** |
| COUNT DISTINCT            | Similar   | **[TBD]** |

**Benchmark data** (`workload_type_compression.sh`, Test 4):

```
[TBD -- paste storage comparison and query output here]
```

---

### 3.5  Frame of Reference (FOR) Encoding

**Implementation**: `orvos_attitem.c`
**Flag**: `OVBT_ATTR_FORMAT_FOR` (0x0008)

For pass-by-value integer types where `max - min` fits in significantly
fewer bits than the original width.

On-disk layout:

```
[OVForHeader: 10 bytes]
  uint64  for_frame_min         minimum value in the frame
  uint8   for_bits_per_value    bits per delta (0..64)
  uint8   for_attlen            original attribute length (1,2,4,8)
[packed deltas: ceil(nelems * bpv / 8) bytes]
```

Delta formula: `delta = value - for_frame_min`.

| Metric                          | Target   | Actual    |
|---------------------------------|----------|-----------|
| Narrow-range INT (range ~100)   | 4-6x     | **[TBD]** |
| Narrow-range BIGINT (range ~1K) | 4-8x     | **[TBD]** |
| Sequential counter (delta=1)    | 8-16x    | **[TBD]** |
| SMALLINT temperature (range ~50)| 2-3x     | **[TBD]** |
| Range scan performance          | Similar  | **[TBD]** |

**Benchmark data** (`workload_type_compression.sh`, Test 3):

```
[TBD -- paste storage comparison and range scan output here]
```

Per-column ratios from `pg_ov_btree_pages('for_orvos')`:

```
[TBD -- paste per-column output here]
```

---

### 3.6  FSST String Compression

**Implementation**: `src/backend/access/orvos/orvos_fsst.c` /
`src/include/access/orvos_fsst.h`
**Flag**: `OVBT_ATTR_FORMAT_FSST` (0x0400)

Builds a 256-entry symbol table of frequently occurring byte sequences
(1-8 bytes) from a sample of column values.  Replaces multi-byte sequences
with single-byte codes.  Code 255 is reserved as an escape.

Algorithm (based on Boncz et al., VLDB 2020):

1. Sample up to 64 KB of string data.
2. Count n-gram frequencies for lengths 2-8 using a 64K-entry hash table.
3. Score: `count * (len - 1)`.
4. Greedy top-255 selection.
5. Encode: longest match at each position, or escape + literal.
6. Apply zstd on top: `string -> FSST -> zstd -> disk`.

Integration: `ov_try_compress_with_fsst()` and `ov_decompress_with_fsst()`
in `orvos_compression.h`.

| Metric                              | Target    | Actual    |
|-------------------------------------|-----------|-----------|
| Additional compression (over zstd)  | 30-60%    | **[TBD]** |
| Compression throughput              | > 200 MB/s| **[TBD]** |
| Decompression throughput            | > 500 MB/s| **[TBD]** |
| Query scan time impact              | < 15%     | **[TBD]** |

**Benchmark data** (from `workload_compression.sh` with high-compressibility
string data):

```
[TBD -- paste FSST-specific benchmark output here]
```

---

### 3.7  Native Varlena Format

**Implementation**: `orvos_attitem.c`
**Flag**: `OVBT_ATTR_FORMAT_NATIVE_VARLENA` (0x0004)

Short varlena values (data <= 126 bytes) stored in PostgreSQL's native
1-byte short varlena format rather than the custom Orvos length-prefix
encoding.  Eliminates per-datum format conversion on read.

| Metric                        | Target        | Actual    |
|-------------------------------|--------------|-----------|
| INSERT throughput improvement | 15-30%        | **[TBD]** |
| SELECT throughput improvement | 15-30%        | **[TBD]** |
| Memory allocation reduction  | Significant   | **[TBD]** |
| Compression ratio impact     | Neutral       | **[TBD]** |

**Benchmark data**:

```
[TBD -- paste INSERT/SELECT timing comparison here]
```

---

## 4  Query Performance Analysis

All query times are median of 3 executions.  "Speedup" is
`HEAP_time / Orvos_time`; values > 1.0 mean Orvos is faster.

### 4.1  Full Table Scan

```sql
SELECT * FROM t;
```

| Table Shape | Rows   | HEAP (ms)  | Orvos (ms)  | Speedup   | Notes          |
|-------------|--------|-----------|-------------|-----------|----------------|
| Narrow (5)  | 500K   | **[TBD]** | **[TBD]**   | **[TBD]** |                |
| Medium (20) | 500K   | **[TBD]** | **[TBD]**   | **[TBD]** |                |
| Wide (80)   | 500K   | **[TBD]** | **[TBD]**   | **[TBD]** |                |

> Full scans reconstruct all columns.  Expected: Orvos slightly slower for
> narrow tables, competitive for wide tables due to compression reducing I/O.

### 4.2  Column Projection

```sql
SELECT col1, col5 FROM t;
```

| Table Shape  | Cols Projected | HEAP (ms)  | Orvos (ms)  | Speedup   |
|-------------|---------------|-----------|-------------|-----------|
| Medium (20) | 2 of 20       | **[TBD]** | **[TBD]**   | **[TBD]** |
| Wide (80)   | 3 of 80       | **[TBD]** | **[TBD]**   | **[TBD]** |
| Wide (80)   | 10 of 80      | **[TBD]** | **[TBD]**   | **[TBD]** |

> Column projection is the core columnar advantage.

### 4.3  Filtered Scan

```sql
SELECT col1, col2 FROM t WHERE status = 'active';
```

| Filter Selectivity | HEAP (ms)  | Orvos (ms)  | Speedup   |
|--------------------|-----------|-------------|-----------|
| 1% of rows         | **[TBD]** | **[TBD]**   | **[TBD]** |
| 10% of rows        | **[TBD]** | **[TBD]**   | **[TBD]** |
| 50% of rows        | **[TBD]** | **[TBD]**   | **[TBD]** |

### 4.4  Aggregation

```sql
SELECT COUNT(*), SUM(amount), AVG(price) FROM t;
```

| Columns Aggregated | HEAP (ms)  | Orvos (ms)  | Speedup   |
|--------------------|-----------|-------------|-----------|
| 1 column           | **[TBD]** | **[TBD]**   | **[TBD]** |
| 3 columns          | **[TBD]** | **[TBD]**   | **[TBD]** |
| All columns        | **[TBD]** | **[TBD]**   | **[TBD]** |

### 4.5  GROUP BY

```sql
SELECT category, region, COUNT(*) FROM t GROUP BY category, region;
```

| Distinct Groups    | HEAP (ms)  | Orvos (ms)  | Speedup   | Notes              |
|--------------------|-----------|-------------|-----------|---------------------|
| 20 (dict-encoded)  | **[TBD]** | **[TBD]**   | **[TBD]** | Low-cardinality    |
| 1000               | **[TBD]** | **[TBD]**   | **[TBD]** | Medium cardinality |
| 100K               | **[TBD]** | **[TBD]**   | **[TBD]** | High cardinality   |

> Dictionary-encoded columns with few distinct groups should show the
> largest speedup.

### 4.6  Index Scan

| Operation       | HEAP (ms)  | Orvos (ms)  | Speedup   | Notes               |
|-----------------|-----------|-------------|-----------|----------------------|
| Point lookup    | **[TBD]** | **[TBD]**   | **[TBD]** | Single row by PK    |
| Range scan (1K) | **[TBD]** | **[TBD]**   | **[TBD]** | 1K rows in range    |
| Index-only scan | **[TBD]** | **[TBD]**   | **[TBD]** | Covering index      |

---

## 5  Space Savings Analysis

### 5.1  Per-Column Compression Ratios

Extracted via:

```sql
SELECT attno, count(*) AS pages,
       sum(totalsz) AS total_bytes,
       sum(uncompressedsz) AS uncompressed_bytes,
       ROUND(sum(uncompressedsz::numeric) / NULLIF(sum(totalsz), 0), 2) AS ratio
FROM pg_ov_btree_pages('tablename')
GROUP BY attno ORDER BY attno;
```

**Dictionary-encoded table (`dict_orvos`, 500K rows):**

| attno | Column      | Type        | Pages      | Ratio      | Notes           |
|-------|-------------|-------------|-----------|-----------|-----------------|
| 0     | TID tree    | --          | **[TBD]** | **[TBD]** |                 |
| 1     | id          | INT         | **[TBD]** | **[TBD]** |                 |
| 2     | status      | VARCHAR(20) | **[TBD]** | **[TBD]** | 5 distinct vals |
| 3     | region      | VARCHAR(30) | **[TBD]** | **[TBD]** | 4 distinct vals |
| 4     | priority    | VARCHAR(10) | **[TBD]** | **[TBD]** | 4 distinct vals |
| 5     | department  | VARCHAR(40) | **[TBD]** | **[TBD]** | 6 distinct vals |

**FOR-encoded table (`for_orvos`, 500K rows):**

| attno | Column           | Type     | Pages      | Ratio      | Notes             |
|-------|------------------|----------|-----------|-----------|-------------------|
| 0     | TID tree         | --       | **[TBD]** | **[TBD]** |                   |
| 1     | id               | INT      | **[TBD]** | **[TBD]** |                   |
| 2     | sensor_reading_a | INT      | **[TBD]** | **[TBD]** | range ~100        |
| 3     | sensor_reading_b | BIGINT   | **[TBD]** | **[TBD]** | range ~1000       |
| 4     | temperature      | SMALLINT | **[TBD]** | **[TBD]** | range ~50         |
| 5     | sequential_ctr   | INT      | **[TBD]** | **[TBD]** | sequential, d=1   |

**NULL-optimized table (`null_orvos`, 500K rows):**

| attno | Column       | NULL % | Pages      | Ratio      | NULL Encoding          |
|-------|-------------|--------|-----------|-----------|------------------------|
| 0     | TID tree    | --     | **[TBD]** | **[TBD]** |                        |
| 1     | id          | 0%     | **[TBD]** | **[TBD]** |                        |
| 2     | always_set  | 0%     | **[TBD]** | **[TBD]** | `OVBT_ATTR_NO_NULLS`  |
| 3     | sparse_null | 5%     | **[TBD]** | **[TBD]** | `OVBT_ATTR_SPARSE_NULLS` |
| 4     | medium_null | 50%    | **[TBD]** | **[TBD]** | Standard bitmap        |
| 5     | dense_null  | 95%    | **[TBD]** | **[TBD]** | Standard bitmap        |
| 6     | rle_null    | 50%    | **[TBD]** | **[TBD]** | `OVBT_ATTR_RLE_NULLS` |

### 5.2  Overall Table Size Reduction

| Table               | HEAP Size  | Orvos Size  | Ratio      | Dominant Encoding     |
|---------------------|-----------|-------------|-----------|------------------------|
| bool_* (8 booleans) | **[TBD]** | **[TBD]**   | **[TBD]** | Bit-packing            |
| dict_* (low-card)   | **[TBD]** | **[TBD]**   | **[TBD]** | Dictionary             |
| for_* (narrow-range)| **[TBD]** | **[TBD]**   | **[TBD]** | FOR                    |
| uuid_* (UUIDs)      | **[TBD]** | **[TBD]**   | **[TBD]** | Fixed-binary           |
| null_* (NULL-heavy)  | **[TBD]** | **[TBD]**   | **[TBD]** | NULL optimization      |
| compress_high_*     | **[TBD]** | **[TBD]**   | **[TBD]** | zstd + FSST            |
| compress_low_*      | **[TBD]** | **[TBD]**   | **[TBD]** | zstd baseline          |

### 5.3  Disk I/O Reduction

| Workload             | HEAP Buffers Read | Orvos Buffers Read | Reduction  |
|----------------------|-------------------|--------------------|-----------|
| Full scan (wide)     | **[TBD]**         | **[TBD]**          | **[TBD]** |
| Column projection    | **[TBD]**         | **[TBD]**          | **[TBD]** |
| Aggregation (1 col)  | **[TBD]**         | **[TBD]**          | **[TBD]** |
| GROUP BY (dict col)  | **[TBD]**         | **[TBD]**          | **[TBD]** |

---

## 6  CPU Overhead

### 6.1  Compression Time vs Space Savings

| Technique        | Compression Throughput | Decompression Throughput | Ratio      | Net Benefit |
|------------------|-----------------------|-------------------------|-----------|-------------|
| zstd alone       | **[TBD]** MB/s        | **[TBD]** MB/s          | **[TBD]** | Baseline    |
| zstd + FSST      | **[TBD]** MB/s        | **[TBD]** MB/s          | **[TBD]** | **[TBD]**   |
| Bit-packing      | **[TBD]** MB/s        | **[TBD]** MB/s          | **[TBD]** | **[TBD]**   |
| Dictionary       | **[TBD]** MB/s        | **[TBD]** MB/s          | **[TBD]** | **[TBD]**   |
| FOR              | **[TBD]** MB/s        | **[TBD]** MB/s          | **[TBD]** | **[TBD]**   |
| Fixed-binary     | N/A                   | N/A                     | **[TBD]** | **[TBD]**   |
| Native varlena   | N/A                   | N/A                     | N/A       | **[TBD]**   |

### 6.2  Decompression Profile

Percentage of scan time spent in decompression (from `perf` profiling):

| Function                   | % of Total | Notes                      |
|----------------------------|-----------|----------------------------|
| `ov_decompress()`          | **[TBD]** | General-purpose (zstd/lz4) |
| `fsst_decompress()`        | **[TBD]** | FSST symbol expansion      |
| `ov_dict_decode()`         | **[TBD]** | Dictionary index lookup    |
| FOR delta unpacking         | **[TBD]** | Bit-unpacking loop         |
| Boolean unpacking           | **[TBD]** | Bit-to-byte expansion      |
| NULL bitmap decode          | **[TBD]** | Sparse/RLE decoding        |
| Varlena reconstruction      | **[TBD]** | Header insertion           |

### 6.3  INSERT Performance Impact

| Operation              | HEAP (rows/s) | Orvos Baseline | Orvos + Encodings | Change     |
|-----------------------|---------------|----------------|-------------------|-----------|
| Bulk INSERT (500K)    | **[TBD]**     | **[TBD]**       | **[TBD]**          | **[TBD]** |
| COPY (500K)           | **[TBD]**     | **[TBD]**       | **[TBD]**          | **[TBD]** |
| Single-row INSERT     | **[TBD]**     | **[TBD]**       | **[TBD]**          | **[TBD]** |

---

## 7  Visualizations

> Charts will be generated from benchmark data.  Placeholder descriptions
> define the intended content and layout.

### 7.1  Compression Ratio by Technique (Bar Chart)

- X-axis: Encoding technique (Bit-pack, Dict, FOR, UUID, NULL, FSST, zstd-only)
- Y-axis: Compression ratio (HEAP size / Orvos size)
- Each bar: ratio for the technique's ideal data type
- Error bars: min/max across table sizes

```
[TBD -- insert chart or ASCII representation here]
```

### 7.2  Query Time vs Table Size (Line Chart)

- X-axis: Row count (1K, 10K, 100K, 1M, 10M)
- Y-axis: Query time (ms, log scale)
- Lines: HEAP, Orvos baseline, Orvos optimized
- Query: column projection (`SELECT 2 of 20 columns`)

```
[TBD -- insert chart or ASCII representation here]
```

### 7.3  Compression Ratio vs Decompression Speed (Scatter Plot)

- X-axis: Decompression throughput (MB/s)
- Y-axis: Compression ratio
- Each point: one technique on one data type
- Ideal: upper-right (high ratio, high throughput)

```
[TBD -- insert chart or ASCII representation here]
```

### 7.4  NULL Encoding Efficiency by Density (Grouped Bar Chart)

- X-axis: NULL density (0%, 5%, 50%, 95%, RLE-pattern)
- Y-axis: Encoding size (bytes per 1000 elements)
- Groups: Standard bitmap, Sparse, RLE, Omitted

```
[TBD -- insert chart or ASCII representation here]
```

### 7.5  Encoding Selection Decision Tree

```
                        Is boolean?
                       /           \
                     Yes             No
                      |               |
                 Bit-pack        Is integer, attbyval?
                                  /              \
                                Yes               No
                                 |                 |
                           Range narrow?       Is UUID?
                            /       \          /      \
                          Yes       No       Yes      No
                           |        |         |        |
                         FOR    Standard   Fixed-   Low cardinality?
                                           Binary    /          \
                                                   Yes          No
                                                    |            |
                                                  Dict     Is string?
                                                            /       \
                                                          Yes       No
                                                           |         |
                                                         FSST    Standard
                                                       + zstd      zstd
```

---

## 8  Implementation Recommendations

Prioritized by ROI: `(compression_gain * applicability) / complexity`.

### 8.1  High ROI (Ship First)

**1. Boolean Bit-Packing** -- 8x theoretical, **[TBD]** x measured.

- Applicability: Any table with boolean columns.
- Complexity: Low -- isolated bit pack/unpack.
- Status: Implemented, tested.
- Recommendation: **Ship as-is.**

**2. NULL Bitmap Omission** -- 100% savings for NOT NULL columns.

- Applicability: Majority of production columns.
- Complexity: Trivial -- single flag check.
- Status: Implemented, tested.
- Recommendation: **Ship as-is.  Zero-cost win for common case.**

**3. Dictionary Encoding** -- **[TBD]** x for low-cardinality.

- Applicability: Status, category, enum-like columns (< 1% distinct).
- Complexity: Medium -- hash-based dictionary build/decode.
- Status: Implemented, tested.
- Recommendation: **Ship as-is.**

**4. Native Varlena Format** -- CPU optimization, not compression.

- Applicability: All short varlena columns.
- Complexity: Low -- format flag and pointer passthrough.
- Status: Implemented, tested.
- Recommendation: **Ship as-is.**

### 8.2  Medium ROI

**5. Frame of Reference (FOR)** -- **[TBD]** x for narrow-range integers.

- Applicability: Sensor data, timestamps, sequential IDs.
- Complexity: Medium -- bit-packing arithmetic.
- Recommendation: **Ship as-is.** Monitor for edge cases.

**6. UUID Fixed-Binary** -- **[TBD]** x.

- Applicability: Tables with UUID columns.
- Complexity: Low -- type check and header stripping.
- Recommendation: **Ship as-is.**

**7. FSST String Compression** -- **[TBD]** % additional over zstd.

- Applicability: String-heavy tables.
- Complexity: Medium -- symbol table construction, two-stage pipeline.
- Recommendation: **Ship as-is.** No external dependencies.

### 8.3  Low ROI / Candidates for Simplification

**8. Sparse NULL Encoding** -- moderate for narrow use case.

- Consider simplifying to just "bitmap omitted" + "standard bitmap"
  if benchmarks show sparse encoding is rarely selected.

**9. RLE NULL Encoding** -- effective only for consecutive NULL blocks.

- Monitor and potentially remove if not cost-effective.

---

## 9  Future Enhancements

### 9.1  Adaptive Compression

Auto-select encoding per column segment based on runtime statistics.
Track effectiveness over time; switch strategies on data drift.
Reference: DuckDB auto-detection per column segment.

### 9.2  Column Reordering

Optimize physical layout for access frequency, compression affinity,
or NULL density grouping.

### 9.3  Zone Maps

Per-page min/max statistics for data skipping.  Compatible with FOR
encoding (min already stored in header).  Reference: Parquet row group
statistics, DuckDB zone maps.

### 9.4  Late Materialization

Defer tuple reconstruction until after filtering.  Apply predicates
directly on compressed columnar data.
Reference: C-Store/Vertica.

### 9.5  Vectorized Execution

Process columns in batches.  Exploit SIMD (AVX2/AVX-512) for batch
operations.  Reference: DuckDB, MonetDB/X100.

### 9.6  SIMD-Optimized Decompression

AVX2/AVX-512 for bit unpacking (FOR deltas, boolean arrays), dictionary
index gather, FSST symbol expansion.

### 9.7  Batch Attribute Item Access

Amortize B-tree traversal across multiple rows:

```c
void ovbt_attr_get_batch(Relation rel, int attno, OVTid *tids,
                         int ntids, Datum *values, bool *nulls);
```

### 9.8  Parallel B-tree Build

One worker per column B-tree during COPY / CREATE TABLE AS.

---

## Appendix A: Encoding Flag Reference

All flags defined in `src/include/access/orvos_internal.h`, stored in
`t_flags` of attribute B-tree leaf items.

| Flag                              | Value    | Description                              |
|-----------------------------------|----------|------------------------------------------|
| `OVBT_ROOT`                       | `0x0001` | Page is root of its B-tree               |
| `OVBT_HAS_NULLS`                 | `0x0002` | Item contains NULLs (bitmap present)     |
| `OVBT_ATTR_FORMAT_NATIVE_VARLENA` | `0x0004` | Short varlena in native PG format        |
| `OVBT_ATTR_FORMAT_FOR`           | `0x0008` | Frame of Reference encoding              |
| `OVBT_ATTR_BITPACKED`            | `0x0010` | Boolean bit-packing (8 per byte)         |
| `OVBT_ATTR_NO_NULLS`             | `0x0020` | No NULLs; bitmap omitted                 |
| `OVBT_ATTR_SPARSE_NULLS`         | `0x0040` | Sparse NULL encoding (offset,count)      |
| `OVBT_ATTR_RLE_NULLS`            | `0x0080` | RLE for sequential NULL runs             |
| `OVBT_ATTR_FORMAT_DICT`          | `0x0100` | Dictionary encoding                      |
| `OVBT_ATTR_FORMAT_FIXED_BIN`     | `0x0200` | Fixed-binary (UUID as 16 bytes)          |
| `OVBT_ATTR_FORMAT_FSST`          | `0x0400` | FSST string compression                  |

## Appendix B: Data Structure Reference

### OVDictHeader (8 bytes)

```c
typedef struct OVDictHeader {
    uint16  num_entries;
    uint16  entry_size;        /* 0 = variable-length entries */
    uint32  total_data_size;
} OVDictHeader;
```

### OVForHeader (10 bytes)

```c
typedef struct OVForHeader {
    uint64  for_frame_min;       /* minimum value in the frame */
    uint8   for_bits_per_value;  /* bits per delta (0..64) */
    uint8   for_attlen;          /* original attribute length (1,2,4,8) */
} OVForHeader;
```

### OVSparseNullEntry (4 bytes)

```c
typedef struct OVSparseNullEntry {
    uint16  sn_position;  /* element index where NULL(s) start */
    uint16  sn_count;     /* number of consecutive NULLs */
} OVSparseNullEntry;
```

### OVRleNullEntry (2 bytes)

```c
typedef struct OVRleNullEntry {
    uint16  rle_count;  /* high bit = is_null, low 15 bits = run length */
} OVRleNullEntry;

#define OVBT_RLE_NULL_FLAG   0x8000
#define OVBT_RLE_COUNT_MASK  0x7FFF
```

### FsstSymbolTable (~2.3 KB)

```c
typedef struct FsstSymbolTable {
    uint32      magic;            /* 0x46535354 ('FSST') */
    uint16      num_symbols;      /* max 255 */
    uint16      padding;
    FsstSymbol  symbols[256];     /* 256 * 9 = 2304 bytes */
} FsstSymbolTable;

typedef struct FsstSymbol {
    uint8   len;                  /* 1-8, 0 = unused */
    uint8   bytes[8];             /* the symbol bytes */
} FsstSymbol;
```

## Appendix C: SQL Queries for Manual Verification

### Compression statistics from pg_statistic

```sql
SELECT
    a.attname,
    s.stanumbers1[1] AS compression_ratio,
    s.stanumbers1[2] AS null_fraction,
    s.stanumbers1[3] AS avg_width_compressed,
    s.stanumbers1[4] AS avg_width_uncompressed
FROM pg_statistic s
JOIN pg_attribute a
  ON s.starelid = a.attrelid AND s.staattnum = a.attnum
WHERE s.stakind1 = 10001
  AND a.attrelid = 'tablename'::regclass
ORDER BY a.attnum;
```

### Per-column page counts and ratios

```sql
SELECT
    attno, count(*) AS pages,
    sum(totalsz) AS total_bytes,
    sum(uncompressedsz) AS uncompressed_bytes,
    ROUND(sum(uncompressedsz::numeric) / NULLIF(sum(totalsz), 0), 2) AS ratio
FROM pg_ov_btree_pages('tablename')
GROUP BY attno ORDER BY attno;
```

### Table size comparison

```sql
SELECT relname,
       pg_size_pretty(pg_relation_size(oid)) AS table_size,
       pg_size_pretty(pg_total_relation_size(oid)) AS total_size,
       pg_relation_size(oid) AS raw_bytes
FROM pg_class
WHERE relname IN ('table_heap', 'table_orvos')
ORDER BY relname;
```

## Appendix D: References

**Research:**
- Boncz et al., "FSST: Fast Random Access String Compression" (VLDB 2020)
- Lemire & Boytsov, "Decoding billions of integers per second through
  vectorization" (SPE 2015)

**Implementations:**
- DuckDB storage internals
- Apache Parquet encoding specification
- Apache Arrow columnar format specification

**PostgreSQL:**
- Table Access Method API (`src/include/access/tableam.h`)
- `pg_statistic` catalog (`src/include/catalog/pg_statistic.h`)

---

**Report generated by**: doc-writer agent
**Source commit**: [TBD -- `git rev-parse HEAD`]
**Benchmark suite version**: 1.0
