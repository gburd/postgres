# Shared Dictionary Compression for Orvos/Noxu

## Design Document -- Phase 4 Enhancement

**Author:** compression-research-agent
**Date:** 2026-04-07
**Status:** Design Proposal

---

## Part 1: Research Report

### 1.1 Current Orvos/Noxu Compression Architecture

Orvos stores each column in a separate B-tree. Leaf pages in attribute trees
contain `NXAttributeArrayItem` entries that hold datum data for a contiguous
range of TIDs. These items may be compressed into `NXAttributeCompressedItem`
using a build-time-selected general-purpose compressor (zstd > LZ4 > pglz).

The current compression pipeline applies several column-level pre-encodings
before general-purpose compression:

| Encoding | Flag | Description |
|----------|------|-------------|
| Frame of Reference | `NXBT_ATTR_FORMAT_FOR` | Bit-packed deltas from minimum value |
| Dictionary | `NXBT_ATTR_FORMAT_DICT` | Per-item dictionary for low-cardinality columns |
| FSST | `NXBT_ATTR_FORMAT_FSST` | Symbol table pre-encoding for strings |
| Boolean bit-packing | `NXBT_ATTR_BITPACKED` | 1 bit per boolean |
| Fixed-binary | `NXBT_ATTR_FORMAT_FIXED_BIN` | Raw binary for UUIDs etc. |

**Key limitation identified in the README (line 969-973):**

> Instead of compressing all the tuples on a page in one batch, store a
> small "dictionary", e.g. in page header or meta page or separate
> dedicated page, and use it to compress tuple by tuple. That could make
> random reads and updates of individual tuples faster. Need to find how
> to create the dictionary first.

This is precisely what the shared dictionary approach addresses.

#### Current Per-Item Dictionary Encoding (NXBT_ATTR_FORMAT_DICT)

The existing dictionary encoding (`noxu_dict.c`) operates at the item level:
- Builds a dictionary of distinct values per `NXAttributeArrayItem`
- Uses uint16 indices (max 65,534 entries)
- Only applied when cardinality ratio < 1% (`NX_DICT_CARDINALITY_THRESHOLD`)
- Dictionary is duplicated in every item on every page

**Drawbacks:**
1. Dictionary overhead is repeated per item (typically 50-200+ items per B-tree)
2. Small per-item sample size limits dictionary quality
3. No cross-item or cross-page pattern exploitation
4. Cannot access individual tuples without decompressing the entire item

#### Current FSST String Compression

FSST (`noxu_fsst.c`) builds a 256-entry symbol table of frequently occurring
1-8 byte sequences. The symbol table is currently either:
- Built per-item from the item's own data (`nx_try_compress_auto_fsst`)
- Embedded in each compressed payload for self-contained decompression

**Drawbacks:**
1. Symbol table (~2.3 KB max) is serialized into every compressed item
2. Small per-item samples produce suboptimal symbol tables
3. No sharing of symbol tables across items or pages

#### Current General-Purpose Compression (zstd/LZ4/pglz)

The `nx_try_compress()` / `nx_decompress()` functions use stateless
compression with no external dictionary. Each call compresses/decompresses
independently, meaning the compressor cannot leverage cross-page patterns.

### 1.2 DuckDB Compression Architecture

DuckDB organizes data into **Row Groups** (default 122,880 rows) containing
**Column Segments** -- fixed-size blocks holding data for a single column
within a row group.

#### Compression Selection

DuckDB uses a two-phase approach per column segment:

1. **Analysis Phase:** Full scan of the segment data (not sampling) to
   determine the optimal compression algorithm. The authors chose full-scan
   over sampling because segments are small enough to fit in CPU cache.

2. **Compression Phase:** Apply the selected algorithm and write compressed
   data to fixed-size disk blocks.

#### Supported Algorithms

| Algorithm | Target Data Types | Strategy |
|-----------|-------------------|----------|
| Constant | Any | Stores single value when all values are identical |
| RLE | Any sorted/partitioned | (value, count) pairs |
| Bit Packing | Integers | Removes leading zeros, tracks max per 1024 values |
| Frame of Reference | Integers, timestamps | Delta from minimum + bit packing |
| Dictionary | Strings, low-cardinality | Extract common values, replace with indices |
| FSST | Strings | Symbol table for intra-string patterns |
| ALP | Floats | Adaptive lossless floating-point |
| Chimp/Patas | Floats | XOR-based with leading/trailing zero optimization |
| Zstd | Fallback | General-purpose (added v1.2.0) |

#### Dictionary Scope

DuckDB applies dictionary encoding **per column segment** within a row group.
Each segment independently decides whether dictionary encoding is beneficial.
Dictionaries are NOT shared across segments or row groups. This means:

- Dictionary is rebuilt for each segment
- No global dictionary management or staleness detection
- Simple implementation at the cost of some redundancy

#### Key Insight for Orvos

DuckDB's per-segment approach is analogous to Noxu's per-item approach.
The segment size (~120K rows) provides a larger sample than Noxu's per-item
data, yielding better dictionaries. However, DuckDB does not implement
cross-segment shared dictionaries, which represents an opportunity for Orvos
to exceed DuckDB's compression ratios.

### 1.3 MonetDB BAT Architecture

MonetDB uses a **Binary Association Table (BAT)** model where each column is
stored as a contiguous array of fixed-width values. Key architectural
properties:

#### Column Storage Model

- Each column is stored as a separate BAT (essentially a flat array)
- BATs are memory-mapped files for efficient OS-level caching
- Values are stored in natural binary format (no per-row headers)
- The system relies on OS virtual memory for cache management

#### Compression Approach

MonetDB historically took a different approach from DuckDB:

- **Minimal explicit compression:** MonetDB relies on the OS page cache and
  memory mapping rather than explicit block compression
- **Dictionary encoding** is used selectively for categorical/string columns
- **Order-preserving encoding** maps strings to integers for efficient comparison
- Column values are stored densely (no alignment padding, no row headers)
- The physical density of columnar storage itself provides significant space
  savings over row stores

#### Memory-Mapped File Design

- BATs are mapped directly into the process address space
- The OS handles paging: hot data stays in RAM, cold data lives on disk
- This eliminates the need for a custom buffer cache (unlike PostgreSQL/Noxu)
- Random access to individual values is O(1) since BATs are flat arrays

#### Key Insight for Orvos

MonetDB's flat-array approach enables O(1) tuple access, which is the ideal
Noxu aspires to with shared dictionaries. When individual items can be
decompressed using a shared dictionary, the decompression granularity shrinks
from "entire page" to "single item," approaching MonetDB's random access
performance while maintaining Noxu's MVCC and PostgreSQL integration.

### 1.4 Zstandard Dictionary Compression

Zstandard provides purpose-built dictionary compression APIs that are directly
applicable to Noxu's architecture.

#### Dictionary Training

The `ZDICT_trainFromBuffer()` API builds a dictionary from sample data:

- **Input:** Concatenated sample buffers + per-sample size array
- **Output:** Dictionary buffer (recommended ~100 KB)
- **Sample requirements:** A few thousand samples, total size ~100x dictionary size
- **Algorithms:** COVER (9 bytes/input byte memory) and FastCover (faster, less memory)
- **Optimization:** `ZDICT_optimizeTrainFromBuffer_fastCover()` automatically
  tests parameter combinations

#### Dictionary Compression API

For repeated use of the same dictionary (exactly our use case):

```
ZSTD_createCDict(dict, dictSize, level) -> CDict*     // compile once
ZSTD_compress_usingCDict(cctx, dst, src, cdict) -> size  // use many times
ZSTD_createDDict(dict, dictSize) -> DDict*             // compile once
ZSTD_decompress_usingDDict(dctx, dst, src, ddict) -> size // use many times
```

**Performance characteristics:**
- `ZSTD_createCDict` is expensive (one-time cost)
- `ZSTD_compress_usingCDict` has minimal startup cost (amortized over many uses)
- CDict/DDict can be shared across threads (read-only)
- Dramatic improvement for small data (<64 KB), which matches Noxu item sizes

#### Dictionary Staleness

Zstandard documentation explicitly warns that "dictionaries 'decay' over time,
as your data changes." The recommended approach is to monitor compression
effectiveness and retrain when it drops.

#### Key Insight for Orvos

Zstd dictionary compression is purpose-built for exactly the pattern Noxu
uses: many small, similar data blocks (attribute items) that share common
patterns. A single trained dictionary can be compiled into a CDict and reused
for all items in an attribute B-tree, eliminating per-item compression startup
costs while dramatically improving compression ratios.

### 1.5 FSST Research (Boncz et al., VLDB 2020)

The FSST algorithm, as implemented in Noxu, builds a 256-entry symbol table
of frequently occurring 1-8 byte sequences. The key properties relevant to
shared dictionaries:

- Symbol tables are most effective when trained on representative samples
- Larger training samples produce better symbol tables (more patterns captured)
- The same symbol table can compress many data blocks efficiently
- Symbol tables are compact (~2.3 KB max for 255 symbols * 9 bytes)
- DuckDB uses FSST per-segment, with the table scoped to a column segment

**Key Insight:** Noxu already stores FSST symbol tables in the attribute
metapage (per the `noxu_fsst.h` documentation), but the current
`nx_try_compress_auto_fsst()` function builds and embeds a per-item table.
A shared FSST symbol table trained from a larger column sample would
produce significantly better compression.

---

## Part 2: Design -- Shared Dictionary Compression for Orvos

### 2.1 Architecture Overview

The shared dictionary approach introduces a **per-attribute dictionary** that
is trained from representative column data and stored persistently. This
dictionary is used by the general-purpose compressor (zstd) for all items in
that attribute's B-tree, replacing the current stateless compression.

```
Current Pipeline:
  raw data -> [pre-encoding] -> [stateless zstd] -> compressed item

Proposed Pipeline:
  raw data -> [pre-encoding] -> [zstd with shared dict] -> compressed item
```

The shared dictionary operates at the zstd level, complementing (not
replacing) the existing pre-encoding stages. FSST, FOR, dictionary encoding,
and other pre-filters continue to operate as before. The shared zstd
dictionary provides an additional compression boost by capturing cross-item
byte patterns.

### 2.2 Dictionary Storage Strategy

#### Location: Dedicated Dictionary Pages

Store the shared dictionary on dedicated pages within the Noxu relation file,
referenced from the metapage.

**Metapage extension:**

```c
typedef struct NXRootDirItem
{
    BlockNumber root;
    BlockNumber dict_page;       /* NEW: first page of dictionary chain */
    uint32      dict_generation; /* NEW: dictionary version counter */
} NXRootDirItem;
```

**Dictionary page format:**

```
Page Header (24 B)
+------------------------------------------+
| NXDictPageHeader                         |
|   uint32  dict_generation                |
|   uint32  total_dict_size                |
|   uint16  chunk_offset    (within chain) |
|   uint16  num_pages       (total pages)  |
+------------------------------------------+
| Dictionary data chunk                    |
|   (up to ~8100 bytes per page)           |
+------------------------------------------+
NXDictPageOpaque
+------------------------------------------+
| BlockNumber nx_next                      |
| uint16      nx_page_id = NX_DICT_PAGE_ID|
+------------------------------------------+
```

A 100 KB dictionary requires ~13 pages (8 KB each). These pages form a
linked list, similar to overflow pages.

**Why dedicated pages (not metapage)?**
- Metapage is already a concurrency bottleneck
- Dictionary size (100 KB) far exceeds remaining metapage space
- Dedicated pages can be read in parallel during backend startup
- Dictionary pages are read-only during normal operations (no lock contention)

#### Scope: Per-Attribute

Each attribute B-tree gets its own shared dictionary. This is optimal because:
- Columns have distinct data distributions
- A single dictionary trained on column data captures column-specific patterns
- Different columns may benefit from different dictionary sizes
- Allows independent dictionary rebuilds per column

### 2.3 Dictionary Building

#### When to Build

1. **During initial bulk load (COPY/multi-row INSERT):** After a configurable
   number of items have been inserted (e.g., 10,000 tuples), trigger
   dictionary training from the accumulated data.

2. **During ANALYZE:** The ANALYZE command already scans sample data. Extend
   it to optionally train/refresh compression dictionaries.

3. **On-demand via SQL command:** `ALTER TABLE ... SET (noxu_rebuild_dict = true)`
   or a dedicated utility function `pg_noxu_train_dict(regclass, attnum)`.

4. **Lazy first-access training:** If no dictionary exists when compressing,
   use stateless compression (current behavior). Build dictionary in the
   background when enough data accumulates.

#### How to Sample

The Zstd documentation recommends samples totaling ~100x the target dictionary
size. For a 100 KB dictionary, this means ~10 MB of sample data.

**Sampling strategy:**

```
1. Scan attribute B-tree leaf pages (random or sequential)
2. Decompress items and collect raw datum data
3. Accumulate samples until target sample size reached
4. Call ZDICT_optimizeTrainFromBuffer_fastCover()
5. Store resulting dictionary on dedicated pages
6. Update metapage dict_generation counter
```

**Sample sources (in priority order):**

1. **Buffer cache:** Sample from currently cached pages (zero I/O)
2. **Sequential scan:** Read leaf pages in order (prefetch-friendly)
3. **Random sample:** Read random leaf pages (better distribution)

For the initial proof of concept, sequential scan sampling is simplest and
adequate.

#### Dictionary Size

- **Default:** 100 KB (zstd recommendation)
- **Minimum:** 32 KB (for columns with limited data)
- **Maximum:** 256 KB (diminishing returns beyond this)
- **Configurable via reloption:** `noxu_dict_size = 102400`

### 2.4 Dictionary Lifecycle

#### Generation Counter

Each dictionary has a monotonically increasing generation counter stored in
the metapage. Compressed items record which dictionary generation was used for
compression.

**Item header extension:**

```c
typedef struct NXAttributeCompressedItem
{
    uint16      t_size;
    uint16      t_flags;
    uint16      t_num_elements;
    uint16      t_num_codewords;
    nxtid       t_firsttid;
    nxtid       t_endtid;
    uint16      t_uncompressed_size;
    uint16      t_dict_generation;  /* NEW: 0 = no dict, else gen number */
    char        t_payload[FLEXIBLE_ARRAY_MEMBER];
} NXAttributeCompressedItem;
```

Note: Adding a 2-byte field to the compressed item header is a minimal
overhead increase (2 bytes per item) that enables robust dictionary versioning.

#### Staleness Detection

Track compression effectiveness metrics:

```c
typedef struct NXDictStats
{
    uint32      dict_generation;
    uint64      bytes_compressed;     /* total input bytes */
    uint64      bytes_output;         /* total output bytes */
    uint64      items_compressed;     /* number of items */
    uint64      dict_misses;          /* items where dict didn't help */
    TimestampTz last_trained;         /* when dictionary was built */
} NXDictStats;
```

**Staleness indicators:**
1. Compression ratio drops below threshold (e.g., < 1.5x average)
2. Dict-miss rate exceeds threshold (e.g., > 20% of items not benefiting)
3. Dictionary age exceeds configurable limit
4. Data distribution shift detected (via ANALYZE statistics)

#### When to Rebuild

- **Automatic:** During VACUUM when staleness detected
- **Manual:** Via `pg_noxu_train_dict()` utility function
- **Threshold-based:** When dict_misses / items_compressed > 0.20

#### Migration Strategy

Existing tables with per-item compression continue to work:
- Items with `t_dict_generation = 0` use stateless decompression (current path)
- Items with `t_dict_generation > 0` use the shared dictionary
- Mixed pages (some items with dict, some without) are fully supported
- No table rewrite needed; new items use the dictionary as they are created
- VACUUM can optionally recompress old items with the new dictionary

### 2.5 API Changes

#### Modified Compression Routines

```c
/* New: compress with shared dictionary */
extern int nx_try_compress_with_dict(const char *src, char *dst,
                                      int srcSize, int dstCapacity,
                                      const void *cdict);

/* New: decompress with shared dictionary */
extern void nx_decompress_with_dict(const char *src, char *dst,
                                     int compressedSize,
                                     int uncompressedSize,
                                     const void *ddict);

/* New: train a dictionary from sample data */
extern char *nx_train_dict(const char **samples,
                           const size_t *sampleSizes,
                           int nbSamples,
                           size_t dictCapacity,
                           size_t *dictSize);
```

#### Backend-Local Dictionary Cache

Each backend maintains a cache of compiled dictionaries:

```c
typedef struct NXDictCache
{
    Oid         relid;
    AttrNumber  attno;
    uint32      generation;
    ZSTD_CDict *cdict;      /* compiled compression dictionary */
    ZSTD_DDict *ddict;      /* compiled decompression dictionary */
    char       *raw_dict;    /* raw dictionary bytes */
    size_t      dict_size;
} NXDictCache;
```

Dictionaries are loaded lazily on first access and invalidated when the
generation counter changes (detected via metapage cache invalidation).

**Memory management:** CDict and DDict are allocated in TopMemoryContext
or a dedicated long-lived context. They persist for the backend's lifetime
and are freed on cache invalidation.

#### Fallback Strategy

When the shared dictionary produces worse compression than stateless:

```c
int compressed_with_dict = nx_try_compress_with_dict(src, buf1, ...);
int compressed_without   = nx_try_compress(src, buf2, ...);

if (compressed_with_dict > 0 &&
    (compressed_without <= 0 || compressed_with_dict < compressed_without))
{
    /* Use dictionary-compressed version */
    use buf1, set t_dict_generation = current_gen;
}
else
{
    /* Fall back to stateless compression */
    use buf2 (or buf1), set t_dict_generation = 0;
}
```

This ensures the dictionary never makes compression worse.

### 2.6 Integration with Existing Encodings

The shared dictionary operates at the general-purpose compression layer,
below all pre-encodings:

```
Input: raw datums
  |
  v
[Pre-encoding: DICT/FOR/FSST/bitpack/fixed-bin as applicable]
  |
  v
[General-purpose compression: zstd WITH shared dictionary]
  |
  v
Output: NXAttributeCompressedItem with t_dict_generation
```

The pre-encodings continue to be per-item and self-contained. The shared
dictionary enhances the final compression step.

**FSST interaction:** The per-attribute FSST symbol table (already stored in
the attribute metapage) is conceptually similar to a shared dictionary at
the string encoding level. Both the FSST symbol table and the zstd shared
dictionary can coexist: FSST reduces string entropy, then zstd with a shared
dictionary compresses the FSST-encoded output even further.

### 2.7 Implementation Phases

#### Phase 1: Proof of Concept (Single Attribute, Static Dictionary)

**Scope:** Manual dictionary training for one attribute, basic compression/
decompression with CDict/DDict.

**Tasks:**
1. Add `NX_DICT_PAGE_ID` page type and dictionary page format
2. Implement `nx_train_dict()` using `ZDICT_trainFromBuffer()`
3. Implement `nx_try_compress_with_dict()` / `nx_decompress_with_dict()`
4. Add `dict_page` and `dict_generation` fields to `NXRootDirItem`
5. Add `t_dict_generation` field to `NXAttributeCompressedItem`
6. Implement `pg_noxu_train_dict(regclass, attnum)` SQL function
7. Wire into `nxbt_compress_item()` with fallback
8. Backend-local dictionary cache (single entry per attribute)

**Deliverable:** Working dictionary compression for a single column, manually
trained. Compression ratio measurements vs. baseline.

#### Phase 2: Full Implementation with Rebuild Support

**Scope:** Automatic dictionary building during ANALYZE, staleness detection,
dictionary versioning, mixed-generation pages.

**Tasks:**
1. Integrate dictionary training into ANALYZE path
2. Implement staleness detection (compression ratio monitoring)
3. Support dictionary rebuild with generation counter
4. Mixed-generation decompression (items with different dict generations)
5. VACUUM integration: optional recompression of old items
6. Reloption for dictionary size: `noxu_dict_size`
7. Extend `pg_nx_btree_pages()` to report dictionary usage stats

**Deliverable:** Automatic dictionary lifecycle management. Tables
transparently benefit from shared dictionaries after ANALYZE.

#### Phase 3: Automatic Dictionary Management

**Scope:** Background dictionary training, shared FSST symbol tables,
cross-attribute optimization.

**Tasks:**
1. Background worker for dictionary training (non-blocking)
2. Shared FSST symbol table training (larger samples)
3. Automatic staleness detection and rebuild scheduling
4. Dictionary preloading at backend startup
5. Parallel dictionary page reads
6. Monitoring views: `pg_noxu_dict_stats`

**Deliverable:** Fully automatic dictionary management with zero manual
intervention.

### 2.8 Expected Benefits

#### Compression Ratio Improvements

Based on Zstd documentation and columnar database research:

| Scenario | Current Ratio | Expected with Shared Dict | Improvement |
|----------|--------------|---------------------------|-------------|
| String columns (repetitive) | 3-5x | 8-15x | 2-3x |
| String columns (varied) | 2-3x | 4-8x | 2-3x |
| Integer columns (clustered) | 4-8x (FOR) | 5-10x | 1.2-1.5x |
| Mixed workloads | 2-4x | 5-10x | 2-3x |
| Small items (<1 KB) | 1.2-2x | 3-6x | 2-4x |

The improvement is most dramatic for small items, where stateless compression
has insufficient context. Zstd's documentation shows "dramatic" improvement
for data under a few KB, which is the typical Noxu item size.

#### Performance Trade-offs

**Compression (write path):**
- One-time dictionary training cost (~seconds for large tables)
- CDict compilation cost (~milliseconds, amortized across backend lifetime)
- Per-item compression: comparable or faster (CDict eliminates per-call
  dictionary building)

**Decompression (read path):**
- DDict compilation cost (~milliseconds, amortized)
- Per-item decompression: comparable speed to stateless
- **Key benefit:** Better compression means fewer pages to read, so overall
  query performance improves

**Random tuple access:**
- Currently: must decompress entire page/item to access one tuple
- With shared dict: individual items can be decompressed independently
  (same as today, but better compression per item means smaller items)
- Future: potential for sub-item decompression if items are split smaller

#### Storage Overhead

| Component | Size | Frequency |
|-----------|------|-----------|
| Dictionary pages | 13-32 pages (100-256 KB) | Per attribute |
| Metapage fields | 8 bytes per attribute | Per attribute |
| Item header extension | 2 bytes | Per compressed item |
| Backend cache (CDict+DDict) | ~200-600 KB | Per backend per attribute |

For a table with 10 columns and 1 million rows, the dictionary overhead is
approximately 130-320 pages (1-2.5 MB), while the compression savings on the
attribute B-trees would typically be 10-50% of total table size.

### 2.9 Compatibility and Migration

**Backward compatibility:** Fully maintained.
- Items without dictionaries (`t_dict_generation = 0`) decompress as before
- No catalog changes required (page format changes are internal to Noxu)
- pg_upgrade compatible (dictionary pages are part of the relation file)

**Forward compatibility:**
- New flag bit `NXBT_ATTR_SHARED_DICT` (0x0800) in `t_flags` indicates
  that the item was compressed with a shared dictionary
- Older code that doesn't understand this flag will fail gracefully with
  a decompression error (detectable, not silent corruption)

**WAL compatibility:**
- Dictionary training is logged as a new WAL record type
- Dictionary pages are included in full-page images during checkpoint
- Compressed items with dictionary references replay correctly using the
  dictionary stored in the relation file

---

## Part 3: Feasibility Assessment

### 3.1 Technical Feasibility

**High confidence.** The design builds on well-understood components:

1. **Zstd dictionary APIs** are stable, well-documented, and purpose-built
   for this use case. The CDict/DDict pattern maps cleanly to Noxu's
   per-backend model.

2. **Page format extension** adds only 2 bytes to the compressed item header,
   which is within the existing variable-size item budget.

3. **Dictionary storage** uses the existing page format infrastructure
   (linked pages, opaque areas), requiring no new storage primitives.

4. **Backend caching** follows the established pattern of `rd_amcache` for
   metapage data, with lazy loading and invalidation on smgr events.

### 3.2 Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Dictionary training too slow | Low | Medium | Background worker, incremental training |
| Dictionary too large for memory | Low | Low | Configurable size cap (256 KB default max) |
| Stale dictionary degrades compression | Medium | Low | Automatic fallback to stateless |
| WAL volume increase from dict pages | Low | Low | Dict pages change rarely |
| Concurrency issues during rebuild | Medium | Medium | Copy-on-write: new dict pages, atomic metapage update |

### 3.3 Proof-of-Concept Validation Plan

1. **Micro-benchmark:** Compress/decompress 10,000 Noxu items with and
   without a trained dictionary. Measure compression ratio and throughput.

2. **End-to-end test:** Load TPC-H lineitem into a Noxu table. Train
   dictionaries for string columns (l_shipmode, l_comment). Measure total
   table size and query performance.

3. **Regression test:** Verify all existing compression tests pass with
   dictionary-enhanced compression enabled.

---

## Part 4: Recommended Next Steps

1. **Implement Phase 1 (PoC):** Start with `nx_try_compress_with_dict()`
   and `nx_decompress_with_dict()` wrappers around the zstd CDict/DDict
   APIs. Add the `pg_noxu_train_dict()` SQL function for manual training.
   Target: single attribute, manual workflow.

2. **Benchmark:** Run compression benchmarks on representative workloads
   (TPC-H, real-world string-heavy tables) to validate the expected 2-5x
   improvement.

3. **Iterate on dictionary size:** Test dictionary sizes from 32 KB to
   256 KB to find the optimal default for Noxu's item size distribution.

4. **Integrate with ANALYZE:** Once Phase 1 is validated, wire dictionary
   training into the ANALYZE code path for automatic dictionary lifecycle.

5. **Consider shared FSST tables:** The same per-attribute shared approach
   applies to FSST symbol tables. Training FSST from a larger sample
   (instead of per-item) would improve string compression independently
   of the zstd dictionary enhancement.

6. **Evaluate sub-item compression:** The shared dictionary enables a
   future optimization where individual datums within an item could be
   compressed/decompressed independently, enabling true per-tuple random
   access without decompressing the entire item.

---

## Appendix A: API Reference for Zstd Dictionary Functions

```c
/* Training */
size_t ZDICT_trainFromBuffer(void *dictBuffer, size_t dictBufferCapacity,
                              const void *samplesBuffer,
                              const size_t *samplesSizes,
                              unsigned nbSamples);

/* Compiled dictionary creation (do once) */
ZSTD_CDict* ZSTD_createCDict(const void *dict, size_t dictSize, int level);
ZSTD_DDict* ZSTD_createDDict(const void *dict, size_t dictSize);

/* Compression/decompression with compiled dict (do many times) */
size_t ZSTD_compress_usingCDict(ZSTD_CCtx *cctx,
                                 void *dst, size_t dstCapacity,
                                 const void *src, size_t srcSize,
                                 const ZSTD_CDict *cdict);
size_t ZSTD_decompress_usingDDict(ZSTD_DCtx *dctx,
                                   void *dst, size_t dstCapacity,
                                   const void *src, size_t srcSize,
                                   const ZSTD_DDict *ddict);

/* Cleanup */
size_t ZSTD_freeCDict(ZSTD_CDict *cdict);
size_t ZSTD_freeDDict(ZSTD_DDict *ddict);
```

## Appendix B: Comparison of Compression Approaches

| Feature | Current (per-item) | DuckDB (per-segment) | Proposed (per-attribute) |
|---------|--------------------|---------------------|--------------------------|
| Dictionary scope | Single item (~50-500 tuples) | Column segment (~120K tuples) | Entire column (all tuples) |
| Dictionary quality | Poor (small sample) | Good (medium sample) | Best (large sample) |
| Storage overhead | None (inline) | Minimal (per segment) | 13-32 pages per column |
| Random item access | Decompress item | Decompress segment | Decompress item (better ratio) |
| Dictionary staleness | N/A (rebuilt per item) | N/A (rebuilt per segment) | Monitored, auto-rebuild |
| Implementation complexity | Low | Medium | Medium |
| Compression ratio (strings) | 2-5x | 5-10x | 8-15x (estimated) |

## Appendix C: Relationship to Existing Enhancement Ideas

The README's "Enhancement ideas" section (lines 967-985) identifies several
related ideas:

1. **"Store a small dictionary in page header or meta page"** -- This is
   exactly what the shared dictionary addresses. Our design uses dedicated
   pages rather than page headers due to dictionary size (100 KB vs ~8 KB
   page).

2. **"Compress tuple by tuple for faster random reads"** -- Shared
   dictionaries enable this. With a pre-loaded dictionary, individual items
   (or even individual datums within items) can be compressed/decompressed
   independently.

3. **"Cache uncompressed pages"** -- The shared dictionary approach reduces
   the need for uncompressed page caching by making decompression faster
   (pre-compiled dictionary) and reducing I/O (better compression means fewer
   pages to read).
