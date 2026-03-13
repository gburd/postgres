# Orvos vs. AlloyDB: Technical Architecture Comparison

## Executive Summary

Orvos and AlloyDB are both PostgreSQL-compatible systems designed to balance OLTP and OLAP workloads, but they take different architectural approaches. Orvos is an experimental PostgreSQL table access method (TAM) focused on columnar storage with MVCC via UNDO logs, while AlloyDB is Google's managed cloud database service that combines disaggregated compute/storage layers with a built-in analytical engine.

---

## Storage Architecture

### Orvos Storage Model
- **Columnar storage with selective projection**: Stores data in columns, enabling column projection to read only required attributes during scans
- **Multiple B-trees per table**:
  - One TID (tuple identifier) B-tree for visibility/presence information
  - One B-tree per column for attribute data
  - This design reduces I/O for analytical queries that access specific columns
- **Per-page organization**: Data organized into pages (standard PostgreSQL 8KB pages)
- **Compression support**: Supports zstd (preferred), LZ4, and pglz compression codecs for column data
- **MVCC via UNDO log**: Uses UNDO records for multi-version concurrency control (similar to zheap), not traditional heap snapshots

### AlloyDB Storage Model (from public documentation)
- **Disaggregated architecture**: Compute and storage layers are independently scalable
- **Integrated analytics engine**: Built-in columnar analytical engine (separate from primary row storage)
- **PostgreSQL-compatible**: Primary transactional storage remains PostgreSQL-compatible
- **Automatic adaptability**: Google markets this as "adaptable storage" that can switch between row and column formats
- **Cloud-native design**: Designed specifically for cloud deployment with distributed storage backend
- **Compression**: Supports compression but specific algorithms not publicly documented

### Key Difference
Orvos stores **all** data primarily in columnar format with multiple B-trees, while AlloyDB appears to maintain **both** row-format storage (for OLTP) and columnar storage (for analytics) as separate paths, switching between them based on query patterns.

---

## MVCC Implementation

### Orvos MVCC
- **UNDO log-based**: Maintains UNDO records for historical versions instead of multiple row versions in storage
- **TID tree for visibility**: Uses a dedicated B-tree to track which TIDs are visible to which transactions
- **Bottom-up deletion**: Implements bottom-up deletion strategy via index_delete_tuples
- **Efficient snapshots**: UNDO log allows efficient visibility checks without duplicating row data
- **Similarity to zheap**: Conceptually similar to PostgreSQL's experimental zheap table access method

### AlloyDB MVCC
- **Not publicly documented in detail**: Google has not released extensive technical documentation about AlloyDB's MVCC implementation
- **Likely PostgreSQL-compatible**: Probably maintains standard PostgreSQL MVCC semantics for compatibility
- **Possible optimizations**: May use separate MVCC mechanisms for columnar vs. row storage paths

---

## Compression Techniques

### Orvos Compression
- **Algorithm priority**: zstd > LZ4 > pglz
- **Implementation strategy**:
  - Zstd provides best compression ratio and speed for columnar data
  - LZ4 offers very fast compression with good ratio
  - pglz serves as fallback when neither zstd nor LZ4 available
- **Column-aware**: Compression applied per-column, taking advantage of column homogeneity
- **Optional compression**: Compression only kept if resulting size is smaller than original

### AlloyDB Compression
- **Details not publicly disclosed**: Google has not released specific compression algorithm details
- **Implied capabilities**: Product marketing suggests compression for both OLTP and OLAP workloads
- **Cloud integration**: Likely leverages Google Cloud infrastructure for compression/decompression

---

## OLTP vs. OLAP Support

### Orvos Approach
- **Column projection for analytics**: SELECT queries reading specific columns skip unneeded data
- **Multiple B-tree indexes**: Per-column B-trees enable efficient range scans on individual columns
- **MVCC efficiency**: UNDO log-based MVCC reduces storage overhead compared to tuple-versioning
- **Trade-offs**:
  - OLAP queries benefit from columnar storage and projection
  - OLTP updates may be costlier due to multiple B-tree maintenance
  - No adaptive switching between formats

### AlloyDB Approach
- **Disaggregated design**: Compute can scale independently of storage
- **Adaptive analytics engine**: Quoted as "hybrid/adaptable storage" that can optimize for workload patterns
- **Separate analytical path**: Columnstore appears to be a distinct processing path, not necessarily the primary storage
- **Read pool scaling**: Supports separate read-only instances for analytical workloads
- **AI integration**: AlloyDB AI provides vector search and ML capabilities
- **Geographic distribution**: Cross-region replication for disaster recovery

---

## Practical Workload Characteristics

### Orvos Strengths
- **Analytical queries**: Column projection significantly reduces I/O for queries selecting few columns
- **Data compression**: Columnar storage enables high compression ratios for similar-typed data
- **Storage efficiency**: Multiple B-trees eliminate storing unnecessary columns
- **Single storage format**: Simpler data model (everything is columnar)

### Orvos Weaknesses
- **OLTP write overhead**: Maintaining multiple B-trees per column increases write complexity
- **Point lookups**: May be less efficient than row storage for single-tuple retrieval
- **Research stage**: Orvos is an experimental PostgreSQL TAM, not production-ready
- **No distributed storage**: Single-node PostgreSQL-based implementation

### AlloyDB Strengths
- **Managed service**: No infrastructure management required
- **Automatic scaling**: Disaggregated architecture enables independent compute/storage scaling
- **High availability**: Built-in automatic failover and cross-region replication
- **Hybrid workloads**: Apparently handles both OLTP and OLAP without manual tuning
- **Integration**: Seamless integration with Google Cloud ecosystem and AI services
- **Production-ready**: Google-maintained, enterprise-grade service

### AlloyDB Weaknesses
- **Vendor lock-in**: Proprietary Google Cloud service
- **Cost**: Managed service pricing vs. open-source self-hosted option
- **Architectural details opaque**: Limited public documentation of internal architecture
- **PostgreSQL compatibility limits**: Likely some incompatibilities despite "PostgreSQL-compatible" claims

---

## Architecture Comparison Table

| Aspect | Orvos | AlloyDB |
|--------|-------|---------|
| **Storage Model** | Columnar (all tables) | Hybrid row/column |
| **Data Organization** | Multiple B-trees per column | Disaggregated compute/storage |
| **MVCC Strategy** | UNDO log-based | PostgreSQL-compatible (assumed) |
| **Compression** | zstd/LZ4/pglz | Proprietary (undocumented) |
| **Scaling** | Single-node PostgreSQL | Unlimited horizontal scaling |
| **Adaptability** | Static columnar | Dynamic row/column switching |
| **Status** | Research/experimental | Production service |
| **Deployment** | Self-hosted PostgreSQL | Google Cloud managed service |
| **Documentation** | Source code available | Limited technical detail public |
| **Cost Model** | Open source (free) | Pay-per-use cloud service |

---

## Design Philosophy Comparison

### Orvos Philosophy
- **Columnar-first design**: Assumes columnar storage is optimal for both OLTP and OLAP
- **Simplicity over flexibility**: Single storage model avoids complexity of switching formats
- **Transparency**: Full source code visibility, academic research approach
- **Optimization focus**: Column projection and compression for analytical performance

### AlloyDB Philosophy
- **Pragmatic hybrid approach**: Maintains separate paths for OLTP (row) and OLAP (column)
- **Managed complexity**: Automatic switching and optimization hidden from users
- **Scalability-first**: Cloud-native design prioritizes unlimited scaling
- **Feature richness**: Integration with modern database features (vectors, ML)

---

## Known Limitations and Unknowns

### Orvos (Well-understood from code)
- **Known limitation**: Primarily researched as a PostgreSQL TAM, not yet production-deployed at scale
- **Known capability**: Supports all PostgreSQL table AM operations (scans, inserts, updates, deletes, index support)

### AlloyDB (Limited public information)
- **Unknown**: Exact MVCC implementation details for columnar path
- **Unknown**: How row-to-column or column-to-row switching decisions are made
- **Unknown**: Performance characteristics of analytical queries vs. pure columnar systems
- **Partially documented**: Claims of "adaptable storage" but mechanism undocumented
- **Speculation**: May use Apache Arrow or similar column representation internally

---

## Conclusion

Both Orvos and AlloyDB address the challenge of supporting mixed OLTP/OLAP workloads:

- **Orvos** commits to columnar storage as a universal solution, implementing it as a PostgreSQL table access method with UNDO-log MVCC and aggressive compression.

- **AlloyDB** takes a pragmatic cloud-native approach with a disaggregated architecture and (apparently) adaptive row/column formats, trading transparency for manageability and scale.

The fundamental trade-off: Orvos prioritizes **architectural simplicity and transparency** (single columnar format everywhere), while AlloyDB prioritizes **operational simplicity and scale** (automatic adaptive formats in a managed cloud service).

For analytical workloads on homogeneous column data, Orvos's columnar storage with compression likely offers excellent performance. For production mixed workloads requiring high availability, automatic scaling, and minimal operational overhead, AlloyDB's managed service approach is more practical, though with less visibility into internal design decisions.

---

## Research Limitations

This comparison is based on:
- Orvos: Direct source code analysis from the PostgreSQL repository
- AlloyDB: Publicly available Google Cloud documentation and marketing materials

AlloyDB's architecture is not extensively documented in academic papers or technical blogs. Much of the information comes from Google Cloud's product marketing ("hybrid/adaptable storage," "disaggregated design") rather than detailed technical specifications. Full architectural details would require access to Google Cloud internal documentation or reverse engineering.
