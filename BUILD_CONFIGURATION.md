# Orvos Build Configuration

Orvos is an **optional** build component in PostgreSQL, with options to enable/disable it and even set it as the default table access method.

## Build Options

### Option 1: Orvos Table AM (--with-orvos / -Dorvos)

**Controls**: Whether to compile Orvos columnar storage support

**Default**: **ENABLED** (Orvos is built by default)

**Autoconf**:
```bash
./configure --with-orvos      # Enable (default)
./configure --without-orvos   # Disable
```

**Meson**:
```bash
meson setup build -Dorvos=enabled   # Enable (default)
meson setup build -Dorvos=disabled  # Disable
meson setup build -Dorvos=auto      # Auto-detect (enabled if possible)
```

**Effect**:
- **When enabled**: `src/backend/access/orvos/` is compiled and linked
- **When disabled**: Orvos code is not compiled, binary is ~200KB smaller
- Catalog entries remain in `pg_am` but handler function is unavailable

**Use Case for Disabling**:
- Minimal binary size requirements
- Security-focused builds (reduce attack surface)
- Embedded systems with limited resources
- You know you'll never use columnar storage

### Option 2: Default Table AM (--with-default-table-am / -Ddefault_table_am)

**Controls**: Which table access method to use by default for new tables

**Default**: **heap** (PostgreSQL's traditional row storage)

**Autoconf**:
```bash
./configure --with-default-table-am=heap   # Default
./configure --with-default-table-am=orvos  # Use Orvos by default
```

**Meson**:
```bash
meson setup build -Ddefault_table_am=heap   # Default
meson setup build -Ddefault_table_am=orvos  # Use Orvos by default
```

**Effect**:
- Sets the `DEFAULT_TABLE_ACCESS_METHOD` configuration variable
- New tables created without `USING` clause use this access method
- Can be overridden at runtime with `default_table_access_method` GUC
- Can be overridden per-table with `CREATE TABLE ... USING <am>`

**Validation**:
- If you set `--with-default-table-am=orvos`, Orvos must be enabled
- Configure will error if you try to use disabled Orvos as default

## Quick Start

### Build with Orvos (Default)

```bash
# Autoconf
./configure --with-lz4  # Orvos enabled by default
make -j$(nproc)

# Meson
meson setup build -Dlz4=enabled  # Orvos enabled by default
ninja -C build
```

### Build without Orvos

```bash
# Autoconf
./configure --without-orvos
make -j$(nproc)

# Meson
meson setup build -Dorvos=disabled
ninja -C build
```

### Build with Orvos as Default Table AM

```bash
# Autoconf
./configure --with-lz4 --with-default-table-am=orvos
make -j$(nproc)

# Meson
meson setup build -Dlz4=enabled -Ddefault_table_am=orvos
ninja -C build
```

## Usage Examples

### Example 1: Default Build (Orvos Enabled, Heap Default)

```bash
./configure --with-lz4
make -j$(nproc)
```

Result:
- Orvos is available but not default
- Tables use heap unless you specify `USING orvos`

```sql
-- Uses heap (default)
CREATE TABLE data1 (id INT, value TEXT);

-- Uses orvos (explicit)
CREATE TABLE data2 (id INT, value TEXT) USING orvos;

-- Change default for this session
SET default_table_access_method = 'orvos';
CREATE TABLE data3 (id INT, value TEXT);  -- Now uses orvos
```

### Example 2: Orvos as Default

```bash
./configure --with-lz4 --with-default-table-am=orvos
make -j$(nproc)
```

Result:
- Orvos is the default for new tables
- Can still create heap tables explicitly

```sql
-- Uses orvos (default)
CREATE TABLE analytics_data (id INT, metrics JSONB);

-- Uses heap (explicit)
CREATE TABLE transactional_data (id INT, value TEXT) USING heap;

-- Change default back to heap for this session
SET default_table_access_method = 'heap';
```

### Example 3: Minimal Build (No Orvos)

```bash
./configure --without-orvos
make -j$(nproc)
```

Result:
- Orvos not available
- Binary is ~200KB smaller
- Attempting to use Orvos gives clear error

```sql
CREATE TABLE data (id INT) USING orvos;
-- ERROR:  function orvos_tableam_handler() does not exist
```

## Runtime Configuration

Even after building, you can control the default table AM at runtime:

### postgresql.conf

```ini
# Use orvos by default for all new tables
default_table_access_method = 'orvos'
```

### Per-Session

```sql
-- Change for current session
SET default_table_access_method = 'orvos';

-- Change for current transaction
SET LOCAL default_table_access_method = 'orvos';
```

### Per-Database

```sql
-- Set default for a specific database
ALTER DATABASE analytics SET default_table_access_method = 'orvos';
ALTER DATABASE transactional SET default_table_access_method = 'heap';
```

### Per-Table (Explicit)

```sql
-- Always explicit, ignores defaults
CREATE TABLE my_data (...) USING orvos;
CREATE TABLE my_data (...) USING heap;
```

## Build-Time vs Runtime

| Setting | Build-Time | Runtime |
|---------|------------|---------|
| **Orvos Available** | `--with-orvos` / `-Dorvos` | Cannot change (requires rebuild) |
| **Default AM** | `--with-default-table-am` | `default_table_access_method` GUC |
| **Per-Table AM** | N/A | `CREATE TABLE ... USING <am>` |

## Recommendations

### For Production Use

```bash
# Enable Orvos, keep heap as default
./configure --with-lz4 --with-orvos
```

**Rationale**:
- Maximum flexibility: both heap and orvos available
- Conservative default: existing applications work unchanged
- Opt-in to columnar: explicitly choose orvos where beneficial

### For Data Warehouse / Analytics

```bash
# Enable Orvos, make it default
./configure --with-lz4 --with-default-table-am=orvos
```

**Rationale**:
- Most tables will benefit from columnar storage
- Reduce need to remember `USING orvos` clause
- Still can use heap for specific OLTP tables

### For Embedded / Minimal Builds

```bash
# Disable Orvos entirely
./configure --without-orvos --without-lz4 --without-zstd
```

**Rationale**:
- Smallest possible binary
- Reduced memory footprint
- Simpler codebase (fewer features)

## Migration Guide

### Existing Databases

When you rebuild PostgreSQL with different options:

1. **Orvos enabled → disabled**: Existing Orvos tables become inaccessible
   - Back up data first: `pg_dump --inserts your_db > backup.sql`
   - Rebuild with Orvos enabled, or migrate tables to heap first

2. **Default AM changed**: Only affects NEW tables
   - Existing tables keep their original access method
   - No need to migrate

3. **Re-enabling Orvos**: Existing Orvos tables work immediately
   - No migration needed

### Checking Current Configuration

```sql
-- Check if Orvos is available
SELECT amname FROM pg_am WHERE amname = 'orvos';
-- Returns 1 row if enabled, 0 rows if disabled

-- Check current default AM
SHOW default_table_access_method;
-- Returns 'heap' or 'orvos'

-- Check a specific table's AM
SELECT relname, amname
FROM pg_class c
JOIN pg_am a ON c.relam = a.oid
WHERE relname = 'my_table';
```

## Troubleshooting

### Error: "function orvos_tableam_handler() does not exist"

**Cause**: Orvos was disabled at build time (`--without-orvos`)

**Solution**: Rebuild with `--with-orvos` or use heap tables

### Error: "Cannot set default table AM to 'orvos' when Orvos is disabled"

**Cause**: Tried `--with-default-table-am=orvos` without `--with-orvos`

**Solution**: Add `--with-orvos` to your configure command

### Tables won't create with Orvos

**Check**:
1. Is Orvos enabled? `SELECT * FROM pg_am WHERE amname = 'orvos';`
2. Syntax correct? `CREATE TABLE ... USING orvos;` (not `WITH orvos`)
3. Permissions? Need `CREATE` privilege

## Performance Considerations

### Build with LZ4 (Recommended)

```bash
./configure --with-lz4 --with-orvos
```

- Orvos with LZ4: 5-10x compression ratio
- Orvos without LZ4: 2-5x compression ratio (falls back to pglz)
- Both work fine, but LZ4 is significantly better

### Binary Size Impact

| Configuration | Binary Size | Notes |
|---------------|-------------|-------|
| No Orvos | Baseline | ~200KB smaller |
| With Orvos | Baseline + 200KB | ~15,000 lines of code |
| Default AM setting | No impact | Just a configuration value |

### Runtime Overhead

- **When Orvos enabled but not used**: Near-zero overhead
- **When Orvos is default**: Same performance as explicit `USING orvos`
- **Switching default AM**: No performance impact, just changes default behavior

## See Also

- [README.md](README.md) - Complete Orvos documentation
- [PROJECT_STATUS.md](PROJECT_STATUS.md) - Current project status
- [benchmarks/README.md](benchmarks/README.md) - Benchmark suite documentation

---

**Last Updated**: 2026-03-07
**PostgreSQL Version**: 19 (development)
**Orvos Status**: Optional, enabled by default
