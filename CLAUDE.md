# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a full PostgreSQL 19devel fork (not an extension) implementing a cluster-wide UNDO subsystem and transactional file operations. The `undo` branch contains all custom work; `master` is an auto-synced pristine mirror of upstream `postgres/postgres`.

## Build Commands

### Autoconf (traditional)
```bash
./configure --prefix=/usr/local/pgsql
make -j$(nproc)
make install
```

### Meson (modern, faster)
```bash
meson setup builddir -Dprefix=/usr/local/pgsql
ninja -C builddir
ninja -C builddir install
```

## Running Tests

### Regression tests (UNDO-specific)
UNDO requires `enable_undo = on` at server startup (PGC_POSTMASTER GUC). Use the provided config:
```bash
make installcheck EXTRA_REGRESS_OPTS="--temp-config=src/test/regress/undo_regress.conf" REGRESS="undo undo_physical fileops"
```

### Full regression suite
```bash
# Autoconf
make check-world EXTRA_REGRESS_OPTS="--temp-config=src/test/regress/undo_regress.conf"

# Meson
meson test -C builddir --suite setup && meson test -C builddir
```

### Single regression test
```bash
make -C src/test/regress installcheck REGRESS="undo"
```

### Recovery tests (Perl TAP)
```bash
make -C src/test/recovery check
# Or a specific test:
cd src/test/recovery && prove t/055_undo_clr.pl
```

### Isolation tests
```bash
make -C src/test/isolation check
```

## Architecture: UNDO Subsystem

### 1. Cluster-wide UNDO (`src/backend/access/undo/`)

Global UNDO logs stored in `base/undo/` (16MB segments). Provides synchronous rollback and UNDO-based MVCC for the standard heap AM.

- **Opt-in per table**: `CREATE TABLE t (...) WITH (enable_undo = on)`
- **Rollback**: Synchronous via `UndoReplay()` during transaction abort
- **Record types**: UNDO_INSERT, UNDO_DELETE, UNDO_UPDATE, UNDO_PRUNE, UNDO_INPLACE
- **UndoRecPtr**: 64-bit (bits 63-40: log number, bits 39-0: byte offset)
- **CLR support**: Compensation Log Records prevent double-application during crash recovery

Key files:
- `undolog.c` - Log file management and space allocation
- `undorecord.c` - Record format and serialization (48-byte header + payload)
- `undoapply.c` - Physical UNDO application during rollback
- `undormgr.c` - UNDO resource manager dispatch (per-AM callbacks)
- `undobuffer.c` - AM-agnostic Tier 2 write buffer (UndoBufferBegin/End/Flush)
- `xactundo.c` - Per-transaction UNDO management (3 persistence levels)
- `undo_bufmgr.c` - Buffer management via shared_buffers
- `undoworker.c` - Background cleanup worker
- `undo_xlog.c` - WAL redo routines

### 2. Transactional FILEOPS (`src/backend/storage/file/fileops.c`)

Crash-safe file operations (CREATE, DELETE, RENAME, WRITE, TRUNCATE, MKDIR, RMDIR, CHMOD, CHOWN, SYMLINK, LINK, SETXATTR, REMOVEXATTR). WAL-logged with redo/undo support.

- **GUC**: `enable_transactional_fileops` (default: on)
- **WAL descriptors**: `src/backend/access/rmgrdesc/fileopsdesc.c`
- **Header**: `src/include/access/fileops_xlog.h`

## Key GUC Parameters

| Parameter | Level | Default | Purpose |
|-----------|-------|---------|---------|
| `enable_undo` | table storage param | off | Enable cluster-wide UNDO per table |
| `enable_transactional_fileops` | postmaster | on | Enable crash-safe file operations |
| `undo_retention_time` | server | - | UNDO record retention period |
| `undo_log_segment_size` | server | - | UNDO log segment size |
| `undo_worker_naptime` | server | - | Background worker sleep interval |

## Test Infrastructure

- **Regression tests**: `src/test/regress/sql/{undo,undo_physical,fileops}.sql`
- **Recovery tests**: `src/test/recovery/t/054-057,060-062` (FILEOPS recovery, CLR, crash, standby, WAL compression, WAL retention, 2PC)
- **Config**: `src/test/regress/undo_regress.conf` (sets `enable_undo = on`)
- **Expected output**: `src/test/regress/expected/{undo,undo_physical,fileops}.out`

## Design Documentation

- `src/backend/access/undo/README` - Comprehensive UNDO log system documentation
- `examples/DESIGN_NOTES.md` - Architectural decisions for the UNDO subsystem
- `examples/HEAP_UNDO_DESIGN.md` - Cluster-wide UNDO with heap AM design
- `examples/INDEX_PRUNING_DESIGN.md` - UNDO-informed index pruning

## Code Style

This is PostgreSQL C code. Follow existing PostgreSQL conventions:
- Use `pgindent` for formatting (run `src/tools/pgindent/pgindent` on modified files)
- Memory management via PostgreSQL memory contexts (palloc/pfree), not malloc/free
- Error reporting via `ereport()`/`elog()`
- WAL logging required for all persistent state changes
- All new UNDO/FILEOPS code has been run through pgindent (see commit `5ad571db5be`)
