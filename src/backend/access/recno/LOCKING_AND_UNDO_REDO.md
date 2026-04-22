# RECNO Locking and UNDO/REDO Implementation

## Overview
This document describes the locking mechanisms and UNDO/REDO functionality implemented for the RECNO storage manager to ensure data consistency under concurrent access and proper crash recovery.

## Locking Mechanisms

### 1. Tuple-Level Locking (`recno_lock.c`)
- **RecnoLockTuple()**: Acquires exclusive or shared locks on individual tuples
- **RecnoUnlockTuple()**: Releases tuple locks
- **RecnoLockMultipleTuples()**: Acquires multiple tuple locks in sorted order to prevent deadlocks
- **RecnoCheckDeadlock()**: Detects potential deadlock situations

### 2. Page-Level Locking
- **RecnoLockPage()**: Acquires page-level locks for bulk operations
- **RecnoUnlockPage()**: Releases page locks
- Used in conjunction with buffer locks for complete protection

### 3. Lock Integration in Operations
- **Insert Operations**: Use buffer locks only (new tuples don't need tuple locks)
- **Update Operations**: Acquire tuple lock before buffer lock to prevent concurrent modifications
- **Delete Operations**: Acquire tuple lock before buffer lock to ensure atomicity

## UNDO/REDO Functionality

### 1. WAL Record Structure
Each WAL record contains both old and new tuple data:
- **Insert**: Only new tuple data (UNDO = mark as deleted)
- **Update**: Both old and new tuple data (UNDO = restore old, REDO = apply new)
- **Delete**: Old tuple data (UNDO = restore tuple, REDO = mark deleted)

### 2. UNDO Logic
- **Insert UNDO**: Mark inserted tuple as deleted with tombstone
- **Update UNDO**: Restore original tuple data from WAL record
- **Delete UNDO**: Restore deleted tuple by removing deletion flag

### 3. REDO Logic
- **Insert REDO**: Add tuple to page at specified offset
- **Update REDO**: Replace tuple data with new version
- **Delete REDO**: Mark tuple as deleted with tombstone

### 4. Isolation Level Support

#### READ COMMITTED
- Uses commit timestamps for visibility
- Allows non-repeatable reads
- UNDO/REDO works with basic timestamp checking

#### REPEATABLE READ
- Uses transaction start timestamp for consistent reads
- Prevents non-repeatable reads within transaction
- UNDO/REDO preserves snapshot consistency

#### SERIALIZABLE
- Includes anti-dependency tracking
- Detects serialization conflicts
- UNDO/REDO maintains serializability guarantees

## Testing Framework

### 1. Automated Tests (`recno_undo_test.c`)
- **RecnoTestInsertUndoRedo()**: Tests insert operations with rollback
- **RecnoTestUpdateUndoRedo()**: Tests update operations with rollback
- **RecnoTestConcurrentAccess()**: Tests concurrent access scenarios
- **RecnoTestIsolationLevels()**: Verifies behavior across isolation levels

### 2. Test Cases
- Insert with commit/rollback under different isolation levels
- Update with commit/rollback under different isolation levels
- Concurrent access conflict detection
- Serializable isolation conflict detection

### 3. SQL Test Script (`test_recno_locks.sql`)
- Basic CRUD operations
- Transaction isolation level testing
- Rollback scenario verification
- Concurrent access simulation

## Key Features

### 1. Deadlock Prevention
- Consistent lock ordering (sorted by TID)
- Timeout-based deadlock detection
- Proper lock release on transaction abort

### 2. Crash Recovery
- Complete before/after images in WAL
- Proper LSN checking for UNDO vs REDO
- Transaction timestamp verification

### 3. Concurrency Control
- Tuple-level locking for fine-grained concurrency
- Buffer locks for page-level consistency
- MVCC with timestamp-based visibility

### 4. Performance Optimizations
- In-place updates when possible
- Minimal lock duration
- Efficient lock acquisition ordering

## Usage

### 1. Building
The locking and UNDO/REDO functionality is automatically included when building PostgreSQL with RECNO support:
```bash
make -C src/backend/access/recno
```

### 2. Testing
Run the test suite to verify functionality:
```sql
-- Create test table
CREATE TABLE test_table (id INTEGER) USING recno;

-- Run built-in tests (would be called internally)
SELECT recno_test_undo_redo('test_table');
```

### 3. Monitoring
Check for proper locking behavior in PostgreSQL logs:
- Look for "RECNO insert/update/delete" messages
- Monitor for deadlock detection messages
- Verify UNDO/REDO test results

## Future Enhancements

1. **Lock Escalation**: Automatic promotion from tuple to page locks
2. **Lock Timeout Configuration**: Configurable deadlock timeout values
3. **Enhanced Conflict Detection**: More sophisticated serializable conflict detection
4. **Performance Metrics**: Lock contention and UNDO/REDO performance statistics
5. **Parallel Recovery**: Multi-threaded UNDO/REDO processing

## Conclusion

The RECNO locking and UNDO/REDO implementation provides:
- **Correctness**: Proper isolation and consistency guarantees
- **Performance**: Fine-grained locking with deadlock prevention
- **Reliability**: Complete crash recovery with before/after images
- **Testability**: Comprehensive test suite for verification

This implementation ensures that RECNO can handle concurrent access safely while maintaining ACID properties across all supported isolation levels.