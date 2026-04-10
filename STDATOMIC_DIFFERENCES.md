# Semantic Differences: Current(v18) vs stdatomic.h Implementation

This document catalogs all semantic differences between PostgreSQL's current
(v18) atomics implementation and the C11 stdatomic.h-based implementation, as
identified by comprehensive audit of Thomas Munro's v3 patches and the v4
dual-implementation approach.

---

## 1. MEMORY ORDERING DIFFERENCES

### 1.1 Basic Read/Write Operations

| Operation | Postgres 18 | V3 Patches | V4 Patches |
|-----------|-------------|------------|------------|
| `pg_atomic_read_u32/64()` | Plain load (no ordering) | `memory_order_relaxed` | `memory_order_relaxed` |
| `pg_atomic_write_u32/64()` | Plain store (no ordering) | `memory_order_relaxed` | `memory_order_relaxed` |
| `pg_atomic_unlocked_read_u32/64()` | Plain load | `memory_order_relaxed` | `memory_order_relaxed` |
| `pg_atomic_unlocked_write_u32/64()` | Plain store | `memory_order_relaxed` | `memory_order_relaxed` |

**Impact:** V4 now matches both Postgres 18 and v3 for basic read/write.
- Postgres 18: No ordering guarantees (compiler can reorder/optimize)
- V3: Atomic but can be reordered by CPU (`relaxed`)
- V4: `relaxed` ordering, matching traditional volatile semantics
- The `_membarrier` variants use `seq_cst` for callers that need ordering.

**Correctness:** `relaxed` is correct because traditional `pg_atomic_read_u32_impl`
(generic-gcc.h) is a plain volatile read with no fence. The `_membarrier` variants
already use `seq_cst` for callers that need ordering guarantees.

**Performance:** On x86 (TSO), behavior is identical since relaxed ≈ seq_cst for
loads/stores. On ARM/POWER this avoids unnecessary DMB/sync instructions,
matching the performance profile of the traditional implementation.

**Decision:** Not configurable. Memory ordering matches traditional semantics.

---

### 1.2 Barrier Operations

| Operation | Postgres 18 (x86) | Postgres 18 (others) | V3 Patches | V4 Patches |
|-----------|-------------------|----------------------|------------|------------|
| `pg_memory_barrier()` | `lock; addl` inline asm | `__atomic_thread_fence(seq_cst)` | `atomic_thread_fence(seq_cst)` | `atomic_thread_fence(seq_cst)` |
| `pg_read_barrier()` | compiler barrier only | `__atomic_thread_fence(acquire)` | `atomic_thread_fence(acquire)` | `atomic_thread_fence(acquire)` |
| `pg_write_barrier()` | compiler barrier only | `__atomic_thread_fence(release)` | `atomic_thread_fence(release)` | `atomic_thread_fence(release)` |

**Impact:**
- **x86**: Postgres 18 uses hand-coded `lock; addl`, stdatomic uses `mfence` or equivalent
- **Other platforms**: No difference in semantics, compiler generates appropriate instructions

**Performance Impact:** Unknown, likely minimal. x86 `lock` prefix is very fast.

**Decision:** Not configurable. Platform will use whichever implementation is selected
by USE_STDATOMIC_H flag.

---

## 2. SPINLOCK POLARITY DIFFERENCE

### 2.1 Flag Values

| Implementation | Unlocked | Locked |
|----------------|----------|--------|
| Postgres 18 | 0 | 1 |
| stdatomic.h | 1 | 0 |

**From:** `src/include/port/atomics/stdatomic_impl.h` lines 76-92

**Explanation:**
- **Postgres 18**: Uses exchange-based TAS where 0=unlocked is natural
  ```c
  pg_atomic_init_flag(ptr) { ptr->value = 0; }  // init to unlocked
  pg_atomic_test_set_flag(ptr) { return exchange(ptr, 1) == 0; }  // try to lock
  ```

- **stdatomic.h**: Uses AND-based operation where 1=unlocked is natural
  ```c
  pg_atomic_init_flag(ptr) { atomic_init(ptr, 1); }  // init to unlocked
  pg_atomic_test_set_flag(ptr) { return fetch_and(ptr, 0) == 0; }  // AND clears bit
  ```

**Impact:**
- **Internal implementation detail only** - public API unchanged
- Debugging tools examining raw spinlock values will see inverted values
- **Cannot mix** Postgres 18 and stdatomic spinlocks in same binary (prevented by conditional compilation)

**Decision:** Not configurable. This is an internal implementation choice with no
external visibility. Both implementations provide identical public API behavior.

**Documentation:** Clearly documented in stdatomic_impl.h (lines 76-92).

---

## 3. SPIN DELAY INSTRUCTION DIFFERENCES

### 3.1 x86/x86_64 Spin Delay

| Platform | Instruction | Implementation |
|----------|-------------|----------------|
| x86 | `rep nop` (PAUSE) | Same in both |
| x86_64 | `rep nop` (PAUSE) | Same in both |

**Verdict:** No difference.

---

### 3.2 ARM64 Spin Delay

| Platform | Postgres 18 | stdatomic.h (GCC/Clang) | stdatomic.h (MSVC) |
|----------|-------------|-------------------------|-------------------|
| ARM64 (Unix) | ISB | ISB | N/A |
| ARM64 (Windows) | No-op | N/A | `__isb(_ARM64_BARRIER_SY)` |

**From:** `src/include/port/spin_delay.h` lines 52-89

**Research Basis:**
https://postgr.es/m/1c2a29b8-5b1e-44f7-a871-71ec5fefc120%40app.fastmail.com

ISB (Instruction Synchronization Barrier) performs better than YIELD hint on
high-core-count ARM64 systems under heavy spinlock contention.

**Impact:**
- **Unix ARM64**: No change (both use ISB)
- **Windows ARM64**: stdatomic adds ISB support (performance improvement)

**Performance:** Positive improvement on Windows ARM64.

**Decision:** Not configurable. ISB is superior based on research.

---

### 3.3 ARM32 (ARMv7) Spin Delay

| Platform | Postgres 18 | stdatomic.h |
|----------|-------------|-------------|
| ARM32 | No-op | YIELD instruction |

**From:** `src/include/port/spin_delay.h` line 62

**Impact:** Minor performance improvement on ARM32 with YIELD support.

**Decision:** Not configurable.

---

### 3.4 PowerPC Spin Delay

| Platform | Postgres 18 | stdatomic.h |
|----------|-------------|-------------|
| PowerPC | No-op | No-op |

**Verdict:** No difference.

---

## 4. PLATFORM-SPECIFIC ATOMIC IMPLEMENTATIONS

### 4.1 PowerPC Atomic Operations

**MAJOR DIFFERENCE:**

**Postgres 18 (arch-ppc.h):**
- Hand-crafted inline assembly using `lwarx`/`stwcx.` instructions
- Optimized compare-exchange with immediate operands
- Uses `lwsync` (lightweight sync) instead of full `sync` for barriers
- Example (TAS operation):
  ```c
  __asm__ __volatile__(
  "	lwarx   %0,0,%3,1	\n"  // Load-and-reserve with hint
  "	cmpwi   %0,0		\n"  // Compare with 0
  "	bne     fail		\n"  // Branch if not equal
  "	addi    %0,%0,1		\n"  // Increment
  "	stwcx.  %0,0,%3		\n"  // Store conditional
  "	beq     ok		\n"
  "	...
  ```

**stdatomic.h:**
- Relies on compiler's stdatomic implementation
- Compiler generates `lwarx`/`stwcx.` loops automatically
- May or may not optimize as well as hand-crafted assembly

**Impact:** **UNKNOWN PERFORMANCE IMPACT** - Requires benchmarking on real PowerPC hardware.

**Correctness:** Should be equivalent if compiler implements stdatomic correctly.

**Decision:** Handled by primary USE_STDATOMIC_H flag. Keep Postgres 18 implementation
available as fallback if stdatomic performs poorly on PowerPC.

**Testing Required:** Before production deployment on PowerPC:
1. Run performance benchmarks comparing Postgres 18 vs stdatomic
2. Run stress tests under high concurrency
3. Verify compiler-generated assembly is optimal

---

### 4.2 x86 Atomic Operations

**Difference:**

**Postgres 18 (arch-x86.h):**
- Memory barriers use hand-coded inline assembly
- `pg_memory_barrier()`: `lock; addl $0,0(%%rsp)` (atomic add to stack)
- `pg_read/write_barrier()`: compiler barrier only (x86 has strong memory model)

**stdatomic.h:**
- Memory barriers use `atomic_thread_fence()`
- Compiler generates `mfence` or equivalent for full barriers
- Read/write barriers still use `atomic_thread_fence()` with acquire/release

**Impact:**
- May generate different instructions
- `lock; addl` vs `mfence` - both provide full barrier, performance likely similar

**Performance:** Likely minimal difference. Both are fast on x86.

**Decision:** Not configurable. If performance issues arise, can be addressed by
allowing arch-x86.h to define `pg_memory_barrier_impl()` override even with
USE_STDATOMIC_H.

---

### 4.3 ARM32 64-bit Atomics

**Postgres 18:**
- Disables 64-bit atomics on 32-bit ARM (no `PG_HAVE_ATOMIC_U64_SUPPORT`)
- Falls back to spinlock-based emulation (fallback.h)

**stdatomic.h:**
- Allows 64-bit atomics on ARM32
- Compiler/runtime provides emulation using locks (ARMv7)
- Per v3 patch comments: "For one known low-end system they are emulated by the
  compiler/runtime with locks (armv7)."

**Impact:**
- stdatomic allows 64-bit atomics on ARM32 where Postgres 18 forbids them
- Both implementations use locks under the hood, just different implementations

**Performance:** Likely equivalent (both use locks).

**Correctness:** Equivalent.

**Decision:** Not configurable. stdatomic approach is simpler (compiler handles it).

---

## 5. TYPE REPRESENTATION DIFFERENCES

### 5.1 Atomic Flag Type

| Implementation | Type | Size (typical) | Alignment |
|----------------|------|----------------|-----------|
| Postgres 18 | `typedef struct { volatile int value; } pg_atomic_flag;` | 4 bytes | 4 bytes |
| stdatomic.h | `typedef _Atomic(uint8) pg_atomic_flag;` | 1 byte | 1 byte |

**Impact:** **BINARY INCOMPATIBLE** between implementations.

**Consequences:**
- Shared memory structures containing atomics cannot be shared between Postgres 18
  and stdatomic builds
- Database files/shared memory must be rebuilt when switching implementations
- This is acceptable because implementations are selected at build time

**Decision:** Not configurable. This is fundamental to the implementation approach.

---

### 5.2 Atomic Integer Types

| Implementation | Type (uint32 example) |
|----------------|----------------------|
| Postgres 18 | `typedef struct { volatile uint32 value; } pg_atomic_uint32;` |
| stdatomic.h | `typedef _Atomic(uint32) pg_atomic_uint32;` |

**Size Impact:** Size is typically the same (4 bytes for uint32, 8 for uint64).

**Binary Compatibility:** **BROKEN** - Cannot mix implementations in same binary or
shared memory.

---

## 6. RELAXED SPINLOCK TEST OPTIMIZATION

### 6.1 Test-Before-TAS Pattern

**Postgres 18 (x86, PowerPC):**
```c
#define TAS_SPIN(lock)  (*(lock) ? 1 : TAS(lock))
```

**stdatomic.h (v3 patches):**
```c
#ifdef PG_SPIN_TRY_RELAXED
	try_to_set = pg_atomic_unlocked_test_flag(lock);  // Relaxed load
#else
	try_to_set = true;
#endif
if (try_to_set && pg_atomic_test_set_flag(lock))
	break;
```

**From:** v3-0003 patch, lines 63-77

**Platform Support:** Defined for x86, x86_64, PowerPC (platforms with strong memory models).

**Impact:**
- Both implementations use this optimization on x86/PowerPC
- stdatomic makes it explicit via `PG_SPIN_TRY_RELAXED` flag
- **No semantic change** from Postgres 18 behavior

**Performance:** Reduces bus traffic on contended locks (good).

**Decision:** Not configurable. This is a well-established optimization.

---

## 7. SPINLOCK DUAL-PATH IMPLEMENTATION

### 7.1 Spinlock Implementation Strategy

When `USE_STDATOMIC_H` is defined, spinlocks are implemented on top of
the `pg_atomic_flag` API rather than platform-specific TAS assembly:

| Component | Traditional Path | stdatomic Path |
|-----------|-----------------|----------------|
| `slock_t` type | `unsigned char` (platform-specific) | `pg_atomic_flag` (`_Atomic(uint8)`) |
| `SpinLockInit` | `S_INIT_LOCK()` | `pg_atomic_init_flag()` |
| `SpinLockAcquire` | `S_LOCK()` (TAS + s_lock) | `pg_atomic_test_set_flag()` + `s_lock()` |
| `SpinLockRelease` | `S_UNLOCK()` | `pg_atomic_clear_flag()` |
| Spin delay | `SPIN_DELAY()` | `pg_spin_delay()` |

**Key design decisions:**

1. **`SpinLockAcquire` is a macro** (not an inline function) so that
   `__FILE__`/`__LINE__`/`__func__` resolve at the call site for
   "stuck spinlock" diagnostics.

2. **`s_lock()` uses the relaxed-load optimization** on x86/PowerPC
   via `PG_SPIN_TRY_RELAXED`: a relaxed load tests the flag before
   attempting the atomic exchange, reducing cache-coherency traffic
   under contention.

3. **`SpinDelayStatus` is duplicated** in `spin.h` (for the stdatomic path)
   and `s_lock.h` (for the traditional path) to avoid circular header
   dependencies.

4. **`S_LOCK_TEST` standalone test** is only built for the traditional path,
   since the stdatomic path uses the same `s_lock()` slow path but through
   the atomics API.

---

## 8. DELETED FUNCTIONALITY

### 8.1 Architecture-Specific Assembly (v3 patches only)

**Files deleted by v3 patches:**
- `src/include/port/atomics/arch-arm.h` (32 lines)
- `src/include/port/atomics/arch-ppc.h` (256 lines)
- `src/include/port/atomics/arch-x86.h` (atomics deleted, barriers kept)
- `src/include/port/atomics/fallback.h` (42 lines)
- `src/include/port/atomics/generic.h` (436 lines)

**V4 dual implementation:** **KEEPS ALL THESE FILES** for non-stdatomic path.

**Impact:** When USE_STDATOMIC_H=0 (Postgres 18), all hand-tuned assembly is still
available. When USE_STDATOMIC_H=1, compiler-generated stdatomic code is used.

**Decision:** Correct approach for dual implementation. Provides fallback if
stdatomic performs poorly on any platform.

---

### 8.2 Test-and-Set Assembly Functions

**Postgres 18:** Some platforms build platform-specific `tas.s` files with hand-written
atomic test-and-set.

**V3 patches:** Delete `src/backend/port/tas/dummy.s` and all tas generation.

**V4 patches:** Keeps tas files for Postgres 18 path.

**Decision:** Correct for dual implementation.

---

## 9. C++ COMPATIBILITY

### 9.1 Header Inclusion

| Implementation | C Compatibility | C++ Compatibility |
|----------------|-----------------|-------------------|
| Postgres 18 | ✓ Yes | ✗ No (uses C-specific constructs) |
| stdatomic.h (ours) | ✓ Yes (`<stdatomic.h>`) | ✓ Yes (`<atomic>` for C++11-C++20) |

**From:** `src/include/port/atomics/stdatomic_impl.h` lines 33-66

**Impact:** C++ extensions can now use atomics when built with `-Duse_stdatomic=yes`.

**Decision:** Not configurable. C++ support is enabled automatically with stdatomic.

---

## 10. FRONTEND CODE SUPPORT

### 10.1 Atomics in Frontend Programs

| Implementation | Frontend Support |
|----------------|------------------|
| Postgres 18 | ✗ No (`#error` directive) |
| stdatomic.h (ours) | ✓ Yes (when USE_STDATOMIC_H defined) |

**From:** `src/include/port/atomics.h` lines 56-70

**Rationale:** Postgres 18 atomics relied on backend-specific features. stdatomic.h
is a standard header with no backend dependencies.

**Decision:** Not configurable. Frontend support automatically enabled with stdatomic.

---

## 11. BUILD FLAG RECOMMENDATIONS

### 11.1 Primary Flag: USE_STDATOMIC_H

**Purpose:** Select stdatomic.h vs Postgres 18 implementation.

**Values:**
- Defined (1): Use stdatomic.h implementation
- Undefined (0): Use Postgres 18 platform-specific implementation

**Set by:** Meson option `-Duse_stdatomic=yes|no|auto`

**Default:** `auto` (use stdatomic if available, Postgres 18 otherwise)

**This is the ONLY flag needed.**

---

### 11.2 Flags NOT Recommended

Based on comprehensive audit, these flags are **NOT NEEDED:**

1. **Memory ordering flag** (`-Duse_relaxed_atomics=yes`): NO
   - Reason: Memory ordering is API contract, not build option
   - seq_cst is safest; can change universally if needed

2. **Spinlock polarity flag** (`-Dinvert_spinlock_polarity=yes`): NO
   - Reason: Internal implementation detail, no external impact

3. **ARM64 ISB vs YIELD flag** (`-Duse_yield_instead_of_isb=yes`): NO
   - Reason: ISB is superior based on research

4. **PowerPC assembly flag** (`-Duse_ppc_assembly=yes`): NO
   - Reason: Handled by primary USE_STDATOMIC_H flag
   - If PowerPC performance issues arise, use `-Duse_stdatomic=no`

5. **Barrier implementation flag** (`-Duse_x86_assembly_barriers=yes`): NO
   - Reason: Compiler-generated barriers are correct and fast

---

## 12. TESTING REQUIREMENTS

### 12.1 Functional Testing

**Required before production:**

1. **Atomic Operation Correctness:**
   - Multi-threaded stress tests for all atomic operations
   - Verify no data races under TSAN (Thread Sanitizer)
   - Test all atomic types (uint8, uint16, uint32, uint64, flag)

2. **Memory Ordering:**
   - Verify relaxed operations don't introduce races
   - Test barrier operations under concurrent load
   - Litmus tests for acquire/release semantics

3. **Spinlock Correctness:**
   - High-contention spinlock tests
   - Fairness tests (no starvation)
   - Nested locking tests

4. **C++ Compatibility:**
   - Compile test with C++11, C++14, C++17, C++20
   - Verify C++ extensions using atomics work correctly

5. **Frontend Atomics:**
   - Test frontend programs using atomic operations
   - Verify no backend dependencies creep in

---

### 12.2 Performance Testing

**Required on ALL supported platforms:**

1. **PowerPC:** Compare Postgres 18 vs stdatomic performance
   - Atomic operation throughput
   - Spinlock contention overhead
   - Memory barrier costs

2. **ARM64:** Verify ISB spin delay improves performance

3. **x86:** Compare lock instruction vs mfence barrier overhead

4. **Regression tests:** Ensure no performance regressions vs Postgres 18

---

### 12.3 Platform Coverage

**Must test on:**
- ✓ x86_64 Linux
- ✓ x86_64 Windows
- ✓ ARM64 Linux
- ✓ ARM64 Windows
- ✓ ARM32 (ARMv7)
- ⚠️ PowerPC (critical - hand-optimized assembly replaced)
- ✓ RISC-V (if available)

---

## 13. DOCUMENTATION REQUIREMENTS

### 13.1 Code Comments

**Completed:**
- ✓ Flag polarity difference documented in stdatomic_impl.h
- ✓ Spin delay choices documented in spin_delay.h
- ✓ Memory ordering documented in atomics.h
- ✓ C++ compatibility documented in stdatomic_impl.h

### 13.2 User Documentation

**Required:**
- Document `-Duse_stdatomic` build option
- Explain when to use Postgres 18 vs stdatomic
- Note binary incompatibility between implementations
- PowerPC performance caveat until benchmarked

---

## 14. SUMMARY: NO ADDITIONAL BUILD FLAGS NEEDED

**Conclusion:** The semantic differences between Postgres 18 and stdatomic.h
implementations do NOT require additional build flags beyond the primary
`USE_STDATOMIC_H` flag (controlled by `-Duse_stdatomic` meson option).

**Reasoning:**

1. **Memory ordering:** Conservative approach (seq_cst) is safer than both
   Postgres 18 and v3. Not configurable by design.

2. **Spinlock polarity:** Internal implementation detail, no external impact.

3. **Platform-specific optimizations:** Handled by primary flag. If stdatomic
   performs poorly on a platform, use Postgres 18 implementation.

4. **Instruction choices (ISB vs YIELD):** Based on research, ISB is superior.
   No reason to make configurable.

5. **Type representations:** Fundamental to implementation, not configurable.

**The dual-implementation approach (keeping both Postgres 18 and stdatomic)
already provides the necessary flexibility for platform-specific needs.**

---

**END OF SEMANTIC DIFFERENCES DOCUMENT**
