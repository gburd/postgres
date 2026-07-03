/*
 * sm_compat.h -- small portability shim for compilers that lack GCC/Clang
 * extensions used by sparsemap.  Include before any sparsemap declarations.
 *
 * Handles MSVC (cl.exe):
 *   - __attribute__((...)): MSVC has no such syntax.  Every use in sparsemap is
 *     an optimization or diagnostic hint (aligned, format, always_inline, hot)
 *     that does not change layout given the natural alignment of the members on
 *     the LLP64/x64 target, so neutralizing it to nothing is safe.
 *   - ssize_t: POSIX (<sys/types.h>); MSVC provides SSIZE_T in <BaseTsd.h>.
 */
#ifndef SM_COMPAT_H
#define SM_COMPAT_H

#ifdef _MSC_VER

#ifndef __attribute__
#define __attribute__(x)
#endif

#include <BaseTsd.h>
typedef SSIZE_T ssize_t;

#endif							/* _MSC_VER */

#endif							/* SM_COMPAT_H */
