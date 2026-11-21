/*-------------------------------------------------------------------------
 *
 * plpython.h - Python as a procedural language for PostgreSQL
 *
 * Note: this file is #include'd by each of the sub-module header files
 * (plpy_elog.h, etc).  It's therefore unnecessary for any plpython *.c
 * files to include it directly.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/pl/plpython/plpython.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PLPYTHON_H
#define PLPYTHON_H

/* postgres.h needs to be included before Python.h, as usual */
#if !defined(POSTGRES_H)
#error postgres.h must be included before plpython.h
#elif defined(Py_PYTHON_H)
#error Python.h must be included via plpython.h
#endif

/*
 * Enable Python Limited API
 */
#define Py_LIMITED_API 0x03020000

/*
 * Pull in Python headers via a wrapper header, to control the scope of
 * the system_header pragma therein.
 */
#include "plpython_system.h"

/*
 * Assert that the calling thread holds the GIL.  PyGILState_Check() is not
 * part of the limited API this module builds against, so the check exists
 * only where the full API is visible.
 */
#if !defined(Py_LIMITED_API)
#define PLy_AssertHoldsGIL() Assert(PyGILState_Check())
#else
#define PLy_AssertHoldsGIL() ((void) 0)
#endif

/* define our text domain for translations */
#undef TEXTDOMAIN
#define TEXTDOMAIN PG_TEXTDOMAIN("plpython")

#endif							/* PLPYTHON_H */
