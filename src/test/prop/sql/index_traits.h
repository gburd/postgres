/*-------------------------------------------------------------------------
 *
 * index_traits.h
 *	  Trait-based property tests for index access methods.
 *
 * An index access method is described by a set of *traits* -- capabilities it
 * declares through its amhandler and exposes at runtime via the catalog
 * functions pg_indexam_has_property() / pg_index_column_has_property().  A
 * trait is a contract that must hold for ANY access method claiming it:
 *
 *	 exact        -- answers equality (col = const) with the same rows a
 *				     sequential scan returns.  (Every searchable AM.)
 *	 ordered      -- can return rows in index order; a forward scan, a backward
 *				     scan, range quals (<, <=, >, >=, BETWEEN) and ORDER BY
 *				     satisfied by the index all agree with a seqscan + sort.
 *				     (nbtree, and any amcanorder AM such as BARK.)
 *	 summarizing  -- a lossy, block-range structure (BRIN): it may return extra
 *				     candidate rows but must never drop a matching one, so an
 *				     index scan rechecked against the qual equals a seqscan.
 *	 multicol     -- supports multi-column keys; a prefix-equality scan agrees.
 *	 unique       -- can enforce uniqueness; a unique index rejects duplicates
 *				     and permits distinct keys.
 *	 include      -- supports INCLUDE columns / index-only scans; an index-only
 *				     scan returns the same rows and payload as a heap scan.
 *	 search_nulls -- can search for NULL / NOT NULL; IS NULL and IS NOT NULL
 *				     over the index agree with a seqscan.
 *
 * The generic trait tests take an am_driver (the AM's name plus how to build
 * an index of it over the test column, and any extension it needs) and run,
 * for each trait the AM actually claims at runtime, the properties of that
 * trait.  So a new access method -- BARK, or an extension AM -- gets the whole
 * applicable trait suite by supplying one driver and declaring its traits in
 * its amhandler; nothing in the generic tests is AM-specific.
 *
 * AM-specific behaviour (nbtree skip scan, a BRIN-only summary property) lives
 * in its own area, not here.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/sql/index_traits.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef INDEX_TRAITS_H
#define INDEX_TRAITS_H

#include "pg_prop.h"
#include "lib/stringinfo.h"

/*
 * One access method under test.  "amname" is the AM (btree, brin, ...);
 * "setup_sql" is DDL run once per connection to make the AM and its opclasses
 * available (CREATE EXTENSION ...), or NULL; "index_opts" is appended after
 * the column list in CREATE INDEX (e.g. "" for btree, or a storage parameter),
 * normally "".  "colexpr" is the indexed expression over the generated table's
 * column k (usually just "k"); most AMs use "k", but an AM needing an opclass
 * spells it here (e.g. "k int4_ops").
 */
typedef struct am_driver
{
	const char *amname;
	const char *setup_sql;		/* may be NULL */
	const char *colexpr;		/* indexed column/opclass text, e.g. "k" */
} am_driver;

/*
 * Run every trait property the AM claims (queried at runtime) against a fresh
 * generated table.  Called from a PG_PROP body; it connects, builds the table
 * and the index, consults pg_indexam_has_property for each trait, and checks
 * the ones that apply.  Fails the case (does not return) on any disagreement.
 *
 * tc is the test case; d is the AM driver.  The property draws the table and
 * query constants from tc, so each PG_PROP that calls this is a full property.
 */
extern void index_traits_check(pg_prop_tc *tc, const am_driver *d);

/*
 * The individual trait checks, exposed so an AM-specific area can run just one
 * trait (e.g. to exercise it harder) if it wants.  Each takes an open
 * connection with the table "idx_t" already built and the index "idx_t_idx"
 * already created on it, plus the drawn query constants.  They are no-ops if
 * the AM does not claim the trait.
 */
extern bool am_claims(pg_prop_tc *tc, pg_prop_conn *conn, const char *amname,
					  const char *property);

#endif							/* INDEX_TRAITS_H */
