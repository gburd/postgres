/*-------------------------------------------------------------------------
 *
 * prop_index_am.c
 *	  Trait-based property tests across the core index access methods.
 *
 * One property per shipped access method, each supplying an am_driver to the
 * generic trait harness (index_traits.c).  The harness queries the AM's traits
 * at runtime (pg_indexam_has_property) and runs only the trait properties the
 * AM claims, so this file does not encode which AM has which capability -- it
 * asks the server.  That is also what lets an out-of-tree AM (BARK, an
 * extension) reuse the entire applicable suite: supply a driver, declare traits
 * in the amhandler, done.
 *
 * The core AMs that can index a scalar int column:
 *	 btree   -- ordered, exact, unique, multicol, include, search_nulls
 *	 hash    -- exact only
 *	 brin    -- summarizing (lossy block ranges)
 *	 gin     -- exact over int via the btree_gin opclass extension
 *	 gist    -- ordered/exact over int via the btree_gist opclass extension
 *
 * spgist is intentionally not here: it ships no integer opclass, so it cannot
 * index the generated int column without a custom opclass, and the generic
 * trait queries are over that int column.  spgist (over the scalar types it
 * does support) is a candidate for its own AM-specific area.
 *
 * AM-specific behaviour that is not a shared trait (nbtree skip scan, etc.)
 * lives in its own area (prop_nbtree.c), not here.
 *
 * Tier B: runs SQL against the cluster the TAP driver started.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/sql/prop_index_am.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "index_am"
#include "pg_prop.h"

#include "index_traits.h"

PG_PROP(btree_traits)
{
	am_driver	d = {"btree", NULL, "k"};

	index_traits_check(tc, &d);
}

PG_PROP(hash_traits)
{
	am_driver	d = {"hash", NULL, "k"};

	index_traits_check(tc, &d);
}

PG_PROP(brin_traits)
{
	am_driver	d = {"brin", NULL, "k"};

	index_traits_check(tc, &d);
}

PG_PROP(gin_traits)
{
	am_driver	d = {"gin",
		"CREATE EXTENSION IF NOT EXISTS btree_gin", "k"};

	index_traits_check(tc, &d);
}

PG_PROP(gist_traits)
{
	am_driver	d = {"gist",
		"CREATE EXTENSION IF NOT EXISTS btree_gist", "k"};

	index_traits_check(tc, &d);
}

PG_PROP(bloom_traits)
{
	/*
	 * bloom (contrib) is a signature index: bitmap-only, lossy (every match is
	 * rechecked), no ordering and no unique.  It indexes the int column k via
	 * its default opclass.  The trait harness will find index_scan=false,
	 * bitmap_scan=true and no can_order, so it exercises the exact (via the
	 * recheck/bitmap path) and summarizing-shaped checks -- the lossy-but-
	 * never-drops-a-row contract.  Skipped cleanly if contrib/bloom is not
	 * installed.
	 */
	am_driver	d = {"bloom",
		"CREATE EXTENSION IF NOT EXISTS bloom", "k"};

	index_traits_check(tc, &d);
}
