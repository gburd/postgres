/*-------------------------------------------------------------------------
 *
 * quel_rangetab.c
 *	  Session-scoped tuple-variable table for the QUEL extension.
 *
 * Berkeley QUEL declares tuple variables via `RANGE OF e IS emp`.
 * The binding (e -> emp) lives until the session ends.  Subsequent
 * statements that reference `e.column` consult this table to resolve
 * the backing relation.
 *
 * Implemented as a small open-addressed hash table indexed by
 * lowercased tuple-variable name.  Sized for the typical ~10
 * concurrent tuple variables per QUEL session; rehashes if a
 * session declares more than 32 tuple variables.
 *
 * The table is in TopMemoryContext so it survives across
 * statement boundaries within a session.  Reset is explicit
 * (called by xact_handler on transaction abort if we want
 * stricter scoping) but Berkeley QUEL semantics keep the bindings
 * across rollback, so reset is currently only on backend start.
 *
 * Portions Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * contrib/quel/quel_rangetab.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "common/hashfn.h"
#include "utils/memutils.h"

#include "quel_grammar.h"

#define QUEL_RANGETAB_INITIAL_SIZE 32

typedef struct QuelRangeSlot
{
	bool		used;
	char	   *name;			/* lowercased; ownership: TopMemoryContext */
	char	   *relation;
	int			lineno;
} QuelRangeSlot;

static QuelRangeSlot *g_slots = NULL;
static int	g_capacity = 0;
static int	g_count = 0;

static int
slot_for(const char *name, int *out_first_free)
{
	uint32		h = string_hash(name, strlen(name));
	int			i;
	int			first_free = -1;

	for (i = 0; i < g_capacity; i++)
	{
		int			idx = (h + i) % g_capacity;
		QuelRangeSlot *s = &g_slots[idx];

		if (!s->used)
		{
			if (first_free < 0)
				first_free = idx;
			break;
		}
		if (strcmp(s->name, name) == 0)
		{
			if (out_first_free)
				*out_first_free = idx;
			return idx;
		}
	}

	if (out_first_free)
		*out_first_free = first_free;
	return -1;
}

void
quel_rangetab_init(void)
{
	MemoryContext old;

	if (g_slots != NULL)
		return;

	old = MemoryContextSwitchTo(TopMemoryContext);
	g_slots = palloc0(sizeof(QuelRangeSlot) * QUEL_RANGETAB_INITIAL_SIZE);
	g_capacity = QUEL_RANGETAB_INITIAL_SIZE;
	g_count = 0;
	MemoryContextSwitchTo(old);
}

void
quel_rangetab_reset(void)
{
	int			i;

	if (g_slots == NULL)
	{
		quel_rangetab_init();
		return;
	}

	for (i = 0; i < g_capacity; i++)
	{
		if (g_slots[i].used)
		{
			pfree(g_slots[i].name);
			pfree(g_slots[i].relation);
			g_slots[i].used = false;
		}
	}
	g_count = 0;
}

static void
rehash_if_needed(void)
{
	int			old_cap;
	QuelRangeSlot *old_slots;
	MemoryContext old;
	int			i;

	if (g_count + 1 < g_capacity * 3 / 4)
		return;

	old_cap = g_capacity;
	old_slots = g_slots;

	old = MemoryContextSwitchTo(TopMemoryContext);
	g_capacity *= 2;
	g_slots = palloc0(sizeof(QuelRangeSlot) * g_capacity);
	MemoryContextSwitchTo(old);

	g_count = 0;
	for (i = 0; i < old_cap; i++)
	{
		if (old_slots[i].used)
		{
			quel_rangetab_set(old_slots[i].name, old_slots[i].relation);
			pfree(old_slots[i].name);
			pfree(old_slots[i].relation);
		}
	}
	pfree(old_slots);
}

void
quel_rangetab_set(const char *name, const char *relation)
{
	int			first_free = -1;
	int			existing;
	MemoryContext old;

	if (g_slots == NULL)
		quel_rangetab_init();

	rehash_if_needed();

	existing = slot_for(name, &first_free);
	old = MemoryContextSwitchTo(TopMemoryContext);

	if (existing >= 0)
	{
		/* Re-binding an existing tuple variable. */
		pfree(g_slots[existing].relation);
		g_slots[existing].relation = pstrdup(relation);
	}
	else
	{
		Assert(first_free >= 0);
		g_slots[first_free].used = true;
		g_slots[first_free].name = pstrdup(name);
		g_slots[first_free].relation = pstrdup(relation);
		g_slots[first_free].lineno = 0;
		g_count++;
	}

	MemoryContextSwitchTo(old);
}

const char *
quel_rangetab_lookup(const char *name)
{
	int			idx;

	if (g_slots == NULL || g_count == 0)
		return NULL;

	idx = slot_for(name, NULL);
	if (idx < 0)
		return NULL;
	return g_slots[idx].relation;
}

bool
quel_rangetab_iterate(int *cursor, QuelRangeEntry *out)
{
	if (g_slots == NULL)
		return false;

	while (*cursor < g_capacity)
	{
		QuelRangeSlot *s = &g_slots[*cursor];

		(*cursor)++;
		if (s->used)
		{
			out->name = s->name;
			out->relation = s->relation;
			out->lineno = s->lineno;
			return true;
		}
	}
	return false;
}

int
quel_rangetab_count(void)
{
	return g_count;
}
