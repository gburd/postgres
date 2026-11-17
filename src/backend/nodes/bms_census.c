/*-------------------------------------------------------------------------
 *
 * bms_census.c
 *	  Measurement-only census of Bitmapset use, by call site.
 *
 * NOT FOR UPSTREAM.  Linked with -Wl,--wrap=bms_<fn> for each function
 * below, so every call from outside bitmapset.c lands in a __wrap_ function
 * that calls the real one and records what it saw.  Calls inside
 * bitmapset.c itself are not wrapped (the linker resolves them locally).
 *
 * Recording is off unless BMS_CENSUS_DIR is set in the environment.  At
 * process exit each process writes <dir>/<pid>.tsv with one row per
 * (call site, function).  For functions that return a set, the row carries
 * the bytes three representations would allocate and free, rounded the way
 * aset.c rounds a chunk:
 *
 *	 bms	the real Bitmapset, measured from its chunks: a fresh result is an
 *			allocation, and a recycled input counts only when repalloc moved
 *			it (the old chunk then counts as freed)
 *	 off	a Bitmapset that also stored the index of its first nonzero word,
 *			so leading zero words cost nothing (16-byte header)
 *	 sbm	an 8-byte node header plus the sbm encoding of the same members
 *			(sbm_get_size), the most favourable sbm layout
 *
 * The two counterfactuals are modelled as recycling an input in place until
 * its rounded size class grows, as Bitmapset does.
 *
 * For read-only functions the row carries the number of input words, the
 * work a word-array implementation does.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

/* Compiled only into the census build (-DBMS_CENSUS plus the --wrap flags). */
#ifdef BMS_CENSUS

#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>

#include "lib/sbm.h"
#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "nodes/pg_list.h"
#include "port/pg_bitutils.h"
#include "storage/ipc.h"
#include "nodes/memnodes.h"
#include "utils/memutils.h"

#define CENSUS_SLOTS 65536		/* power of two */

typedef enum CensusFn
{
	F_COPY, F_SINGLETON, F_UNION, F_INTERSECT, F_DIFFERENCE, F_OFFSET,
	F_ADD_MEMBER, F_DEL_MEMBER, F_ADD_MEMBERS, F_REPLACE_MEMBERS,
	F_ADD_RANGE, F_INT_MEMBERS, F_DEL_MEMBERS, F_JOIN,
	F_EQUAL, F_COMPARE, F_IS_SUBSET, F_SUBSET_COMPARE, F_IS_MEMBER,
	F_MEMBER_INDEX, F_OVERLAP, F_OVERLAP_LIST, F_NONEMPTY_DIFFERENCE,
	F_SINGLETON_MEMBER, F_GET_SINGLETON_MEMBER, F_NUM_MEMBERS,
	F_MEMBERSHIP, F_NEXT_MEMBER, F_PREV_MEMBER, F_HASH_VALUE, F_FREE,
	F_COUNT
} CensusFn;

static const char *const census_fn_name[F_COUNT] = {
	"copy", "make_singleton", "union", "intersect", "difference",
	"offset_members", "add_member", "del_member", "add_members",
	"replace_members", "add_range", "int_members", "del_members", "join",
	"equal", "compare", "is_subset", "subset_compare", "is_member",
	"member_index", "overlap", "overlap_list", "nonempty_difference",
	"singleton_member", "get_singleton_member", "num_members",
	"membership", "next_member", "prev_member", "hash_value", "free",
};

typedef struct CensusSlot
{
	const void *site;			/* return address in the caller, or NULL */
	int			fn;
	int			max_words;
	uint64		calls;
	uint64		members;
	uint64		words;
	uint64		alloc[3];		/* bytes allocated: bms, off, sbm */
	uint64		freed[3];		/* bytes freed: bms, off, sbm */
} CensusSlot;

/* Rounded sizes of one set in the three representations. */
typedef struct CensusSize
{
	uint64		sz[3];
} CensusSize;

static CensusSlot census_slots[CENSUS_SLOTS];
static int	census_pid = 0;		/* process that owns census_slots */
static int	census_state = -1;	/* -1 unknown, 0 off, 1 on */
static const char *census_dir = NULL;
static MemoryContext census_cxt = NULL;

extern Bitmapset *__real_bms_copy(const Bitmapset *a);
extern Bitmapset *__real_bms_make_singleton(int x);
extern Bitmapset *__real_bms_union(const Bitmapset *a, const Bitmapset *b);
extern Bitmapset *__real_bms_intersect(const Bitmapset *a, const Bitmapset *b);
extern Bitmapset *__real_bms_difference(const Bitmapset *a, const Bitmapset *b);
extern Bitmapset *__real_bms_offset_members(const Bitmapset *a, int offset);
extern Bitmapset *__real_bms_add_member(Bitmapset *a, int x);
extern Bitmapset *__real_bms_del_member(Bitmapset *a, int x);
extern Bitmapset *__real_bms_add_members(Bitmapset *a, const Bitmapset *b);
extern Bitmapset *__real_bms_replace_members(Bitmapset *a, const Bitmapset *b);
extern Bitmapset *__real_bms_add_range(Bitmapset *a, int lower, int upper);
extern Bitmapset *__real_bms_int_members(Bitmapset *a, const Bitmapset *b);
extern Bitmapset *__real_bms_del_members(Bitmapset *a, const Bitmapset *b);
extern Bitmapset *__real_bms_join(Bitmapset *a, Bitmapset *b);
extern bool __real_bms_equal(const Bitmapset *a, const Bitmapset *b);
extern int	__real_bms_compare(const Bitmapset *a, const Bitmapset *b);
extern bool __real_bms_is_subset(const Bitmapset *a, const Bitmapset *b);
extern BMS_Comparison __real_bms_subset_compare(const Bitmapset *a,
												const Bitmapset *b);
extern bool __real_bms_is_member(int x, const Bitmapset *a);
extern int	__real_bms_member_index(Bitmapset *a, int x);
extern bool __real_bms_overlap(const Bitmapset *a, const Bitmapset *b);
extern bool __real_bms_overlap_list(const Bitmapset *a, const List *b);
extern bool __real_bms_nonempty_difference(const Bitmapset *a,
										   const Bitmapset *b);
extern int	__real_bms_singleton_member(const Bitmapset *a);
extern bool __real_bms_get_singleton_member(const Bitmapset *a, int *member);
extern int	__real_bms_num_members(const Bitmapset *a);
extern BMS_Membership __real_bms_membership(const Bitmapset *a);
extern int	__real_bms_next_member(const Bitmapset *a, int prevbit);
extern int	__real_bms_prev_member(const Bitmapset *a, int prevbit);
extern uint32 __real_bms_hash_value(const Bitmapset *a);
extern void __real_bms_free(Bitmapset *a);

/*
 * Bytes aset.c would charge for a request of `size`: a power-of-two chunk of
 * at least 8 bytes plus the 8-byte chunk header, or a dedicated block for
 * requests above the 8kB chunk limit.
 */
static uint64
aset_space(size_t size)
{
	if (size > 8192)
		return MAXALIGN(size) + 8 + 48;
	if (size < 8)
		size = 8;
	return pg_nextpower2_64(size) + 8;
}

/*
 * Write this process's rows.  Registered with on_proc_exit the first time
 * the process records anything.
 */
static void
census_dump(int code, Datum arg)
{
	char		path[MAXPGPATH];
	FILE	   *f;

	if (census_pid != MyProcPid)
		return;
	snprintf(path, sizeof(path), "%s/%d.tsv", census_dir, MyProcPid);
	f = fopen(path, "w");
	if (f == NULL)
		return;
	for (int i = 0; i < CENSUS_SLOTS; i++)
	{
		const CensusSlot *s = &census_slots[i];
		Dl_info		info;
		const char *fname = "?";
		uintptr_t	off = (uintptr_t) s->site;

		if (s->calls == 0)
			continue;
		if (s->site != NULL && dladdr(s->site, &info) != 0 &&
			info.dli_fname != NULL)
		{
			fname = info.dli_fname;
			off = (uintptr_t) s->site - (uintptr_t) info.dli_fbase;
		}
		fprintf(f, "%s\t%lx\t%s\t%llu\t%llu\t%llu\t%d",
				fname, (unsigned long) off, census_fn_name[s->fn],
				(unsigned long long) s->calls,
				(unsigned long long) s->members,
				(unsigned long long) s->words, s->max_words);
		for (int k = 0; k < 3; k++)
			fprintf(f, "\t%llu\t%llu", (unsigned long long) s->alloc[k],
					(unsigned long long) s->freed[k]);
		fputc('\n', f);
	}
	fclose(f);
}

/*
 * Find (or claim) the slot for (site, fn), or NULL when recording is off.
 * A forked child starts with its parent's table, so the table is cleared the
 * first time a new process records.
 */
static CensusSlot *
census_slot(const void *site, int fn)
{
	uint64		h;

	if (census_state < 0)
	{
		census_dir = getenv("BMS_CENSUS_DIR");
		census_state = (census_dir != NULL && census_dir[0] != '\0');
	}
	if (census_state == 0 || MyProcPid == 0)
		return NULL;
	if (census_pid != MyProcPid)
	{
		memset(census_slots, 0, sizeof(census_slots));
		census_pid = MyProcPid;
		census_cxt = NULL;
		on_proc_exit(census_dump, (Datum) 0);
	}

	h = ((uint64) (uintptr_t) site * UINT64CONST(0x9E3779B97F4A7C15)) ^ fn;
	for (int probe = 0; probe < CENSUS_SLOTS; probe++)
	{
		CensusSlot *s = &census_slots[(h + probe) & (CENSUS_SLOTS - 1)];

		if (s->calls == 0)
		{
			s->site = site;
			s->fn = fn;
			return s;
		}
		if (s->site == site && s->fn == fn)
			return s;
	}
	return NULL;
}

/* Record a read-only call that looked at `words` input words. */
static void
census_read(const void *site, int fn, int words)
{
	CensusSlot *s = census_slot(site, fn);

	if (s == NULL)
		return;
	s->calls++;
	s->words += words;
	if (words > s->max_words)
		s->max_words = words;
}

/*
 * Rounded sizes of `a` in the three representations; all zero for NULL.  The
 * sbm encoding is built from the members in a private context.
 */
static CensusSize
census_size(const Bitmapset *a)
{
	CensusSize	z = {{0, 0, 0}};
	MemoryContext old;
	uint64	   *members;
	int			n;
	int			m;
	int			first;
	int			last;
	Sbm		   *map;

	if (a == NULL)
		return z;
	/* Bump contexts cannot report chunk space; fall back to the request. */
	if (IsA(GetMemoryChunkContext(unconstify(Bitmapset *, a)), AllocSetContext))
		z.sz[0] = GetMemoryChunkSpace(unconstify(Bitmapset *, a));
	else
		z.sz[0] = aset_space(offsetof(Bitmapset, words) +
							 a->nwords * sizeof(bitmapword));
	first = __real_bms_next_member(a, -1) / BITS_PER_BITMAPWORD;
	last = __real_bms_prev_member(a, -1) / BITS_PER_BITMAPWORD;
	z.sz[1] = aset_space(16 + (last - first + 1) * sizeof(bitmapword));
	if (CritSectionCount > 0)
	{
		z.sz[2] = z.sz[0];
		return z;
	}
	if (census_cxt == NULL)
		census_cxt = AllocSetContextCreate(TopMemoryContext, "bms census",
										   ALLOCSET_DEFAULT_SIZES);
	old = MemoryContextSwitchTo(census_cxt);
	n = __real_bms_num_members(a);
	members = palloc(sizeof(uint64) * n);
	n = 0;
	m = -1;
	while ((m = __real_bms_next_member(a, m)) >= 0)
		members[n++] = (uint64) m;
	map = sbm_create_from_array(members, n);
	z.sz[2] = aset_space(8 + sbm_get_size(map));
	MemoryContextSwitchTo(old);
	MemoryContextReset(census_cxt);
	return z;
}

/* Count the members and words of a result. */
static void
census_shape(CensusSlot *s, const Bitmapset *r)
{
	s->calls++;
	if (r == NULL)
		return;
	s->members += __real_bms_num_members(r);
	s->words += r->nwords;
	if (r->nwords > s->max_words)
		s->max_words = r->nwords;
}

/* A function that always returns a fresh set (or NULL). */
static void
census_fresh(const void *site, int fn, const Bitmapset *r)
{
	CensusSlot *s = census_slot(site, fn);
	CensusSize	z;

	if (s == NULL)
		return;
	census_shape(s, r);
	z = census_size(r);
	for (int k = 0; k < 3; k++)
		s->alloc[k] += z.sz[k];
}

/*
 * A function that recycled input `a_ptr` (sized `za` before the call) into
 * result `r`.
 */
static void
census_recycled(const void *site, int fn, const void *a_ptr, CensusSize za,
				const Bitmapset *r)
{
	CensusSlot *s = census_slot(site, fn);
	CensusSize	zr;

	if (s == NULL)
		return;
	census_shape(s, r);
	zr = census_size(r);

	/* The real Bitmapset changes memory only when the chunk moved. */
	if ((const void *) r != a_ptr)
	{
		s->alloc[0] += zr.sz[0];
		s->freed[0] += za.sz[0];
	}

	/* Counterfactuals reallocate only when the size class grows. */
	for (int k = 1; k < 3; k++)
	{
		if (r == NULL)
			s->freed[k] += za.sz[k];
		else if (a_ptr == NULL || zr.sz[k] > za.sz[k])
		{
			s->alloc[k] += zr.sz[k];
			s->freed[k] += za.sz[k];
		}
	}
}

/* Sizes of `a`, or zeros when recording is off (skips the sbm build). */
static CensusSize
census_size_if_on(const Bitmapset *a)
{
	CensusSize	z = {{0, 0, 0}};

	if (census_state == 0)
		return z;
	return census_size(a);
}

#define SITE __builtin_return_address(0)
#define NW(s) ((s) != NULL ? (s)->nwords : 0)

#define PRODUCER(name, fn, params, args) \
Bitmapset *__wrap_bms_##name params; \
Bitmapset * \
__wrap_bms_##name params \
{ \
	Bitmapset  *r = __real_bms_##name args; \
	census_fresh(SITE, fn, r); \
	return r; \
}

#define RECYCLER(name, fn, params, args) \
Bitmapset *__wrap_bms_##name params; \
Bitmapset * \
__wrap_bms_##name params \
{ \
	const void *site = SITE; \
	const void *a_ptr = a; \
	CensusSize	za = census_size_if_on(a); \
	Bitmapset  *r = __real_bms_##name args; \
	census_recycled(site, fn, a_ptr, za, r); \
	return r; \
}

#define READER(rtype, name, fn, params, args, words) \
rtype __wrap_bms_##name params; \
rtype \
__wrap_bms_##name params \
{ \
	census_read(SITE, fn, words); \
	return __real_bms_##name args; \
}

PRODUCER(copy, F_COPY, (const Bitmapset *a), (a))
PRODUCER(make_singleton, F_SINGLETON, (int x), (x))
PRODUCER(union, F_UNION, (const Bitmapset *a, const Bitmapset *b), (a, b))
PRODUCER(intersect, F_INTERSECT, (const Bitmapset *a, const Bitmapset *b), (a, b))
PRODUCER(difference, F_DIFFERENCE, (const Bitmapset *a, const Bitmapset *b), (a, b))
PRODUCER(offset_members, F_OFFSET, (const Bitmapset *a, int offset), (a, offset))
RECYCLER(add_member, F_ADD_MEMBER, (Bitmapset *a, int x), (a, x))
RECYCLER(del_member, F_DEL_MEMBER, (Bitmapset *a, int x), (a, x))
RECYCLER(add_members, F_ADD_MEMBERS, (Bitmapset *a, const Bitmapset *b), (a, b))
RECYCLER(replace_members, F_REPLACE_MEMBERS, (Bitmapset *a, const Bitmapset *b), (a, b))
RECYCLER(add_range, F_ADD_RANGE, (Bitmapset *a, int lower, int upper), (a, lower, upper))
RECYCLER(int_members, F_INT_MEMBERS, (Bitmapset *a, const Bitmapset *b), (a, b))
RECYCLER(del_members, F_DEL_MEMBERS, (Bitmapset *a, const Bitmapset *b), (a, b))

/*
 * bms_join recycles one input and frees the other.  Model it as recycling
 * the larger input and freeing the smaller.
 */
Bitmapset  *__wrap_bms_join(Bitmapset *a, Bitmapset *b);
Bitmapset *
__wrap_bms_join(Bitmapset *a, Bitmapset *b)
{
	const void *site = SITE;
	CensusSize	za = census_size_if_on(a);
	CensusSize	zb = census_size_if_on(b);
	bool		keep_a = za.sz[0] >= zb.sz[0];
	Bitmapset  *r = __real_bms_join(a, b);
	CensusSlot *s;

	census_recycled(site, F_JOIN, keep_a ? (void *) a : (void *) b,
					keep_a ? za : zb, r);
	s = census_slot(site, F_JOIN);
	if (s != NULL)
		for (int k = 0; k < 3; k++)
			s->freed[k] += keep_a ? zb.sz[k] : za.sz[k];
	return r;
}

void		__wrap_bms_free(Bitmapset *a);
void
__wrap_bms_free(Bitmapset *a)
{
	const void *site = SITE;

	if (a != NULL && census_state != 0)
	{
		CensusSize	z = census_size(a);
		CensusSlot *s = census_slot(site, F_FREE);

		if (s != NULL)
		{
			s->calls++;
			for (int k = 0; k < 3; k++)
				s->freed[k] += z.sz[k];
		}
	}
	__real_bms_free(a);
}

READER(bool, equal, F_EQUAL, (const Bitmapset *a, const Bitmapset *b), (a, b), NW(a) + NW(b))
READER(int, compare, F_COMPARE, (const Bitmapset *a, const Bitmapset *b), (a, b), NW(a) + NW(b))
READER(bool, is_subset, F_IS_SUBSET, (const Bitmapset *a, const Bitmapset *b), (a, b), NW(a) + NW(b))
READER(BMS_Comparison, subset_compare, F_SUBSET_COMPARE, (const Bitmapset *a, const Bitmapset *b), (a, b), NW(a) + NW(b))
READER(bool, is_member, F_IS_MEMBER, (int x, const Bitmapset *a), (x, a), NW(a))
READER(int, member_index, F_MEMBER_INDEX, (Bitmapset *a, int x), (a, x), NW(a))
READER(bool, overlap, F_OVERLAP, (const Bitmapset *a, const Bitmapset *b), (a, b), NW(a) + NW(b))
READER(bool, overlap_list, F_OVERLAP_LIST, (const Bitmapset *a, const List *b), (a, b), NW(a))
READER(bool, nonempty_difference, F_NONEMPTY_DIFFERENCE, (const Bitmapset *a, const Bitmapset *b), (a, b), NW(a) + NW(b))
READER(int, singleton_member, F_SINGLETON_MEMBER, (const Bitmapset *a), (a), NW(a))
READER(bool, get_singleton_member, F_GET_SINGLETON_MEMBER, (const Bitmapset *a, int *member), (a, member), NW(a))
READER(int, num_members, F_NUM_MEMBERS, (const Bitmapset *a), (a), NW(a))
READER(BMS_Membership, membership, F_MEMBERSHIP, (const Bitmapset *a), (a), NW(a))
READER(int, next_member, F_NEXT_MEMBER, (const Bitmapset *a, int prevbit), (a, prevbit), NW(a))
READER(int, prev_member, F_PREV_MEMBER, (const Bitmapset *a, int prevbit), (a, prevbit), NW(a))
READER(uint32, hash_value, F_HASH_VALUE, (const Bitmapset *a), (a), NW(a))

#endif							/* BMS_CENSUS */
