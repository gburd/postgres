/*
 * test_sbm_bms_shapes: time Bitmapset and sbm operations on the set shapes
 * a Bitmapset census of the planner found.
 *
 * The census (bms_census.c, a measurement-only build) showed that the only
 * Bitmapsets sbm encodes much smaller are relid sets in queries over many
 * partitions.  This function measures what each operation costs on those
 * shapes, so the memory saving can be weighed against the CPU cost.  For
 * `nparts` partitions the shapes are:
 *
 *	 tiny		{1,2,3} and {2,4}: the common case (most Bitmapsets in the
 *				regression suite and pgbench are one word)
 *	 child		one child relid, {nparts-3} and {nparts-1}: a child rel's
 *				relids, all leading zero words in a Bitmapset
 *	 childjoin	{3, nparts+5, 2*nparts+5} and {3, nparts+7, 2*nparts+7}: a
 *				child join's relids, an outer-join relid plus one child of
 *				each side
 *	 allparts	[2, nparts+2) and {nparts-3}: every partition, as built by
 *				expand_partitioned_rtentry, against one child
 *
 * Each operation runs `iters` times per timed block; each block repeats
 * `reps` times, alternating which structure goes first, and the minimum
 * per-operation time is reported, since timing noise only adds delay.  The
 * sbm side is built tight (capacity equal to its encoded size), the most
 * favourable case for both its memory and its copy cost.  Both byte columns
 * are the allocated chunk (GetMemoryChunkSpace), header and rounding
 * included; an sbm made with sbm_create() is one contiguous chunk.
 */

#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "lib/sbm.h"
#include "nodes/bitmapset.h"
#include "portability/instr_time.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/tuplestore.h"

PG_FUNCTION_INFO_V1(test_sbm_bms_shapes);

typedef enum ShapeOp
{
	OP_UNION, OP_EQUAL, OP_SUBSET, OP_OVERLAP, OP_MEMBER, OP_ITERATE,
	OP_ADD, OP_COPY, OP_HASH, NUM_OPS
} ShapeOp;

static const char *const op_name[NUM_OPS] = {
	"union", "equal", "is_subset(b,a)", "overlap", "is_member",
	"iterate", "copy+add_member", "copy", "hash",
};

/* One shape: two sets as member arrays, and a member of the first. */
typedef struct Shape
{
	const char *name;
	int64		na;
	int64		nb;
	uint64	   *a;
	uint64	   *b;
	int			x;
} Shape;

static double
elapsed_ns(instr_time start)
{
	instr_time	now;

	INSTR_TIME_SET_CURRENT(now);
	INSTR_TIME_SUBTRACT(now, start);
	return (double) INSTR_TIME_GET_NANOSEC(now);
}

static Bitmapset *
to_bms(const uint64 *v, int64 n)
{
	Bitmapset  *r = NULL;

	for (int64 i = 0; i < n; i++)
		r = bms_add_member(r, (int) v[i]);
	return r;
}

/*
 * An sbm holding v[0..n) whose capacity is its encoded size, growing the
 * capacity only as far as sbm_add_many needs for working room.
 */
static Sbm *
to_sbm_tight(const uint64 *v, int64 n)
{
	Sbm		   *probe = sbm_create_from_array(v, (size_t) n);
	size_t		size = sbm_get_size(probe);

	sbm_free(probe);
	for (;;)
	{
		Sbm		   *m = sbm_create(size);

		if (sbm_add_many(m, v, (size_t) n))
			return m;
		sbm_free(m);
		size += 8;
	}
}

/* Time `iters` runs of `op` on Bitmapsets; returns ns per operation. */
static double
time_bms(ShapeOp op, const Bitmapset *a, const Bitmapset *b, int x,
		 int iters, volatile uint64 *sink)
{
	instr_time	t;

	INSTR_TIME_SET_CURRENT(t);
	for (int i = 0; i < iters; i++)
	{
		switch (op)
		{
			case OP_UNION:
				{
					Bitmapset  *r = bms_union(a, b);

					*sink += r->nwords;
					bms_free(r);
					break;
				}
			case OP_EQUAL:
				*sink += bms_equal(a, b);
				break;
			case OP_SUBSET:
				*sink += bms_is_subset(b, a);
				break;
			case OP_OVERLAP:
				*sink += bms_overlap(a, b);
				break;
			case OP_MEMBER:
				*sink += bms_is_member(x, a);
				break;
			case OP_ITERATE:
				{
					int			m = -1;

					while ((m = bms_next_member(a, m)) >= 0)
						*sink += m;
					break;
				}
			case OP_ADD:
				{
					Bitmapset  *r = bms_add_member(bms_copy(a), x + 1);

					*sink += r->nwords;
					bms_free(r);
					break;
				}
			case OP_COPY:
				{
					Bitmapset  *r = bms_copy(a);

					*sink += r->nwords;
					bms_free(r);
					break;
				}
			case OP_HASH:
				*sink += bms_hash_value(a);
				break;
			case NUM_OPS:
				break;
		}
	}
	return elapsed_ns(t) / iters;
}

/* Time `iters` runs of `op` on sbms; returns ns per operation. */
static double
time_sbm(ShapeOp op, const Sbm *a, const Sbm *b, int x, int iters,
		 volatile uint64 *sink)
{
	instr_time	t;

	INSTR_TIME_SET_CURRENT(t);
	for (int i = 0; i < iters; i++)
	{
		switch (op)
		{
			case OP_UNION:
				{
					Sbm		   *r = sbm_union(a, b);

					*sink += sbm_get_size(r);
					sbm_free(r);
					break;
				}
			case OP_EQUAL:
				*sink += sbm_equals(a, b);
				break;
			case OP_SUBSET:
				*sink += sbm_is_subset(b, a);
				break;
			case OP_OVERLAP:
				*sink += sbm_overlap(a, b);
				break;
			case OP_MEMBER:
				*sink += sbm_contains(a, (uint64) x, NULL);
				break;
			case OP_ITERATE:
				{
					SbmCursor	c = SBM_CURSOR_INIT;
					uint64		m = SBM_IDX_MAX;

					while ((m = sbm_next_member(a, m, &c)) != SBM_IDX_MAX)
						*sink += m;
					break;
				}
			case OP_ADD:
				{
					Sbm		   *r = sbm_copy(a);

					sbm_add_grow(&r, (uint64) x + 1);
					*sink += sbm_get_size(r);
					sbm_free(r);
					break;
				}
			case OP_COPY:
				{
					Sbm		   *r = sbm_copy(a);

					*sink += sbm_get_size(r);
					sbm_free(r);
					break;
				}
			case OP_HASH:
				*sink += sbm_hash(a);
				break;
			case NUM_OPS:
				break;
		}
	}
	return elapsed_ns(t) / iters;
}

/* Fill `s` with members [lo, hi) for a and {bval} for b. */
static void
shape_range(Shape *s, const char *name, uint64 lo, uint64 hi, uint64 bval,
			int x)
{
	s->name = name;
	s->na = (int64) (hi - lo);
	s->a = palloc_array(uint64, s->na);
	for (int64 i = 0; i < s->na; i++)
		s->a[i] = lo + (uint64) i;
	s->nb = 1;
	s->b = palloc_array(uint64, 1);
	s->b[0] = bval;
	s->x = x;
}

static void
shape_list(Shape *s, const char *name, const uint64 *a, int64 na,
		   const uint64 *b, int64 nb, int x)
{
	s->name = name;
	s->na = na;
	s->a = palloc_array(uint64, na);
	memcpy(s->a, a, sizeof(uint64) * na);
	s->nb = nb;
	s->b = palloc_array(uint64, nb);
	memcpy(s->b, b, sizeof(uint64) * nb);
	s->x = x;
}

Datum
test_sbm_bms_shapes(PG_FUNCTION_ARGS)
{
	int64		nparts = PG_GETARG_INT64(0);
	int			iters = PG_GETARG_INT32(1);
	int			reps = PG_GETARG_INT32(2);
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Shape		shapes[4];
	volatile uint64 sink = 0;

	if (nparts < 16 || nparts > 100000 || iters < 1 || reps < 1)
		elog(ERROR, "invalid argument");
	InitMaterializedSRF(fcinfo, 0);

	/* Build the four shapes described in the header comment. */
	{
		const uint64 ta[] = {1, 2, 3};
		const uint64 tb[] = {2, 4};
		const uint64 ca[] = {nparts - 3};
		const uint64 cb[] = {nparts - 1};
		const uint64 ja[] = {3, nparts + 5, 2 * nparts + 5};
		const uint64 jb[] = {3, nparts + 7, 2 * nparts + 7};

		shape_list(&shapes[0], "tiny", ta, 3, tb, 2, 3);
		shape_list(&shapes[1], "child", ca, 1, cb, 1, (int) nparts - 3);
		shape_list(&shapes[2], "childjoin", ja, 3, jb, 3, (int) nparts + 5);
		shape_range(&shapes[3], "allparts", 2, nparts + 2, nparts - 3,
					(int) nparts / 2);
	}

	for (int si = 0; si < lengthof(shapes); si++)
	{
		const Shape *s = &shapes[si];
		Bitmapset  *ba = to_bms(s->a, s->na);
		Bitmapset  *bb = to_bms(s->b, s->nb);
		Sbm		   *sa = to_sbm_tight(s->a, s->na);
		Sbm		   *sb = to_sbm_tight(s->b, s->nb);

		for (int op = 0; op < NUM_OPS; op++)
		{
			double		best_b = -1;
			double		best_s = -1;
			Datum		vals[7];
			bool		nulls[7] = {false};

			for (int r = 0; r < reps; r++)
			{
				double		tb_ns;
				double		ts_ns;

				if (r % 2 == 0)
				{
					tb_ns = time_bms(op, ba, bb, s->x, iters, &sink);
					ts_ns = time_sbm(op, sa, sb, s->x, iters, &sink);
				}
				else
				{
					ts_ns = time_sbm(op, sa, sb, s->x, iters, &sink);
					tb_ns = time_bms(op, ba, bb, s->x, iters, &sink);
				}
				if (best_b < 0 || tb_ns < best_b)
					best_b = tb_ns;
				if (best_s < 0 || ts_ns < best_s)
					best_s = ts_ns;
			}
			vals[0] = CStringGetTextDatum(s->name);
			vals[1] = CStringGetTextDatum(op_name[op]);
			vals[2] = Float8GetDatum(best_b);
			vals[3] = Float8GetDatum(best_s);
			vals[4] = Float8GetDatum(best_s / best_b);
			vals[5] = Int64GetDatum((int64) GetMemoryChunkSpace(ba));
			vals[6] = Int64GetDatum((int64) GetMemoryChunkSpace(sa));
			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, vals,
								 nulls);
		}
		bms_free(ba);
		bms_free(bb);
		sbm_free(sa);
		sbm_free(sb);
	}
	if (sink == 42)
		elog(NOTICE, "unlikely");
	return (Datum) 0;
}
