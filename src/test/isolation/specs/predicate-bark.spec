# Test page-level predicate locking in a BARK index.
#
# A serializable index scan (in one transaction) and an index insert (in
# another) that touch the same leaf page form a read/write conflict and must
# produce a serialization failure when they interleave.  Scans and inserts that
# touch different leaf pages must not conflict (no false positive).
#
# The table is large enough that distant key ranges live on different leaf
# pages, so the no-conflict permutations exercise the fine-grained page locking
# rather than a single whole-index page.

setup
{
 create table bark_pred (id int4) with (autovacuum_enabled = off);
 insert into bark_pred select g from generate_series(1, 20000) g;
 create index bark_pred_idx on bark_pred using bark (id);
}

teardown
{
 drop table bark_pred;
}

session s1
setup
{
 begin isolation level serializable;
 set enable_seqscan = off;
 set enable_bitmapscan = off;
 set enable_indexonlyscan = on;
}
# reads a low range, inserts into a low range (same leaves as s2's write/read)
step r1		{ select count(*) from bark_pred where id between 100 and 200; }
step w1		{ insert into bark_pred values (150); }
# far-apart variants for the no-conflict permutations
step r1f	{ select count(*) from bark_pred where id between 100 and 200; }
step w1f	{ insert into bark_pred values (150); }
step c1		{ commit; }

session s2
setup
{
 begin isolation level serializable;
 set enable_seqscan = off;
 set enable_bitmapscan = off;
 set enable_indexonlyscan = on;
}
# reads the range s1 writes into, and writes into the range s1 reads
step r2		{ select count(*) from bark_pred where id between 120 and 180; }
step w2		{ insert into bark_pred values (175); }
# far-apart variants: a disjoint high range, many leaves away
step r2f	{ select count(*) from bark_pred where id between 18000 and 18100; }
step w2f	{ insert into bark_pred values (18050); }
step c2		{ commit; }


# Overlapping ranges, interleaved so both read before the other commits:
# a read/write cycle that must be caught as a serialization failure.
permutation r1 w1 r2 w2 c1 c2
permutation r1 r2 w1 w2 c1 c2
permutation r2 r1 w2 w1 c2 c1

# Disjoint ranges on different leaf pages: no conflict, both commit.
permutation r1f w1f c1 r2f w2f c2
permutation r1f r2f w1f w2f c1 c2
permutation r2f r1f w2f w1f c2 c1
