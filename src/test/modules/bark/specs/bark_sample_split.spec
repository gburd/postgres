# The planner's separator sample (bark_estimate_qual_selectivity) across a
# root split.
#
# Keys are 900-byte strings in a bark_jsonb_wildcard_ops index, so a page
# holds about eight keys or pivots and the setup builds a three-level tree.
# s1 plans a #? qual, which builds the sample, and stops at
# bark-sample-build after reading the root from the meta page, before
# reading any page.  s2 inserts until the root splits.  Woken, s1 reads the
# tree from the old root, which is now an inner page that has a right
# sibling.  The plan must finish without an error and with an estimate within
# 3x of the 300 rows it sized the table at before it stopped, and a second
# plan, which finds the root moved and builds the sample again, within 3x
# of the 3300 there are after.

setup
{
	CREATE EXTENSION IF NOT EXISTS injection_points;
	CREATE EXTENSION IF NOT EXISTS pageinspect;
	CREATE EXTENSION bark_wildcard;
	CREATE FUNCTION ss_doc(int) RETURNS jsonb LANGUAGE sql IMMUTABLE AS
		$$ SELECT jsonb_build_object('k', lpad($1::text, 8, '0') ||
			   (SELECT string_agg(md5($1::text || g::text), '') FROM generate_series(1, 28) g)) $$;
	CREATE TABLE ss (n int, doc jsonb) WITH (autovacuum_enabled = off);
	INSERT INTO ss SELECT g, ss_doc(g) FROM generate_series(1, 600, 2) g;
	CREATE INDEX ss_idx ON ss USING bark (doc bark_jsonb_wildcard_ops);
	ANALYZE ss;
	CREATE FUNCTION ss_sane(act int) RETURNS bool LANGUAGE plpgsql AS $$
	DECLARE plan json; est float8;
	BEGIN
		EXECUTE 'EXPLAIN (FORMAT JSON) SELECT * FROM ss WHERE doc #? ''k''' INTO plan;
		est := (plan -> 0 -> 'Plan' ->> 'Plan Rows')::float8;
		RETURN est BETWEEN act / 3.0 AND act * 3.0;
	END $$;
}

teardown
{
	DROP TABLE ss;
	DROP FUNCTION ss_doc(int), ss_sane(int);
	DROP EXTENSION bark_wildcard;
}

session s1
setup
{
	SELECT injection_points_set_local();
}
step s1_level	{ SELECT level FROM bark_metap('ss_idx'); }
step s1_wait	{ SELECT injection_points_attach('bark-sample-build', 'wait'); }
step s1_plan	{ SELECT ss_sane(300) AS sane; }
step s1_again	{ SELECT ss_sane(3300) AS sane; }

session s2
step s2_split
{
	INSERT INTO ss SELECT g, ss_doc(g) FROM generate_series(2, 6000, 2) g;
	SELECT level >= 3 AS root_split FROM bark_metap('ss_idx');
}
step s2_wake
{
	SELECT injection_points_detach('bark-sample-build');
	SELECT injection_points_wakeup('bark-sample-build');
}

permutation s1_level s1_wait s1_plan s2_split s2_wake s1_again
