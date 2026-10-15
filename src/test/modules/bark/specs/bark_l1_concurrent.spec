# The level-1 entry cache across another backend's splits.
#
# s1 warms its level-1 entry table with lookups over the whole key range.
# s2 then splits the level-1 pages s1 has cached, many times, by inserting
# keys between the existing ones.  s1's cached pages keep their low keys, but
# a key may now lie past its nearest cached page's high key: that lookup
# goes through the root (no bark-l1-start notice) and records the key's new
# page, so the same lookup then starts below the root (one notice).  Every
# lookup and range must return the same rows as a sequential scan.
#
# The second permutation stops s1's descent after it started at a cached
# level-1 page and read the downlink to its key's leaf, before following it
# (bark-search-descend).  s2 meanwhile splits that leaf by inserting keys
# around s1's.  Woken, s1 reads the leaf, moves right if its key went to the
# new page, and returns its row.

setup
{
	CREATE EXTENSION injection_points;
	CREATE EXTENSION pageinspect;
	CREATE FUNCTION l1k(int) RETURNS text LANGUAGE sql IMMUTABLE AS
		$$ SELECT lpad($1::text, 8, '0') || repeat('x', 380) $$;
	CREATE TABLE l1s (k text COLLATE "C", n int) WITH (autovacuum_enabled = off);
	INSERT INTO l1s SELECT l1k(g), g FROM generate_series(0, 7999, 2) g;
	CREATE INDEX l1s_idx ON l1s USING bark (k);
	-- rows the index returns for each probe must equal a sequential scan's
	CREATE FUNCTION l1s_ok() RETURNS bigint LANGUAGE plpgsql AS $$
	DECLARE bad bigint := 0; a text; b text;
	BEGIN
		FOR g IN 0..7999 BY 3 LOOP
			PERFORM set_config('enable_seqscan', 'off', true);
			PERFORM set_config('enable_bitmapscan', 'off', true);
			SELECT coalesce(string_agg(n::text, ',' ORDER BY n), '') INTO a FROM l1s
				WHERE k >= l1k(g) AND k < l1k(g + 2);
			PERFORM set_config('enable_seqscan', 'on', true);
			PERFORM set_config('enable_indexscan', 'off', true);
			SELECT coalesce(string_agg(n::text, ',' ORDER BY n), '') INTO b FROM l1s
				WHERE k >= l1k(g) AND k < l1k(g + 2);
			PERFORM set_config('enable_indexscan', 'on', true);
			IF a IS DISTINCT FROM b THEN bad := bad + 1; END IF;
		END LOOP;
		RETURN bad;
	END $$;
}

teardown
{
	DROP TABLE l1s;
	DROP FUNCTION l1k(int), l1s_ok();
	DROP EXTENSION pageinspect;
	DROP EXTENSION injection_points;
}

session s1
setup
{
	SET enable_seqscan = off;
	SET enable_bitmapscan = off;
	SELECT injection_points_set_local();
}
step s1_warm
{
	SELECT count(*) FROM generate_series(0, 7999, 5) g,
		LATERAL (SELECT k FROM l1s WHERE k >= l1k(g) ORDER BY k LIMIT 1) s;
}
step s1_level	{ SELECT level >= 2 AS tall FROM bark_metap('l1s_idx'); }
step s1_notice	{ SELECT injection_points_attach('bark-l1-start', 'notice'); }
step s1_one		{ SELECT count(*) FROM l1s WHERE k = l1k(4000); }
step s1_quiet	{ SELECT injection_points_detach('bark-l1-start'); }
step s1_check	{ SELECT l1s_ok() AS mismatches; }
step s1_wait	{ SELECT injection_points_attach('bark-search-descend', 'wait'); }
step s1_stuck	{ SELECT n FROM l1s WHERE k = l1k(6000); }

session s2
step s2_split	{ INSERT INTO l1s SELECT l1k(g), g FROM generate_series(1, 7999, 2) g; }
step s2_more	{ INSERT INTO l1s SELECT l1k(g) || 'z', 100000 + g FROM generate_series(0, 7999) g; }
step s2_near	{ INSERT INTO l1s SELECT l1k(5990 + g) || 'q', 200000 + g FROM generate_series(0, 20) g; }
step s2_wake
{
	SELECT injection_points_detach('bark-search-descend');
	SELECT injection_points_wakeup('bark-search-descend');
}

permutation s1_warm s1_level s2_split s2_more s1_notice s1_one s1_one s1_quiet s1_check
permutation s1_warm s1_wait s1_stuck s2_near s2_wake s1_check
