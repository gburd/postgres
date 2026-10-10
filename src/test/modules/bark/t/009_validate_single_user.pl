# Copyright (c) 2026, PostgreSQL Global Development Group

# amvalidate() on invalid BARK operator classes with no client.  In
# single-user mode barkvalidate's INFO reports go nowhere (errstart returns
# false), and amvalidate must still return false for each class.  The
# rules are tested one by one, with their messages, in sql/bark_validate.sql.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# As test_misc's 008: single-user mode fails permission checks on Windows.
if ($windows_os)
{
	plan skip_all => 'this test is not supported by this platform';
}

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->start;

# Three classes that together break every rule barkvalidate reports.
$node->safe_psql(
	'postgres', q{
SET allow_system_table_mods = on;
CREATE FUNCTION bv_setof_void(internal) RETURNS SETOF void
  AS 'btint4sortsupport' LANGUAGE internal STRICT;
CREATE FUNCTION bv_query_bad(int4[], int4, internal, internal, internal, internal)
  RETURNS void AS 'ginqueryarrayextract' LANGUAGE internal STRICT;

-- Scalar: no comparator, a set-returning sortsupport, support number 7,
-- strategy 0, a search operator over the wrong types, an ordering
-- operator with strategy 5 and no sort family.
CREATE OPERATOR FAMILY bv_bt USING btree;
CREATE OPERATOR CLASS bv_bt FOR TYPE int4 USING bark FAMILY bv_bt AS
    OPERATOR 1 <, OPERATOR 2 <=, OPERATOR 3 =,
    OPERATOR 5 <~> (int4, int4) FOR ORDER BY float_ops,
    FUNCTION 2 bv_setof_void(internal),
    FUNCTION 4 btequalimage(oid);
UPDATE pg_amop SET amopstrategy = 0
  WHERE amopfamily = (SELECT oid FROM pg_opfamily WHERE opfname = 'bv_bt')
    AND amopstrategy = 2;
UPDATE pg_amop SET amoprighttype = 'int8'::regtype
  WHERE amopfamily = (SELECT oid FROM pg_opfamily WHERE opfname = 'bv_bt')
    AND amopstrategy = 3;
UPDATE pg_amop SET amopsortfamily = 0
  WHERE amopfamily = (SELECT oid FROM pg_opfamily WHERE opfname = 'bv_bt')
    AND amoppurpose = 'o';
UPDATE pg_amproc SET amprocnum = 7
  WHERE amprocfamily = (SELECT oid FROM pg_opfamily WHERE opfname = 'bv_bt')
    AND amprocnum = 4;

-- Multikey: comparator under the storage type only, a cross-type one,
-- in_range, a wrong procedure 8 and no 9, no 7, an ordering operator with
-- no sort family, a search operator over the wrong types.
CREATE OPERATOR CLASS bv_mk FOR TYPE int4[] USING bark AS
    OPERATOR 1 && (anyarray, anyarray),
    OPERATOR 20 + (int4, int4) FOR ORDER BY integer_ops,
    FUNCTION 1 btint4cmp(int4, int4),
    FUNCTION 1 (int4, int8) btint48cmp(int4, int8),
    FUNCTION 3 (int4[], int4[]) in_range(int4, int4, int4, bool, bool),
    FUNCTION 8 bv_query_bad(int4[], int4, internal, internal, internal, internal),
    STORAGE int4;
UPDATE pg_amop SET amopsortfamily = 0
  WHERE amopfamily = (SELECT oid FROM pg_opfamily WHERE opfname = 'bv_mk')
    AND amoppurpose = 'o';
UPDATE pg_amop SET amoprighttype = 'int4'::regtype
  WHERE amopfamily = (SELECT oid FROM pg_opfamily WHERE opfname = 'bv_mk')
    AND amopstrategy = 1;

-- Multikey: no comparator at all.
CREATE OPERATOR CLASS bv_mk2 FOR TYPE int4[] USING bark AS
    OPERATOR 1 && (anyarray, anyarray),
    FUNCTION 7 ginarrayextract(anyarray, internal, internal),
    STORAGE int4;
});

# With a client every rule is reported.
my ($ret, $stdout, $stderr) = $node->psql('postgres',
	"SELECT string_agg(amvalidate(oid)::text, ',' ORDER BY opcname) FROM pg_opclass WHERE opcname LIKE 'bv%'"
);
is($stdout, 'false,false,false', 'invalid with a client');
my $reports = () = $stderr =~ /INFO:/g;
is($reports, 16, 'every rule reported to a client');
$node->stop;

my $query =
  "SELECT opcname, amvalidate(oid) FROM pg_opclass WHERE opcname LIKE 'bv%' ORDER BY 1;\n";
($stdout, $stderr) = ('', '');
my $ok = run_log(
	[
		'postgres', '--single', '-F',
		'-c' => 'log_min_messages=warning',
		'-c' => 'exit_on_error=true',
		'-D' => $node->data_dir,
		'postgres'
	],
	'<' => \$query,
	'>' => \$stdout,
	'2>' => \$stderr);
ok($ok, 'single-user amvalidate ran');
my $invalid = () = $stdout =~ /amvalidate = "f"/g;
is($invalid, 3, 'invalid with no client');
unlike("$stdout$stderr", qr/access method bark/, 'nothing reported');

done_testing();
