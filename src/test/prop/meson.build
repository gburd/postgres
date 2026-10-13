# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Property-based tests built on libhegel.  Reached only when
# prop_tests_enabled (see the top-level meson.build): -Dprop_tests=enabled,
# libhegel present, and TAP tests enabled.
#
# All areas link into one binary, pg_prop, whose first argument selects the
# area:  pg_prop <area> [options] [property ...].  Two TAP drivers run it:
# 001_prop.pl runs the in-process areas (Tier A: src/common, src/port code
# linked directly); 002_sql_props.pl starts a node and runs the SQL-oracle
# areas (Tier B: planner / executor / type code, reached over libpq) against
# it.  A developer debugging a failure runs one area (or one property) by hand
# with --iterations / --seed.  See README and DESIGN.

# Area source files.  Add one line as each target lands; keep the area lists
# in t/001_prop.pl (Tier A) and t/002_sql_props.pl (Tier B) in step.
prop_area_sources = files(
  # Tier A: in-process
  # Tier B: SQL oracle (needs a running node)
  'prop_path.c',
  'prop_jsonapi.c',
  'prop_encoding.c',
  'prop_unicode_case.c',
  'prop_unicode_norm.c',
  'prop_pglz.c',
  'prop_base64.c',
  'prop_prng.c',
  'prop_crypto.c',
  'prop_scram.c',
  'prop_binaryheap.c',
  'prop_bitutils.c',
  'prop_numeric.c',
  'prop_numcast.c',
  'prop_numscale.c',
  'prop_numround.c',
  'prop_numfmt.c',
  'prop_numericalg.c',
  'prop_numericcmp.c',
  'prop_numericdiv.c',
  'prop_decimalfmt.c',
  'prop_mathfn.c',
  'prop_mathround.c',
  'prop_powexp.c',
  'prop_float.c',
  'prop_floatspecial.c',
  'prop_floatcast.c',
  'prop_intops.c',
  'prop_intbit.c',
  'prop_bitint.c',
  'prop_bitlogic.c',
  'prop_bitstringops.c',
  'prop_safemath.c',
  'prop_arithcmp.c',
  'prop_cmpops.c',
  'prop_text.c',
  'prop_textfns.c',
  'prop_textpos.c',
  'prop_textcase.c',
  'prop_textlength.c',
  'prop_textnum.c',
  'prop_textsplit.c',
  'prop_strfunc.c',
  'prop_strpos.c',
  'prop_substring.c',
  'prop_padtrim.c',
  'prop_trimfns.c',
  'prop_like.c',
  'prop_likepattern.c',
  'prop_stringmatch.c',
)


pg_prop = executable('pg_prop',
  files('pg_prop.c', 'pg_prop_sql.c') + prop_area_sources,
  dependencies: [frontend_code, libpq, libhegel],
  kwargs: default_bin_args + {
    'install': false,
  },
)
testprep_targets += pg_prop

tests += {
  'name': 'prop',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_prop.pl',
      't/002_sql_props.pl',
    ],
    'deps': [pg_prop],
  },
}
