# src/test/prop/Makefile
#
# Property-based tests built on libhegel.  Only reachable when configure was
# run with --with-hegel (which also requires --enable-tap-tests); see
# src/test/Makefile.
#
# Everything links into one program, pg_prop, whose first argument selects the
# area:  pg_prop <area> [options] [property ...].  The TAP driver runs each
# area; a developer debugging a failure runs one area or one property by hand
# with --iterations / --seed.  See README and DESIGN.

PGFILEDESC = "pg_prop - property-based tests"
PGAPPICON = win32

PROGRAM = pg_prop
# One object per area file, plus the shared harness (pg_prop.o) and the SQL
# oracle helper (pg_prop_sql.o).  Add prop_<area>.o here as each area lands
# (keep in step with the area lists in t/001_prop.pl and t/002_sql_props.pl).
OBJS = \
	$(WIN32RES) \
	pg_prop.o \
	pg_prop_sql.o \
	prop_path.o \
	prop_jsonapi.o \
	prop_encoding.o \
	prop_unicode_case.o \
	prop_unicode_norm.o \
	prop_pglz.o \
	prop_base64.o \
	prop_prng.o \
	prop_crypto.o \
	prop_scram.o \
	prop_binaryheap.o \
	prop_bitutils.o \
	prop_numeric.o \
	prop_numcast.o \
	prop_numscale.o \
	prop_numround.o \
	prop_numfmt.o \
	prop_numericalg.o \
	prop_numericcmp.o \
	prop_numericdiv.o \
	prop_decimalfmt.o \
	prop_mathfn.o \
	prop_mathround.o \
	prop_powexp.o \
	prop_float.o \
	prop_floatspecial.o \
	prop_floatcast.o \
	prop_intops.o \
	prop_intbit.o \
	prop_bitint.o \
	prop_bitlogic.o \
	prop_bitstringops.o \
	prop_safemath.o \
	prop_arithcmp.o \
	prop_cmpops.o \
	prop_text.o \
	prop_textfns.o \
	prop_textpos.o \
	prop_textcase.o \
	prop_textlength.o \
	prop_textnum.o \
	prop_textsplit.o \
	prop_strfunc.o \
	prop_strpos.o \
	prop_substring.o \
	prop_padtrim.o \
	prop_trimfns.o \
	prop_like.o \
	prop_likepattern.o \
	prop_stringmatch.o \
	prop_regexp.o \
	prop_regexfns.o \
	prop_regexmatch.o \
	prop_regexpreplace.o \
	prop_char.o \
	prop_charconv.o \
	prop_charfuncs.o \
	prop_varchartypmod.o \
	prop_cash.o \
	prop_bool.o \
	prop_boolcast.o \
	prop_booltest.o \
	prop_boolexpr.o \
	prop_casewhen.o \
	prop_condexpr.o \
	prop_coalesce.o \
	prop_coalescefn.o \
	prop_nullsemantics.o \
	prop_nulldistinct.o \
	prop_oid.o \
	prop_uuid.o \
	prop_uuidops.o \
	prop_bytea.o \
	prop_byteaops.o \
	prop_bit.o \
	prop_record.o \
	prop_enum.o \
	prop_jsonb.o \
	prop_jsonbops.o \
	prop_jsonbpath.o \
	prop_jsonbagg2.o \
	prop_jsonbnum.o \
	prop_jsonbtype.o \
	prop_jsonbsub.o \
	prop_jsonbkeys.o \
	prop_jsonbcontain.o \
	prop_jsonbdelete.o \
	prop_jsonbexists.o \
	prop_jsonbmerge.o \
	prop_jsonbarray.o \
	prop_jsonbroundtrip.o \
	prop_jsonnest.o \
	prop_jsonbuild.o \
	prop_jsonconstruct.o \
	prop_jsontext.o \
	prop_jsonformat.o \
	prop_jsonpath.o \
	prop_jsonpathq.o \
	prop_jsonpathmath.o \
	prop_jsoneach.o \
	prop_array.o \
	prop_arrayops.o \
	prop_arrayslice.o \
	prop_arraycmp.o \
	prop_arrayconstruct.o \
	prop_arrayreduce.o \
	prop_arraysort.o \
	prop_arraynull.o \
	prop_anyall.o \
	prop_inlist.o \
	prop_range.o \
	prop_rangeops.o \
	prop_rangeagg.o \
	prop_multirange.o \
	prop_inet.o \
	prop_inetarith.o \
	prop_networkops.o \
	prop_cidrops.o \
	prop_inet6.o \
	prop_macaddr.o \
	prop_macaddr8.o \
	prop_tsquery.o \
	prop_tsvector.o \
	prop_tsvector2.o \
	prop_tsquerylaw.o \
	prop_tsrank.o \
	prop_date.o \
	prop_dateparts.o \
	prop_datefmt.o \
	prop_datecast.o \
	prop_datetrunc.o \
	prop_timeops.o \
	prop_timeextract.o \
	prop_timestamptz.o \
	prop_tstzarith.o \
	prop_tzconv.o \
	prop_interval.o \
	prop_intervalparts.o \
	prop_intervalcmp.o \
	prop_intervalio.o \
	prop_tochar.o \
	prop_toformat.o \
	prop_scalarfn.o \
	prop_hashfn.o \
	prop_hashfns2.o \
	prop_pglsn.o \
	prop_width_bucket.o \
	prop_genseries.o \
	prop_genseries2.o \
	prop_castchain.o \
	prop_concatop.o \
	prop_coerce.o \
	prop_collate.o \
	prop_textord.o \
	prop_jsonvsjsonb.o \
	prop_geopoint.o \
	prop_geoseg.o \
	prop_boxops.o \
	prop_xmlfmt.o \
	prop_aggregate.o \
	prop_aggstrategies.o \
	prop_aggorder.o \
	prop_aggdistinct.o \
	prop_aggdedup.o \
	prop_boolagg.o \
	prop_minmaxagg.o \
	prop_ordagg.o \
	prop_percentilefn.o \
	prop_filteragg.o \
	prop_countfilter.o \
	prop_stringagg.o \
	prop_strconcat.o \
	prop_jsonagg.o \
	prop_jsonaggord.o \
	prop_jsonobjectagg.o \
	prop_window.o \
	prop_windowframes.o \
	prop_windowrank.o \
	prop_windowexcl.o \
	prop_windowagg.o \
	prop_windownav.o \
	prop_windowdistinct.o \
	prop_grouphaving.o \
	prop_groupingsets.o \
	prop_groupsets2.o \
	prop_grouphash.o \
	prop_havingclause.o \
	prop_rollup2.o \
	prop_partprune.o \
	prop_nbtree.o \
	prop_indexscan.o \
	prop_indexdiff.o \
	prop_joinmethods.o \
	prop_outerjoin.o \
	prop_joincard.o \
	prop_selfjoin.o \
	prop_jointypes.o \
	prop_antisemijoin.o \
	prop_setops.o \
	prop_setopstrat.o \
	prop_exceptintersect.o \
	prop_unionall.o \
	prop_subquery.o \
	prop_subqscalar.o \
	prop_corrsubq.o \
	prop_cte.o \
	prop_recursivecte.o \
	prop_dmlcte.o \
	prop_lateral.o \
	prop_valuesrows.o \
	prop_srf.o \
	prop_unnestjoin.o \
	prop_distinct.o \
	prop_distincton.o \
	prop_sortlimit.o \
	prop_sortmerge.o \
	prop_orderbynulls.o \
	prop_fetchties.o \
	prop_pagination.o \
	prop_predpushdown.o \
	prop_spillpaths.o \
	prop_tablesample.o \
	prop_rowcompare.o \
	prop_mvcc.o \
	prop_savepoint.o \
	prop_returning.o \
	prop_upsert.o \
	prop_mergecmd.o \
	prop_updatesem.o \
	prop_defaults.o \
	prop_checkconstraint.o \
	prop_foreignkey.o \
	prop_views.o \
	prop_countsem.o \
	prop_hstore.o \
	prop_hstore2.o \
	prop_ltree.o \
	prop_ltree2.o \
	prop_citext.o \
	prop_citext2.o \
	prop_cube.o \
	prop_cubedist.o

PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS_INTERNAL += -L$(top_builddir)/src/fe_utils -lpgfeutils $(libpq_pgport)
# libhegel's C engine.  Its include/lib dirs reach us through --with-includes
# / --with-libraries (CPPFLAGS / LDFLAGS); here we just name the library.
PG_LIBS_INTERNAL += -lhegel_c

NO_INSTALL = 1
TAP_TESTS = 1

subdir = src/test/prop
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk

# libhegel is usually installed outside the system loader path (reached via
# --with-libraries), so record each configured extra library directory in
# pg_prop's rpath; otherwise the dynamic loader cannot find libhegel_c.so at
# run time.  Must come after Makefile.global, which defines LIBRARY_DIRS.
# Harmless when LIBRARY_DIRS is empty.
LDFLAGS += $(foreach dir,$(LIBRARY_DIRS),-Wl,-rpath,$(dir))

pg_prop$(X): | submake-libpgfeutils
check: pg_prop$(X)
