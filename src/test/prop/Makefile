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
	prop_substring.o

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
