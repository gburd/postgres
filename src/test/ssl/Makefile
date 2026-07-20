#-------------------------------------------------------------------------
#
# Makefile for src/test/ssl
#
# Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/test/ssl/Makefile
#
#-------------------------------------------------------------------------

EXTRA_INSTALL = contrib/sslinfo

subdir = src/test/ssl
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

export OPENSSL with_ssl

# The sslfiles targets are separated into their own file due to interactions
# with settings in Makefile.global.
.PHONY: sslfiles sslfiles-clean
sslfiles sslfiles-clean:
	$(MAKE) -f $(srcdir)/sslfiles.mk $@

clean distclean:
	rm -rf tmp_check
	$(MAKE) -f $(srcdir)/sslfiles.mk $@

# Doesn't depend on sslfiles because we don't rebuild them by default
check:
	$(prove_check)

installcheck:
	$(prove_installcheck)

# Threaded ssl harness: run the ssl TAP suite under the multithreaded carrier
# (threaded_ssl.conf via TEMP_CONFIG) so server-side TLS runs on backend fibers.
#
# This is the regression coverage the be_tls_* -> xtc_tls_* TLS-backend swap
# needs before it can land (cert verification, client-cert peer DN, CRL reject,
# SCRAM channel binding tls-server-end-point, and the ssl_sni gate all on the
# carrier path).  003_sslinfo is deliberately excluded: the bundled sslinfo
# contrib module is process-backend-only (no threaded backend-model metadata),
# so the threaded runtime correctly refuses to load sslinfo.so.  Marking
# sslinfo threaded-safe is Phase 16 / Gate E2-Extensions work, not TLS work.
# 001/002/004 cover the full server-side TLS security surface on the carrier.
.PHONY: check-threaded-ssl
check-threaded-ssl: export TEMP_CONFIG=$(abs_top_srcdir)/src/test/ssl/threaded_ssl.conf
check-threaded-ssl: export PROVE_TESTS=t/001_ssltests.pl t/002_scram.pl t/004_sni.pl
check-threaded-ssl:
	@case " $(PG_TEST_EXTRA) " in \
	  *" ssl "*) : ;; \
	  *) echo "check-threaded-ssl: PG_TEST_EXTRA must include 'ssl' or the ssl tests skip silently (false green). Re-run: make check-threaded-ssl PG_TEST_EXTRA=ssl" >&2; exit 1 ;; \
	esac
	$(prove_check)
