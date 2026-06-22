# Copyright (c) 2022-2026, PostgreSQL Global Development Group

# used by frontend programs like pg_waldump
rmgr_desc_sources = files(
  'atmdesc.c',
  'brindesc.c',
  'clogdesc.c',
  'committsdesc.c',
  'dbasedesc.c',
  'fileopsdesc.c',
  'genericdesc.c',
  'gindesc.c',
  'gistdesc.c',
  'hashdesc.c',
  'heapdesc.c',
  'logicalmsgdesc.c',
  'mxactdesc.c',
  'nbtdesc.c',
  'relmapdesc.c',
  'relundodesc.c',
  'replorigindesc.c',
  'rmgrdesc_utils.c',
  'seqdesc.c',
  'smgrdesc.c',
  'spgdesc.c',
  'standbydesc.c',
  'tblspcdesc.c',
  'undodesc.c',
  'xactdesc.c',
  'xlogdesc.c',
)

# RECNO is always built on the undo branch (see USE_RECNO handling in the
# top-level meson.build), and rmgrlist.h registers recno_desc/recno_identify
# unconditionally, so recnodesc.c must always be compiled.
rmgr_desc_sources += files('recnodesc.c')

backend_sources += rmgr_desc_sources
