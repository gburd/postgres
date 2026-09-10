Evidence for a THIRD, distinct, pre-existing bug found while validating Fix A/B
under gmake check-threaded-pooled: NOT fixed, NOT chased, out of scope for this
task (reloptions.c + xml2). Recorded here so it is not lost.

Captured from: gmake -C src/test/regress check-threaded-pooled
(pg-autoconf tree, USE_XTC_CARRIER=1, multithreaded=on pooled_protocol_carriers=4,
cassert build), AFTER the guc.c fix (see guc_c_sibling_bug_before_crash.log) was
applied. The run got substantially further (reached the plpgsql/domain/rangefuncs
parallel group) before crashing here:

---

2026-09-10 03:44:47.363 UTC postmaster[101935] pg_regress/plpgsql STATEMENT:  select x, pg_typeof(x), y, pg_typeof(y)
	  from f1(11, array[1, 2.2], 42, 34.5);
TRAP: failed Assert("parent->firstchild == context"), File: "mcxt.c", Line: 694, PID: 101935
2026-09-10 03:44:47.365 UTC postmaster[101935] pg_regress/plpgsql ERROR:  RETURN cannot have a parameter in function with OUT parameters at character 74
...
postgres: ec2-user regression [local] (ExceptionalCondition+0x55)[0xbadab5]
postgres: ec2-user regression [local] (MemoryContextSetParent+0x107)[0xc22387]
postgres: ec2-user regression [local] (MemoryContextDelete+0xd1)[0xc22471]
postgres: ec2-user regression [local] (AtEOXact_RI+0x92)[0xb2bee2]
postgres: ec2-user regression [local] [0x64403f]
postgres: ec2-user regression [local] [0x646185]
postgres: ec2-user regression [local] (CommitTransactionCommand+0xd)[0x64624d]
postgres: ec2-user regression [local] [0xa2348f]
postgres: ec2-user regression [local] [0xa2c771]
postgres: ec2-user regression [local] (PgSessionRunProtocolSchedulerUntilBoundary+0x1545)[0xa32f95]
postgres: ec2-user regression [local] [0x91dbf4]
postgres: ec2-user regression [local] [0x91ec81]
postgres: ec2-user regression [local] [0x91f256]

---

Analysis (not fixed -- recorded for the record):

- Crash site: AtEOXact_RI() -> MemoryContextDelete(dead->scratch_cxt) ->
  MemoryContextSetParent(), asserting the context being unlinked is actually
  its parent's firstchild.  This is memory-context lifecycle corruption in the
  RI (referential-integrity trigger) fast-path metadata cleanup
  (src/backend/utils/adt/ri_triggers.c, ri_fpmeta_dead_list /
  InvalidateConstraintCacheCallBack()), NOT the InterruptHoldoffCount class of
  bug that Fix A / the guc.c sibling fix address.
- Completely unrelated to reloptions.c, xml.c, xml.h, xpath.c, xslt_proc.c, or
  guc.c -- none of those files are on this crash's call path.
- The regression tests active at the moment of the crash (plpgsql, domain,
  rangefuncs, temp, alter_table, sequence, ...) all exercise domain constraints
  and/or foreign-key-adjacent DDL in the same parallel group; the reloptions/
  guc/xml/xml2 tests this task validates do NOT touch domains or foreign keys
  and do not reach this code path (confirmed: a schedule of
  test_setup+reloptions+guc+xml, and xml2's own contrib check, both ran clean
  to completion under the identical threaded_pooled.conf -- see
  .ec2/relx-20260910/fixA/final_autoconf_threaded_pooled_no_crash.log and
  .ec2/relx-20260910/fixB/hammer_final.log).
- This blocks the FULL gmake check-threaded-pooled parallel_schedule from
  completing, but does not block validation of Fix A or Fix B specifically.
  Per the task's explicit instruction ("if it wedges... that is the known bug,
  NOT your fixes... note what you see and do not chase it"), this is recorded
  as a distinct open item for whichever phase/gate owns RI-trigger threaded
  memory-context lifecycle, not chased further here.
