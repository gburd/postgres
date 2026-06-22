# Regression test for the RECNO CAS fast-path eligibility gate.
#
# NOTE: This spec passes on BOTH pre-fix and post-fix builds.  It locks in
# the RECNO_TUPLE_LOCKED eligibility check in recno_tuple_update's CAS
# gate: a locker (recno_tuple_lock) sets RECNO_TUPLE_LOCKED on the tuple
# header, which causes any concurrent CAS writer's eligibility mask to
# fail and fall through to the exclusive/slow path, and the slow path
# takes LOCKTAG_TUPLE with wait=true.  If a future edit drops
# RECNO_TUPLE_LOCKED from that eligibility mask, CAS writers would bypass
# LOCKTAG_TUPLE entirely and lap the locker; this spec catches that
# regression.
#
# The actual fairness fix (conditional LOCKTAG_TUPLE gate added to the
# CAS path) targets a DIFFERENT vector: writer-vs-writer starvation via
# the EPQ reacquire loop in recno_tuple_lock under sustained CAS
# contention on a hot row.  Isolationtester cannot express that vector
# because it is a liveness bug that requires N>>2 sustained concurrent
# writers plus subxact UNDO churn.  The teeth for the writer-vs-writer
# vector live in an out-of-tree stress repro; see run_wedge.sh in the
# scratch tree used during development.
#
# CAS-fast-path prerequisites (must all hold to exercise the gate):
#   * new tuple same on-disk size as old (integer arithmetic keeps size)
#   * old tuple committed, not deleted / locked / uncommitted / spec
#   * old tuple has no overflow data
#   * old tuple already stamped with a version pointer (WS-PVS1: the
#     FIRST in-place UPDATE grows +8B for the trailing RelUndoRecPtr and
#     goes through the exclusive path; only subsequent same-column-size
#     UPDATEs stay on the CAS fast path)
#
# The setup performs one warm-up UPDATE so id=1 is verptr-stamped and the
# permutations below hit the CAS fast path.

setup
{
  CREATE TABLE recno_cas_lock (id int PRIMARY KEY, counter int) USING recno;
  INSERT INTO recno_cas_lock VALUES (1, 0);
  INSERT INTO recno_cas_lock VALUES (2, 0);
  -- warm-up: stamp the trailing verptr so subsequent same-size UPDATEs
  -- are CAS-fast-path-eligible.
  UPDATE recno_cas_lock SET counter = counter + 1 WHERE id = 1;
}

teardown
{
  DROP TABLE recno_cas_lock;
}

session locker
setup           { BEGIN; }
step locker_lock    { SELECT id, counter FROM recno_cas_lock WHERE id = 1 FOR NO KEY UPDATE; }
step locker_commit  { COMMIT; }
step locker_abort   { ROLLBACK; }

session w1
setup           { BEGIN; }
step w1_update      { UPDATE recno_cas_lock SET counter = counter + 1 WHERE id = 1; }
step w1_commit      { COMMIT; }

session w2
setup           { BEGIN; }
step w2_update      { UPDATE recno_cas_lock SET counter = counter + 1 WHERE id = 1; }
step w2_commit      { COMMIT; }

session reader
step reader_read    { SELECT id, counter FROM recno_cas_lock ORDER BY id; }

# Permutation 1: A single CAS writer must block behind a FOR NO KEY UPDATE
# locker on the same row.  Pre-fix the CAS writer bypasses LOCKTAG_TUPLE and
# completes without waiting; post-fix it blocks and only proceeds after the
# locker commits.
permutation locker_lock w1_update locker_commit w1_commit w2_commit reader_read

# Permutation 2: Two CAS writers behind the locker.  Both must queue.  After
# the locker commits, writers unblock in some order and the final counter
# reflects both increments applied on top of the warm-up + locker snapshot.
permutation locker_lock w1_update w2_update locker_commit w1_commit w2_commit reader_read

# Permutation 3: Same as Permutation 1 but the locker aborts instead of
# committing.  The CAS writer still must wait for the locker's txn to end
# before proceeding, then applies its update.
permutation locker_lock w1_update locker_abort w1_commit w2_commit reader_read
