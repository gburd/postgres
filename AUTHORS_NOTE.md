# A Note to the Original Authors

Hi Heikki and everyone who worked on Zedstore,

I wanted to reach out and let you know what's happened with your work.

## What is Orvos?

**Orvos is Zedstore, renamed.** That's it—same architecture, same code, just a different name. The columnar table access method you designed and built is still here, with all its clever ideas intact: the TID B-tree for visibility, per-column B-trees for data, delta updates, UNDO logging, Simple8b compression, and everything else.

## Why the rename?

Honestly, I don't know the full history. When I found this code, it was already called "orvos" in the codebase but still had "zedstore" references scattered throughout (the WAL resource manager was still RM_ZEDSTORE_ID, function prefixes were mixed, etc.). It seems like someone started a rename at some point but never finished it. I completed that work, standardizing on "orvos" throughout.

If you'd prefer the original name back, or if there's context I'm missing, I'm happy to hear it.

## What I've been doing

I found this code dormant in a PostgreSQL fork and spent time bringing it up to modern PostgreSQL 19 standards:

- **Build system integration** – Added it back to the Makefile/meson build
- **API updates** – Fixed ~50 compilation errors from PostgreSQL API drift (TableAM changes, storage API modernization, etc.)
- **C90 compliance** – Fixed all declaration-after-statement warnings
- **Bug fixes** – Fixed a critical VACUUM crash, documented a known UNDO materialization bug
- **Testing** – Created comprehensive regression tests and coverage analysis

The core design is untouched. Your architecture is solid.

## Why I'm doing this

I find columnar storage fascinating, and your implementation is genuinely impressive—especially the delta UPDATE optimization and the UNDO-based visibility system. It deserves to work with current PostgreSQL.

I'm not claiming this as my own work. The hard thinking and clever design are all yours. I'm just the maintenance person making sure it still compiles and runs.

## Current status

- ✅ Builds cleanly with 0 errors, 0 warnings
- ✅ All CRUD operations work
- ✅ Indexes work (btree, sequential scans)
- ✅ MVCC and transactions work
- ✅ Regression tests passing
- ⚠️ One known bug: VACUUM crashes after large UPDATEs (materialization bug in `ov_materialize_delta_columns()`)

## Thank you

Thanks for building something interesting and teaching those of us who read code like this. If you have questions, corrections, or want to be involved, please reach out.

Best regards,
Greg Burd

---

**Note**: This work is on the `orvos` branch in my PostgreSQL fork. All commit history and authorship from the original Zedstore work is preserved.
