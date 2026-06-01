// guc_accessor_poc.cocci — proof-of-concept Coccinelle semantic patch.
//
// PURPOSE / CONVENTION DEMO
// -------------------------
// This is the worked example for the project's "ship mechanical transforms as
// re-runnable .cocci" convention (see src/tools/cocci/run-cocci.py and the
// threading docs).  It demonstrates the single most-cited mechanical sweep in
// the threaded conversion: the GUC system stores each of its ~350 settings in
// a process-global variable that callers read directly (`a_global`).  Under
// threaded mode that storage moves behind a function-call API
// (GetGUC<Type>()/SetGUC<Type>(); xtc: xtc_cfg), so every direct read must
// become an accessor call.
//
// That rewrite is purely mechanical and high-volume -- exactly what a semantic
// patch is for.  Re-running this .cocci after a merge from origin/master picks
// up any new call sites for free, instead of re-doing the edit by hand.
//
// STATUS: ILLUSTRATIVE.  The GetGUCInt()/GetGUCBool() accessor API is defined
// in F3, not yet on this branch, so this patch is shipped (not applied) as the
// convention's reference.  `run-cocci.py` runs it in dry-run mode to show the
// matched diff; do NOT `--apply` it until the F3 accessor layer lands.  When it
// does, extend the name lists (or generate them from guc_tables.c) and apply
// tree-wide.
//
// Each rule matches a direct *read* of the named GUC global (an identifier used
// as an expression) and rewrites it to the typed accessor.  Declarations and
// assignments are intentionally left alone -- the storage definition in
// guc_tables.c and the GUC machinery's own writes are handled separately.

// --- int-typed GUC read: log_min_duration_statement -> GetGUCInt(...) -------
@@
identifier g =~ "^log_min_duration_statement$";
@@
- g
+ GetGUCInt(GUC_log_min_duration_statement)

// --- bool-typed GUC read: log_duration -> GetGUCBool(...) -------------------
@@
identifier g =~ "^log_duration$";
@@
- g
+ GetGUCBool(GUC_log_duration)
