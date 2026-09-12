# Pooled starvation: the message budget is NECESSARY but NOT SUFFICIENT — fairness is a second, structural bug

Date: 2026-09-12. Agent de68f552 (local repro, cassert, multithreaded pooled). Ran out of turns
mid-measurement; did NOT commit (its full working tree is preserved at
`.ec2/orphaned-wip/starvation-budget-plus-auxfiber-de68f552.patch`, 453 lines). This records what it
proved so the next attempt starts from evidence, not from the (now-disproven) assumption that the
budget alone fixes starvation.

## What the agent got right and validated
1. **The WIP's real defect was the TPS pg_unreachable, not a missing pgstat attach** (see
   POOLED_SESSION_STARVATION.md's correction section). It correctly **gated the budget to the pooled
   path only** in `PgSessionRunProtocolSchedulerUntilBoundary`:
   `message_budget = PgRuntimeIsPooledProtocol(CurrentPgRuntime) ? pooled_protocol_carrier_message_budget : 0;`
   -- so thread-per-session can no longer produce PG_STEP_YIELD_BUDGET and its pg_unreachable is
   genuinely unreachable. This is the correct semantic and should be kept.
2. **The budget fires** -- counters show budget_yields climbing (2729, 14419) with queue_waits
   climbing alongside. The mechanism works: busy sessions DO yield and re-enter the queue.

## The blocking finding: yielding does NOT make it fair (a SECOND bug)
With the budget ON (budget=5, carriers=2, **c=8**), per-client transaction counts over a fixed run:

```
client 0:      1      client 4:  25209
client 1:      1      client 2:  33501
client 3:      1
client 5:      1
client 6:      1
client 7:      1
```
Six of eight clients completed **exactly one** transaction; two clients monopolized. In another
config the two winners ran ~11.9 MILLION protocol events each while the six losers sat at 1. **The
budget makes busy sessions yield, but the runnable-queue re-lease keeps handing the carrier back to
the SAME small set of sessions** -- so oversubscribed sessions still starve. Starvation is not
merely "a busy session never yields" (the budget fixes that); it is also **"the lease is not fair."**

The agent's own load-bearing comment (kept in the WIP) explains why the naive version self-defeats:
a carrier that re-checks the runnable queue first would immediately re-lease the very backend it just
released. The WIP checks the dispatch queue (brand-new connections) BEFORE the runnable queue to
avoid that -- but that only rotates NEW connections in ahead of yielders; among already-attached
oversubscribed sessions, the runnable-queue order still lets a couple monopolize. The round-robin is
not actually round-robin under this load.

## What the next attempt must do (the real fix)
The budget is step 1 (keep it, pooled-gated). Step 2 is **fairness in the lease/re-enqueue**:
- A yielded session must go to the **TAIL** and be **served strictly after** every other runnable
  session gets a turn -- true FIFO round-robin across the runnable_queue, with no path by which the
  just-yielded session is re-leased ahead of its siblings.
- Verify `PgRuntimeProtocolSchedulerPopRunnable` (pop head) + `PgCarrierYieldRunnableOnBudget` (push
  tail) actually give FIFO. The data says they do not in practice under c>carriers -- find why
  (candidate: multiple carriers + the dispatch-before-runnable ordering + wake timing let two
  sessions ping-pong between the two carriers while the other six never get popped). Instrument which
  backend each carrier leases each cycle.
- Success criterion is the c=8/carriers=2 (and c=64/carriers=32) per-client spread: every client
  should get roughly `total_tps / clients`, not a 33501-vs-1 split. Only then re-run the read `-S`
  scaling sweep vs fork -- fairness is a precondition for the scaling win, not the same thing.

## Scope / collision note
The agent's working tree ALSO contained (out of its brief): a correct proc.c aux-fiber
`sem_fiber_backed` fix (the sibling agent 0127e37b owns that -- do not double-land) and an unrelated
walwriter.c PG_XTC_WALWRITER_TEST_EXIT test hook (a supervision self-heal TAP hook, unrelated to
starvation). The preserved patch contains all three; the next starvation attempt should extract ONLY
the budget + fairness parts (postgres.c budget gate, launch_backend.c queue ordering,
backend_runtime_backend.c PgCarrierYieldRunnableOnBudget/PopRunnable, globals.c, guc_parameters.dat,
backend_runtime.h, miscadmin.h) and leave proc.c/walwriter.c to their owners.

## Honest status
The starvation fix is NOT done. The budget (necessary) works and is correctly pooled-gated; the
fairness of the lease (also necessary) is a distinct unsolved bug. So pooled still cannot be claimed
to scale past the carrier count -- the c>carriers per-client spread proves it still starves. This is
real, evidence-based progress (the problem is now two well-characterized bugs instead of one fuzzy
"plateau"), but it is not yet the win.
