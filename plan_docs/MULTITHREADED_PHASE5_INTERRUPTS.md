# Phase 5 Logical Interrupt Notes

This note records the current Phase 5 interrupt boundary and the validation
decision around hot-standby recovery conflicts.

## Current Boundary

- Recovery-conflict interrupts now route through the logical backend interrupt
  mailbox while preserving existing process-mode behavior.
- Process-mode Unix signal delivery remains the external transport, but the
  backend-visible state is represented by logical interrupt state.
- `CHECK_FOR_INTERRUPTS()` remains the common service point for process and
  future threaded runtimes.

## Recovery-Conflict Fixture Decision

A dedicated hot-standby recovery-conflict fixture was not built during Phase 5.
That does not mean Phase 5 was left incomplete. The phase was considered
complete after tracing the existing recovery-conflict delivery path and
confirming that it now passes through the logical interrupt machinery.

This is a deliberate phase decision after working through the code, not an
open implementation TODO. The recovery-conflict path already enters the backend
through normal process-mode signalling, records logical recovery-conflict
pending state, and is serviced by `CHECK_FOR_INTERRUPTS()`. Phase 5 changed the
backend-visible state and service path; it did not need a new standby cluster
fixture to finish that implementation step.

Treat the missing fixture as a validation deferral, not an implementation gap.
Gate B or a focused follow-up can add a dedicated hot-standby fixture if we want
direct regression coverage for that path, but Phase 6 did not need to wait on
that fixture before proceeding.
