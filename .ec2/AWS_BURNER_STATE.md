# AWS burner state (2026-09-11)

## ACTIVE: profile `lava`, account 769093516156
- **REGION MUST BE us-east-1** (or us-west-2 / eu-west-1 / ap-south-1).
- **us-east-2 is BLOCKED**: `PendingVerification` -- brand-new account, cannot RunInstances
  there yet ("normally resolved within minutes, allow up to 4 hours"). Read+key+SG work; only
  the launch is refused. Re-test us-east-2 later; until then do not use it.
- Verified end-to-end in us-east-1: describe, AMI lookup, create-key-pair,
  create-security-group, authorize-ingress, run-instances, terminate-instances,
  delete-security-group, delete-key-pair.
- AMIs (al2023 x86_64, newest at time of check):
  us-east-1 ami-0b5358cc8c5df0b02 | us-west-2 ami-0469a6bed63b8634c
  eu-west-1 ami-0b3ba1acb76a70451 | ap-south-1 ami-0f937e388a57b1f65
- Currently EMPTY of other owners' instances in us-east-2; re-check per region before
  terminating anything and NEVER touch an instance whose KeyName+Name tag are not yours.

## DEAD: profile `bene`, account 292759875395
- Expired Fri Sep 11 12:03:00 UTC 2026; now fully revoked
  (`InvalidClientTokenId` on sts, `AuthFailure` on ec2).
- **ORPHANED INSTANCE, cannot be terminated via API:**
  `i-0b46a091b23ab6ae7`, key+Name `xtc-pmsig2-20260909-163729`, c6i.4xlarge,
  us-east-2, launched 2026-09-09T20:39:23Z. Created by a sub-agent, so no .pem was ever
  written to ~/.ssh here -- it cannot be reached to shut down from inside either.
  **Needs out-of-band cleanup by the account owner.**
- Other owners on bene (never touched): fts-wand-*, solnix-*, pgtv-team-*,
  numa-bench/bcs-jakub, osv-*.

## Earlier dead profiles
mala (724081032357), chiuso -- both expired.
