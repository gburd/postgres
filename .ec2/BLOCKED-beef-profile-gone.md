# EC2 validation BLOCKED: the beef AWS profile vanished mid-session (2026-08-24)

## Blocker
Launching the rebase+libxtc-v1.35.2 validation box FAILED:
  aws: [ERROR]: The config profile (beef) could not be found
~/.aws/{config,credentials} were rewritten 2026-08-23 20:26 and now contain ONLY
a profile named `chiuso` (account 187887018457) -- the `beef` profile
(account 840154381708) is GONE.  Earlier this entire session `--profile beef`
worked repeatedly, so it was removed out from under the session.

I did NOT substitute `chiuso` -- wrong account, not the sanctioned beef account;
all this project's EC2 work is beef-only.

## Resource possibly left in beef during the failed launch (CLEAN UP when beef is back)
While beef still worked, the launch attempt created:
  - key-pair: xtc-rbv-20260824-070848  (us-east-1)
  - security-group: named xtc-rbv-20260824-070848 (GroupId not captured; the SG
    create MAY have succeeded before the profile vanished)
The run-instances FAILED (profile gone by then) -> NO INSTANCE launched -> no
cost-bearing compute.  Only a possibly-dangling key + SG in us-east-1.
CLEANUP when beef returns:
  aws ec2 delete-key-pair --profile beef --region us-east-1 --key-name xtc-rbv-20260824-070848
  aws ec2 describe-security-groups --profile beef --region us-east-1 --filters Name=group-name,Values=xtc-rbv-20260824-070848 --query 'SecurityGroups[].GroupId' --output text  # then delete-security-group if present
Also re-verify all 5 regions clean (us-east-1/2, us-west-1/2, eu-west-1) since I
could not run the standard post-run teardown sweep without the profile.

## State of the actual work (UNAFFECTED -- all local/git, nothing lost)
- Rebase onto origin/master + libxtc v1.35.2 bump: DONE on branch
  xtc-rebase-candidate (HEAD 2276e70edc), all conflict resolutions
  compile-verified locally.  origin/xtc UNCHANGED (8a42285eec) -- correct, since
  validation is not yet green.
- Validation script ready: /tmp/rebaseval.sh (build libxtc v1.35.2 + PG cassert +
  process regress + threaded test_backend_runtime + threaded smoke).  Tarballs
  ready: /tmp/pgxtc.tar.gz (candidate), /tmp/libxtc.tar.gz (v1.35.2 src).

## To resume once beef is restored
1. Clean up the possibly-dangling xtc-rbv key/SG (above) + 5-region sweep.
2. Launch a c7i.8xlarge (us-east-1, beef), ship the 3 files, run /tmp/rebaseval.sh.
3. If green (process regress 0-fail, test_backend_runtime 18/0, threaded smoke +
   clean stop): git push origin +xtc-rebase-candidate:xtc (force, approved).
4. If red: fix the flagged resolution, re-validate.  DO NOT force-push on red.
