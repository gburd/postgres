#!/usr/bin/env bash
# run-suite-ec2.sh -- reusable EC2 harness for running the xtc branch test suites
# on real NUMA hardware with ephemeral NVMe (avoids the meson-on-btrfs dev-env
# TAP/smoke harness hang).  ALWAYS terminate when done.
#
# Subcommands:
#   launch      -- start the instance, wait for SSH
#   bootstrap   -- mount NVMe, install nix + toolchain
#   sync        -- push the branch working tree (incl. flake.lock) to the box
#   build       -- nix build the branch (pins libxtc via flake.lock)
#   suites ARGS -- run the requested gmake/meson suites
#   pull        -- copy results/logs back to /tmp/xtc-ec2-results
#   terminate   -- terminate the instance + confirm gone
#   ssh         -- open an interactive shell on the box
#
# Env overrides: INSTANCE_TYPE AMI KEY SG PEM PGDIR
set -euo pipefail
export AWS_PROFILE="${AWS_PROFILE:-numa}"
export AWS_REGION="${AWS_REGION:-us-east-2}"
export AWS_DEFAULT_REGION="$AWS_REGION"

INSTANCE_TYPE="${INSTANCE_TYPE:-m6i.metal}"
AMI="${AMI:-ami-0ed003ef4d5dc91e1}"
KEY="${KEY:-xtc-suite}"
SG="${SG:-sg-039d75a8e32268efb}"
PEM="${PEM:-$HOME/.ssh/xtc-suite.pem}"
PGDIR="${PGDIR:-/home/gburd/ws/postgres/xtc}"
TAGN="xtc-suite"
STATE="/tmp/xtc-ec2-state.env"
SSHOPTS="-i $PEM -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=15 -o ServerAliveInterval=30 -o IdentitiesOnly=yes"

log(){ echo "[$(date +%H:%M:%S)] $*" >&2; }
have_state(){ [ -f "$STATE" ] && . "$STATE" && [ -n "${IP:-}" ]; }
rsh(){ have_state; ssh $SSHOPTS ec2-user@"$IP" "$@"; }

launch(){
  log "launching $INSTANCE_TYPE ($AMI) in $AWS_REGION"
  local iid
  iid=$(aws ec2 run-instances --image-id "$AMI" --instance-type "$INSTANCE_TYPE" \
    --key-name "$KEY" --security-group-ids "$SG" \
    --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":300,"VolumeType":"gp3","Iops":16000,"Throughput":1000,"DeleteOnTermination":true}}]' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$TAGN}]" \
    --query 'Instances[0].InstanceId' --output text)
  log "instance $iid; waiting for running"
  aws ec2 wait instance-running --instance-ids "$iid"
  local ip
  ip=$(aws ec2 describe-instances --instance-ids "$iid" \
       --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
  printf 'IID=%s\nIP=%s\n' "$iid" "$ip" > "$STATE"
  log "instance $iid @ $ip; waiting for SSH"
  local i
  for i in $(seq 1 45); do
    ssh $SSHOPTS ec2-user@"$ip" true 2>/dev/null && { log "SSH up"; return 0; }
    sleep 10
  done
  log "SSH did not come up"; return 1
}

bootstrap(){
  log "mount NVMe + install nix/toolchain"
  rsh 'sudo bash -s' <<'R'
set -e
dev=$(lsblk -bdno NAME,SIZE,TYPE | awk '$3=="disk"{print $1,$2}' | sort -k2 -n | tail -1 | awk '{print $1}')
if ! mountpoint -q /mnt/nvme 2>/dev/null; then
  mkfs.xfs -f "/dev/$dev" >/dev/null 2>&1 || true
  mkdir -p /mnt/nvme && mount "/dev/$dev" /mnt/nvme && chmod 1777 /mnt/nvme
fi
dnf -y groupinstall "Development Tools" >/dev/null 2>&1 || true
dnf -y install git tar gzip xz which >/dev/null 2>&1 || true
lscpu | grep -iE 'NUMA node\(s\)|^CPU\(s\):' | sed 's/^/  /'
df -h /mnt/nvme | tail -1 | sed 's/^/  nvme: /'
R
  log "install nix (multi-user) if absent"
  rsh 'command -v nix >/dev/null 2>&1 || sh <(curl -L https://nixos.org/nix/install) --daemon --yes >/tmp/nixinstall.log 2>&1; nix --version 2>/dev/null || echo NIX_INSTALL_MAYBE_NEEDS_RELOGIN'
}

sync(){
  log "sync working tree (HEAD + flake.lock) to /mnt/nvme/src/pg"
  rsh 'rm -rf /mnt/nvme/src/pg && mkdir -p /mnt/nvme/src/pg'
  ( cd "$PGDIR" && git archive --format=tar HEAD ) | rsh 'tar -xf - -C /mnt/nvme/src/pg'
  # flake.lock is tracked, so it rides along; confirm the libxtc pin
  rsh 'grep -A1 "\"rev\"" /mnt/nvme/src/pg/flake.lock | grep -m1 rev || true'
  log "synced"
}

# build/suites are intentionally invoked with explicit remote commands from the
# caller once nix is confirmed; kept thin so the caller can iterate.
rawssh(){ have_state; ssh $SSHOPTS ec2-user@"$IP" "$@"; }

pull(){
  have_state
  mkdir -p /tmp/xtc-ec2-results
  log "pulling results to /tmp/xtc-ec2-results"
  scp $SSHOPTS -r ec2-user@"$IP":/mnt/nvme/src/pg/build*/meson-logs /tmp/xtc-ec2-results/ 2>/dev/null || true
  scp $SSHOPTS ec2-user@"$IP":'/mnt/nvme/*.log' /tmp/xtc-ec2-results/ 2>/dev/null || true
}

terminate(){
  have_state || { log "no state; nothing to terminate"; return 0; }
  log "terminating $IID"
  aws ec2 terminate-instances --instance-ids "$IID" --query 'TerminatingInstances[0].CurrentState.Name' --output text
  aws ec2 wait instance-terminated --instance-ids "$IID" && log "terminated $IID"
  rm -f "$STATE"
}

case "${1:-}" in
  launch) launch ;;
  bootstrap) bootstrap ;;
  sync) sync ;;
  ssh) shift; rawssh "$@" ;;
  pull) pull ;;
  terminate) terminate ;;
  *) echo "usage: $0 {launch|bootstrap|sync|ssh <cmd>|pull|terminate}" >&2; exit 2 ;;
esac
