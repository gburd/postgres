#!/usr/bin/env bash
# mtpg_ec2_ab_provision.sh -- provision a 2-instance A/B benchmark cluster on EC2:
#   SUT     (system under test)  -- runs postgres (process + threaded lanes)
#   LOADGEN (load driver)        -- runs pgbench over the PRIVATE network
# Both in the SAME subnet + a cluster PLACEMENT GROUP for low-latency, low-jitter
# private networking, so client load never steals CPU from the server and the
# network path is as clean as EC2 allows.
#
# This ONLY provisions + bootstraps.  It prints the exact SUT commands to build
# PG and run src/tools/benchmark/mtpg_remote_bench.sh.  Terminate with the
# printed teardown command when done (bench boxes cost money).
#
# Requires: aws CLI (profile in $PROFILE), an existing key pair + SG that allows
# SSH from you AND all-traffic within the SG (so LOADGEN can reach SUT:5439).
set -uo pipefail
PROFILE="${PROFILE:-numa}"
REGION="${REGION:-us-east-1}"
KEY="${KEY:-xtc-p17}"
SG="${SG:?set SG=sg-... (must allow SSH from you + intra-SG all-traffic)}"
SUBNET="${SUBNET:?set SUBNET=subnet-... (an AZ with m6id.8xlarge capacity, e.g. us-east-1c)}"
AMI="${AMI:-ami-0fd6240f599091088}"        # AL2023 x86_64
ITYPE="${ITYPE:-m6id.8xlarge}"             # 32 vCPU, NVMe
PG_KEY_PEM="${PG_KEY_PEM:-$HOME/.ssh/$KEY.pem}"
PGROUP="${PGROUP:-xtc-ab-pg}"

aws() { command aws --profile "$PROFILE" --region "$REGION" "$@"; }

# cluster placement group for tight network locality (create if missing)
aws ec2 describe-placement-groups --group-names "$PGROUP" >/dev/null 2>&1 || \
  aws ec2 create-placement-group --group-name "$PGROUP" --strategy cluster >/dev/null
echo "placement group: $PGROUP"

# ensure the SG allows intra-SG all traffic (so LOADGEN dials SUT:5439 privately)
aws ec2 authorize-security-group-ingress --group-id "$SG" \
  --protocol -1 --source-group "$SG" >/dev/null 2>&1 || true

launch() { # $1=Name  -> prints instance id
  aws ec2 run-instances --image-id "$AMI" --instance-type "$ITYPE" \
    --key-name "$KEY" --security-group-ids "$SG" --subnet-id "$SUBNET" \
    --placement "GroupName=$PGROUP" --associate-public-ip-address \
    --block-device-mappings 'DeviceName=/dev/xvda,Ebs={VolumeSize=40,VolumeType=gp3}' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$1}]" \
    --query 'Instances[0].InstanceId' --output text
}

SUT=$(launch xtc-ab-sut)
LG=$(launch xtc-ab-loadgen)
echo "SUT=$SUT LOADGEN=$LG (waiting for running...)"
aws ec2 wait instance-running --instance-ids "$SUT" "$LG"

read SUT_PUB SUT_PRIV < <(aws ec2 describe-instances --instance-ids "$SUT" \
  --query 'Reservations[0].Instances[0].[PublicIpAddress,PrivateIpAddress]' --output text)
read LG_PUB LG_PRIV < <(aws ec2 describe-instances --instance-ids "$LG" \
  --query 'Reservations[0].Instances[0].[PublicIpAddress,PrivateIpAddress]' --output text)

cat > /tmp/xtc_ab_cluster.env <<EOF
PROFILE=$PROFILE
REGION=$REGION
KEY=$KEY
PG_KEY_PEM=$PG_KEY_PEM
SUT_IID=$SUT
LG_IID=$LG
SUT_PUB=$SUT_PUB
SUT_PRIV=$SUT_PRIV
LG_PUB=$LG_PUB
LG_PRIV=$LG_PRIV
SSHOPTS="-i $PG_KEY_PEM -o IdentitiesOnly=yes -o IdentityAgent=none -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=25"
EOF
echo "wrote /tmp/xtc_ab_cluster.env"
echo
echo "NEXT:"
echo "  1) bootstrap BOTH: scp xtc_val2_bootstrap.sh; run on SUT (full build) + LOADGEN (only needs pgbench client)."
echo "  2) On the SUT, build PG (release+FP), install to inst/."
echo "  3) On the SUT, run the external-driver benchmark pointed at the LOADGEN:"
echo "       LOADGEN=ec2-user@$LG_PRIV SUT_IP=$SUT_PRIV \\"
echo "       PGBENCH=/mnt/nvme/work/pg/inst/usr/local/pgsql/bin/pgbench \\"
echo "       CARRIERS='auto 16 32 64' CLIENTS='16 32 64 128' WORKLOADS='tpcb select update' \\"
echo "       DURATION=120 WARMUP=30 SCALE=100 \\"
echo "       bash src/tools/benchmark/mtpg_remote_bench.sh"
echo "     (LOADGEN needs the same pgbench binary + libpq on its PATH/LD_LIBRARY_PATH.)"
echo
echo "TEARDOWN (do when done -- these cost money):"
echo "  aws --profile $PROFILE --region $REGION ec2 terminate-instances --instance-ids $SUT $LG"
