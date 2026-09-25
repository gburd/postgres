#!/bin/bash
# contrib/pg_fts/bench/aws_launch.sh
# Launch an EC2 instance for pg_fts benchmarking in us-east-2.
# Adapted from Jim Mlodgenski's GREG_BENCHMARK_GUIDE.md sections 2-3, but for
# an FTS workload: normal (non-metal) memory-optimized instances, no hugepages.
#
# Usage: bash aws_launch.sh [instance-type]   (default: r7i.4xlarge)
set -euo pipefail

REGION=us-east-2
ITYPE="${1:-r7i.4xlarge}"
KEY=pgfts-bench
SG=pgfts-bench-sg
VOL_GB="${VOL_GB:-1000}"

# --- key pair (one-time) ---
if [ ! -f ~/.ssh/${KEY}.pem ]; then
    aws ec2 create-key-pair --key-name "$KEY" --key-type rsa \
        --query 'KeyMaterial' --output text --region "$REGION" > ~/.ssh/${KEY}.pem
    chmod 600 ~/.ssh/${KEY}.pem
fi

# --- security group (one-time) ---
SG_ID=$(aws ec2 describe-security-groups --group-names "$SG" --region "$REGION" \
        --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || true)
if [ -z "$SG_ID" ] || [ "$SG_ID" = "None" ]; then
    SG_ID=$(aws ec2 create-security-group --group-name "$SG" \
        --description "pg_fts benchmark SSH" --region "$REGION" \
        --query 'GroupId' --output text)
    MY_IP=$(curl -s https://checkip.amazonaws.com)
    aws ec2 authorize-security-group-ingress --group-id "$SG_ID" \
        --protocol tcp --port 22 --cidr "${MY_IP}/32" --region "$REGION"
fi
echo "Security group: $SG_ID"

# --- latest Amazon Linux 2023 AMI ---
# --- latest official Fedora Cloud Base AMI (Fedora's AWS account 125523088429) ---
# Prefer a numbered stable release (e.g. -44-), not ELN/Rawhide.
AMI=$(aws ec2 describe-images --owners 125523088429 \
    --filters "Name=name,Values=Fedora-Cloud-Base-AmazonEC2.x86_64-*" \
              "Name=state,Values=available" \
              "Name=architecture,Values=x86_64" \
    --query 'reverse(sort_by(Images, &CreationDate))[].[Name,ImageId]' \
    --output text --region "$REGION" \
    | grep -E 'x86_64-[0-9]+-' | head -1 | awk '{print $2}')
if [ -z "$AMI" ] || [ "$AMI" = "None" ]; then
    echo "No numbered Fedora Cloud AMI found in $REGION." >&2
    exit 1
fi
echo "AMI (Fedora Cloud): $AMI"

# --- launch ---
INSTANCE_ID=$(aws ec2 run-instances --image-id "$AMI" --instance-type "$ITYPE" \
    --key-name "$KEY" --security-group-ids "$SG_ID" \
    --block-device-mappings "[{\"DeviceName\":\"/dev/xvdb\",\"Ebs\":{\"VolumeSize\":${VOL_GB},\"VolumeType\":\"gp3\",\"Iops\":16000,\"Throughput\":1000,\"DeleteOnTermination\":true}}]" \
    --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=pgfts-bench}]' \
    --region "$REGION" --query 'Instances[0].InstanceId' --output text)
echo "Instance: $INSTANCE_ID"

aws ec2 wait instance-running --instance-ids "$INSTANCE_ID" --region "$REGION"
PUBLIC_IP=$(aws ec2 describe-instances --instance-ids "$INSTANCE_ID" --region "$REGION" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
echo "IP: $PUBLIC_IP"
echo
echo "Wait for SSH, then:  ssh -i ~/.ssh/${KEY}.pem fedora@${PUBLIC_IP}"
echo "TERMINATE WHEN DONE:  aws ec2 terminate-instances --instance-ids $INSTANCE_ID --region $REGION"
echo "$INSTANCE_ID $PUBLIC_IP" > /tmp/pgfts_bench_instance
