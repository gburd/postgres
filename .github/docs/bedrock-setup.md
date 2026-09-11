# AWS Bedrock Setup for AI Code Review

This guide explains how to use AWS Bedrock instead of the direct Anthropic API for AI code reviews.

## Why Use Bedrock?

- **AWS Credits:** Use existing AWS credits
- **Regional Availability:** Deploy in specific AWS regions
- **Compliance:** Meet specific compliance requirements
- **Integration:** Easier integration with AWS infrastructure
- **IAM Roles:** Use IAM roles instead of API keys when running on AWS

## Prerequisites

1. **AWS Account** with Bedrock access
2. **Bedrock Model Access** - Claude 3.5 Sonnet must be enabled
3. **IAM Permissions** for Bedrock API calls

## Step 1: Enable Bedrock Model Access

1. Log into AWS Console
2. Navigate to **Amazon Bedrock**
3. Go to **Model access** (left sidebar)
4. Click **Modify model access**
5. Find and enable: **Anthropic - Claude 3.5 Sonnet v2**
6. Click **Save changes**
7. Wait for status to show "Access granted" (~2-5 minutes)

## Step 2: Create IAM User for GitHub Actions

### Option A: IAM User with Access Keys (Recommended for GitHub Actions)

1. Go to **IAM Console**
2. Click **Users** → **Create user**
3. Username: `github-actions-bedrock`
4. Click **Next**

**Attach Policy:**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "bedrock:InvokeModel"
      ],
      "Resource": [
        "arn:aws:bedrock:*::foundation-model/anthropic.claude-3-5-sonnet-*"
      ]
    }
  ]
}
```

5. Click **Create policy** → **JSON** → Paste above
6. Name: `BedrockClaudeInvokeOnly`
7. Attach policy to user
8. Click **Create user**

**Create Access Keys:**
1. Click on the created user
2. Go to **Security credentials** tab
3. Click **Create access key**
4. Select: **Third-party service**
5. Click **Next** → **Create access key**
6. **Download** or copy:
   - Access key ID (starts with `AKIA...`)
   - Secret access key (only shown once!)

### Option B: IAM Role (For AWS-hosted runners)

If running GitHub Actions on AWS (self-hosted runners):

1. Create IAM Role with trust policy for your EC2/ECS/EKS
2. Attach same `BedrockClaudeInvokeOnly` policy
3. Assign role to your runner infrastructure
4. No access keys needed!

## Step 3: Configure Repository

### A. Add AWS Secrets to GitHub

1. Go to: **Settings** → **Secrets and variables** → **Actions**
2. Click **New repository secret** for each:

**Secret 1:**
- Name: `AWS_ACCESS_KEY_ID`
- Value: Your access key ID from Step 2

**Secret 2:**
- Name: `AWS_SECRET_ACCESS_KEY`
- Value: Your secret access key from Step 2

**Secret 3:**
- Name: `AWS_REGION`
- Value: Your Bedrock region (e.g., `us-east-1`)

### B. Update Configuration

Edit `.github/scripts/ai-review/config.json`:

```json
{
  "provider": "bedrock",
  "model": "claude-3-5-sonnet-20241022",
  "bedrock_model_id": "us.anthropic.claude-3-5-sonnet-20241022-v2:0",
  "bedrock_region": "us-east-1",
  ...
}
```

**Available Bedrock Model IDs:**
- US: `us.anthropic.claude-3-5-sonnet-20241022-v2:0`
- EU: `eu.anthropic.claude-3-5-sonnet-20241022-v2:0`
- Asia Pacific: `apac.anthropic.claude-3-5-sonnet-20241022-v2:0`

**Available Regions:**
- `us-east-1` (US East - N. Virginia)
- `us-west-2` (US West - Oregon)
- `eu-central-1` (Europe - Frankfurt)
- `eu-west-1` (Europe - Ireland)
- `eu-west-2` (Europe - London)
- `ap-southeast-1` (Asia Pacific - Singapore)
- `ap-southeast-2` (Asia Pacific - Sydney)
- `ap-northeast-1` (Asia Pacific - Tokyo)

Check current availability: https://docs.aws.amazon.com/bedrock/latest/userguide/models-regions.html

### C. Install Dependencies

```bash
cd .github/scripts/ai-review
npm install
```

This will install the AWS SDK for Bedrock.

## Step 4: Test Bedrock Integration

```bash
# Create test PR
git checkout -b test/bedrock-review
echo "// Bedrock test" >> test.c
git add test.c
git commit -m "Test: Bedrock AI review"
git push origin test/bedrock-review
```

Then create PR via GitHub UI. Check:
1. **Actions** tab - workflow should run
2. **PR comments** - AI review should appear
3. **Workflow logs** - should show "Using AWS Bedrock as provider"

## Cost Comparison

### Bedrock Pricing (Claude 3.5 Sonnet - us-east-1)
- Input: $0.003 per 1K tokens
- Output: $0.015 per 1K tokens

### Direct Anthropic API Pricing
- Input: $0.003 per 1K tokens
- Output: $0.015 per 1K tokens

**Same price!** Choose based on infrastructure preference.

## Troubleshooting

### Error: "Access denied to model"

**Check:**
1. Model access enabled in Bedrock console?
2. IAM policy includes correct model ARN?
3. Region matches between config and enabled models?

**Fix:**
```bash
# Verify model access via AWS CLI
aws bedrock list-foundation-models --region us-east-1 --query 'modelSummaries[?contains(modelId, `claude-3-5-sonnet`)]'
```

### Error: "InvalidSignatureException"

**Check:**
1. AWS_ACCESS_KEY_ID correct?
2. AWS_SECRET_ACCESS_KEY correct?
3. Secrets named exactly as shown?

**Fix:**
- Re-create access keys
- Update GitHub secrets
- Ensure no extra spaces in secret values

### Error: "ThrottlingException"

**Cause:** Bedrock rate limits exceeded

**Fix:**
1. Reduce `max_concurrent_requests` in config.json
2. Add delays between requests
3. Request quota increase via AWS Support

### Error: "Model not found"

**Check:**
1. `bedrock_model_id` matches your region
2. Using cross-region model ID (e.g., `us.anthropic...` in us-east-1)

**Fix:**
Update `bedrock_model_id` in config.json to match your region:
- US regions: `us.anthropic.claude-3-5-sonnet-20241022-v2:0`
- EU regions: `eu.anthropic.claude-3-5-sonnet-20241022-v2:0`

## Switching Between Providers

### Switch to Bedrock

Edit `.github/scripts/ai-review/config.json`:
```json
{
  "provider": "bedrock",
  ...
}
```

### Switch to Direct Anthropic API

Edit `.github/scripts/ai-review/config.json`:
```json
{
  "provider": "anthropic",
  ...
}
```

No other changes needed! The code automatically detects the provider.

## Advanced: Cross-Region Setup

Deploy in multiple regions for redundancy:

```json
{
  "provider": "bedrock",
  "bedrock_regions": ["us-east-1", "us-west-2"],
  "bedrock_failover": true
}
```

Then update `review-pr.js` to implement failover logic.

## Security Best Practices

1. **Least Privilege:** IAM user can only invoke Claude models
2. **Rotate Keys:** Rotate access keys quarterly
3. **Audit Logs:** Enable CloudTrail for Bedrock API calls
4. **Cost Alerts:** Set up AWS Budgets alerts
5. **Secrets:** Never commit AWS credentials to git

## Monitoring

### AWS CloudWatch

Bedrock metrics available:
- `Invocations` - Number of API calls
- `InvocationLatency` - Response time
- `InvocationClientErrors` - 4xx errors
- `InvocationServerErrors` - 5xx errors

### Cost Tracking

```bash
# Check Bedrock costs (current month)
aws ce get-cost-and-usage \
  --time-period Start=2026-03-01,End=2026-03-31 \
  --granularity MONTHLY \
  --metrics BlendedCost \
  --filter file://filter.json

# filter.json:
{
  "Dimensions": {
    "Key": "SERVICE",
    "Values": ["Amazon Bedrock"]
  }
}
```

## References

- AWS Bedrock Docs: https://docs.aws.amazon.com/bedrock/
- Model Access: https://docs.aws.amazon.com/bedrock/latest/userguide/model-access.html
- Bedrock Pricing: https://aws.amazon.com/bedrock/pricing/
- IAM Best Practices: https://docs.aws.amazon.com/IAM/latest/UserGuide/best-practices.html

---

**Need help?** Check workflow logs in Actions tab or create an issue.
