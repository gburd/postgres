#!/usr/bin/env node

import { readFile } from 'fs/promises';
import { Anthropic } from '@anthropic-ai/sdk';
import { BedrockRuntimeClient, InvokeModelCommand } from '@aws-sdk/client-bedrock-runtime';
import * as core from '@actions/core';
import * as github from '@actions/github';
import parseDiff from 'parse-diff';
import { minimatch } from 'minimatch';

// Load configuration
const config = JSON.parse(await readFile(new URL('./config.json', import.meta.url)));

// Validate Bedrock configuration
if (config.provider === 'bedrock') {
  // Validate model ID format
  const bedrockModelPattern = /^anthropic\.claude-[\w-]+-\d{8}-v\d+:\d+$/;
  if (!config.bedrock_model_id || !bedrockModelPattern.test(config.bedrock_model_id)) {
    core.setFailed(
      `Invalid Bedrock model ID: "${config.bedrock_model_id}". ` +
      `Expected format: anthropic.claude-<model>-<YYYYMMDD>-v<version>:<revision> ` +
      `Example: anthropic.claude-3-5-sonnet-20241022-v2:0`
    );
    process.exit(1);
  }

  // Warn about suspicious dates
  const dateMatch = config.bedrock_model_id.match(/-(\d{8})-/);
  if (dateMatch) {
    const modelDate = new Date(
      dateMatch[1].substring(0, 4),
      dateMatch[1].substring(4, 6) - 1,
      dateMatch[1].substring(6, 8)
    );
    const now = new Date();

    if (modelDate > now) {
      core.warning(
        `Model date ${dateMatch[1]} is in the future. ` +
        `This may indicate a configuration error.`
      );
    }
  }

  core.info(`Using Bedrock model: ${config.bedrock_model_id}`);
}

// Initialize clients based on provider
let anthropic = null;
let bedrockClient = null;

if (config.provider === 'bedrock') {
  core.info('Using AWS Bedrock as provider');
  bedrockClient = new BedrockRuntimeClient({
    region: config.bedrock_region || 'us-east-1',
    // Credentials will be loaded from environment (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    // or from IAM role if running on AWS
  });
} else {
  core.info('Using Anthropic API as provider');
  anthropic = new Anthropic({
    apiKey: process.env.ANTHROPIC_API_KEY,
  });
}

const octokit = github.getOctokit(process.env.GITHUB_TOKEN);
const context = github.context;

// Cost tracking
let totalCost = 0;
const costLog = [];

/**
 * Main review function
 */
async function reviewPullRequest() {
  try {
    // Get PR number from either pull_request event or workflow_dispatch input
    let prNumber = context.payload.pull_request?.number;

    // For workflow_dispatch, check inputs (available as environment variable)
    if (!prNumber && process.env.INPUT_PR_NUMBER) {
      prNumber = parseInt(process.env.INPUT_PR_NUMBER, 10);
    }

    // Also check context.payload.inputs for workflow_dispatch
    if (!prNumber && context.payload.inputs?.pr_number) {
      prNumber = parseInt(context.payload.inputs.pr_number, 10);
    }

    if (!prNumber || isNaN(prNumber)) {
      throw new Error('No PR number found in context. For manual runs, provide pr_number input.');
    }

    core.info(`Starting AI review for PR #${prNumber}`);

    // Fetch PR details
    const { data: pr } = await octokit.rest.pulls.get({
      owner: context.repo.owner,
      repo: context.repo.repo,
      pull_number: prNumber,
    });

    // Skip draft PRs (unless manually triggered)
    const isManualDispatch = context.eventName === 'workflow_dispatch';
    if (pr.draft && !isManualDispatch) {
      core.info('Skipping draft PR (use workflow_dispatch to review draft PRs)');
      return;
    }
    if (pr.draft && isManualDispatch) {
      core.info('Reviewing draft PR (manual dispatch override)');
    }

    // Fetch PR diff
    const { data: diffData } = await octokit.rest.pulls.get({
      owner: context.repo.owner,
      repo: context.repo.repo,
      pull_number: prNumber,
      mediaType: {
        format: 'diff',
      },
    });

    // Parse diff
    const files = parseDiff(diffData);
    core.info(`Found ${files.length} files in PR`);

    // Filter reviewable files
    const reviewableFiles = files.filter(file => {
      // Skip deleted files
      if (file.deleted) return false;

      // Skip binary files
      if (file.binary) return false;

      // Check skip patterns
      const shouldSkip = config.skip_paths.some(pattern =>
        minimatch(file.to, pattern, { matchBase: true })
      );

      return !shouldSkip;
    });

    core.info(`${reviewableFiles.length} files are reviewable`);

    if (reviewableFiles.length === 0) {
      await postComment(prNumber, '✓ No reviewable files found in this PR.');
      return;
    }

    // Review each file
    const allReviews = [];
    for (const file of reviewableFiles) {
      try {
        const review = await reviewFile(file, prNumber);
        if (review) {
          allReviews.push(review);
        }
      } catch (error) {
        core.error(`Error reviewing ${file.to}: ${error.message}`);
      }

      // Check cost limit per PR
      if (totalCost >= config.cost_limits.max_per_pr_dollars) {
        core.warning(`Reached PR cost limit ($${config.cost_limits.max_per_pr_dollars})`);
        break;
      }
    }

    // Post summary comment
    if (allReviews.length > 0) {
      await postSummaryComment(prNumber, allReviews, pr);
    }

    // Add labels based on reviews
    await updateLabels(prNumber, allReviews);

    // Log cost
    core.info(`Total cost for this PR: $${totalCost.toFixed(2)}`);

  } catch (error) {
    core.setFailed(`Review failed: ${error.message}`);
    throw error;
  }
}

/**
 * Review a single file
 */
async function reviewFile(file, prNumber) {
  core.info(`Reviewing ${file.to}`);

  // Determine file type and select prompt
  const fileType = getFileType(file.to);
  if (!fileType) {
    core.info(`Skipping ${file.to} - no matching prompt`);
    return null;
  }

  // Load prompt
  const prompt = await loadPrompt(fileType);

  // Check file size
  const totalLines = file.chunks.reduce((sum, chunk) => sum + chunk.changes.length, 0);
  if (totalLines > config.max_file_size_lines) {
    core.warning(`Skipping ${file.to} - too large (${totalLines} lines)`);
    return null;
  }

  // Build code context
  const code = buildCodeContext(file);

  // Call Claude API
  const reviewText = await callClaude(prompt, code, file.to);

  // Parse review for issues
  const review = {
    file: file.to,
    fileType,
    content: reviewText,
    issues: extractIssues(reviewText),
  };

  // Post inline comments if configured
  if (config.review_settings.post_line_comments && review.issues.length > 0) {
    await postInlineComments(prNumber, file, review.issues);
  }

  return review;
}

/**
 * Determine file type from filename
 */
function getFileType(filename) {
  for (const [type, patterns] of Object.entries(config.file_type_patterns)) {
    if (patterns.some(pattern => minimatch(filename, pattern, { matchBase: true }))) {
      return type;
    }
  }
  return null;
}

/**
 * Load prompt for file type
 */
async function loadPrompt(fileType) {
  const promptPath = new URL(`./prompts/${fileType}.md`, import.meta.url);
  return await readFile(promptPath, 'utf-8');
}

/**
 * Build code context from diff
 */
function buildCodeContext(file) {
  let context = `File: ${file.to}\n`;

  if (file.from !== file.to) {
    context += `Renamed from: ${file.from}\n`;
  }

  context += '\n```diff\n';

  for (const chunk of file.chunks) {
    context += `@@ -${chunk.oldStart},${chunk.oldLines} +${chunk.newStart},${chunk.newLines} @@\n`;

    for (const change of chunk.changes) {
      if (change.type === 'add') {
        context += `+${change.content}\n`;
      } else if (change.type === 'del') {
        context += `-${change.content}\n`;
      } else {
        context += ` ${change.content}\n`;
      }
    }
  }

  context += '```\n';

  return context;
}

/**
 * Call Claude API for review (supports both Anthropic and Bedrock)
 */
async function callClaude(prompt, code, filename) {
  const fullPrompt = `${prompt}\n\n${code}`;

  // Estimate token count (rough approximation: 1 token ≈ 4 chars)
  const estimatedInputTokens = Math.ceil(fullPrompt.length / 4);

  core.info(`Calling Claude for ${filename} (~${estimatedInputTokens} tokens) via ${config.provider}`);

  try {
    let inputTokens, outputTokens, responseText;

    if (config.provider === 'bedrock') {
      // AWS Bedrock API call
      const payload = {
        anthropic_version: "bedrock-2023-05-31",
        max_tokens: config.max_tokens_per_request,
        messages: [{
          role: 'user',
          content: fullPrompt,
        }],
      };

      const command = new InvokeModelCommand({
        modelId: config.bedrock_model_id,
        contentType: 'application/json',
        accept: 'application/json',
        body: JSON.stringify(payload),
      });

      const response = await bedrockClient.send(command);
      const responseBody = JSON.parse(new TextDecoder().decode(response.body));

      inputTokens = responseBody.usage.input_tokens;
      outputTokens = responseBody.usage.output_tokens;
      responseText = responseBody.content[0].text;

    } else {
      // Direct Anthropic API call
      const message = await anthropic.messages.create({
        model: config.model,
        max_tokens: config.max_tokens_per_request,
        messages: [{
          role: 'user',
          content: fullPrompt,
        }],
      });

      inputTokens = message.usage.input_tokens;
      outputTokens = message.usage.output_tokens;
      responseText = message.content[0].text;
    }

    // Track cost
    const cost =
      (inputTokens / 1000) * config.cost_limits.estimated_cost_per_1k_input_tokens +
      (outputTokens / 1000) * config.cost_limits.estimated_cost_per_1k_output_tokens;

    totalCost += cost;
    costLog.push({
      file: filename,
      inputTokens,
      outputTokens,
      cost: cost.toFixed(4),
    });

    core.info(`Claude response: ${inputTokens} input, ${outputTokens} output tokens ($${cost.toFixed(4)})`);

    return responseText;

  } catch (error) {
    // Enhanced error messages for common Bedrock issues
    if (config.provider === 'bedrock') {
      if (error.name === 'ValidationException') {
        core.error(
          `Bedrock validation error: ${error.message}\n` +
          `Model ID: ${config.bedrock_model_id}\n` +
          `This usually means the model ID format is invalid or ` +
          `the model is not available in region ${config.bedrock_region}`
        );
      } else if (error.name === 'ResourceNotFoundException') {
        core.error(
          `Bedrock model not found: ${config.bedrock_model_id}\n` +
          `Verify the model is available in region ${config.bedrock_region}\n` +
          `Check model access in AWS Bedrock Console: ` +
          `https://console.aws.amazon.com/bedrock/home#/modelaccess`
        );
      } else if (error.name === 'AccessDeniedException') {
        core.error(
          `Access denied to Bedrock model: ${config.bedrock_model_id}\n` +
          `Verify:\n` +
          `1. AWS credentials have bedrock:InvokeModel permission\n` +
          `2. Model access is granted in Bedrock console\n` +
          `3. The model is available in region ${config.bedrock_region}`
        );
      } else {
        core.error(`Bedrock API error for ${filename}: ${error.message}`);
      }
    } else {
      core.error(`Claude API error for ${filename}: ${error.message}`);
    }
    throw error;
  }
}

/**
 * Extract structured issues from review text
 */
function extractIssues(reviewText) {
  const issues = [];

  // Simple pattern matching for issues
  // Look for lines starting with category tags like [Memory], [Security], etc.
  const lines = reviewText.split('\n');
  let currentIssue = null;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];

    // Match category tags at start of line
    const categoryMatch = line.match(/^\s*\[([^\]]+)\]/);
    if (categoryMatch) {
      if (currentIssue) {
        issues.push(currentIssue);
      }
      currentIssue = {
        category: categoryMatch[1],
        description: line.substring(categoryMatch[0].length).trim(),
        line: null,
      };
    } else if (currentIssue && line.trim()) {
      // Continue current issue description
      currentIssue.description += ' ' + line.trim();
    } else if (line.trim() === '' && currentIssue) {
      // End of issue
      issues.push(currentIssue);
      currentIssue = null;
    }

    // Try to extract line numbers
    const lineMatch = line.match(/line[s]?\s+(\d+)(?:-(\d+))?/i);
    if (lineMatch && currentIssue) {
      currentIssue.line = parseInt(lineMatch[1]);
      if (lineMatch[2]) {
        currentIssue.endLine = parseInt(lineMatch[2]);
      }
    }
  }

  if (currentIssue) {
    issues.push(currentIssue);
  }

  return issues;
}

/**
 * Post inline comments on PR
 */
async function postInlineComments(prNumber, file, issues) {
  for (const issue of issues) {
    try {
      // Find the position in the diff for this line
      const position = findDiffPosition(file, issue.line);

      if (!position) {
        core.warning(`Could not find position for line ${issue.line} in ${file.to}`);
        continue;
      }

      const body = `**[${issue.category}]**\n\n${issue.description}`;

      await octokit.rest.pulls.createReviewComment({
        owner: context.repo.owner,
        repo: context.repo.repo,
        pull_number: prNumber,
        body,
        commit_id: context.payload.pull_request.head.sha,
        path: file.to,
        position,
      });

      core.info(`Posted inline comment for ${file.to}:${issue.line}`);

    } catch (error) {
      core.warning(`Failed to post inline comment: ${error.message}`);
    }
  }
}

/**
 * Find position in diff for a line number
 */
function findDiffPosition(file, lineNumber) {
  if (!lineNumber) return null;

  let position = 0;
  let currentLine = 0;

  for (const chunk of file.chunks) {
    for (const change of chunk.changes) {
      position++;

      if (change.type !== 'del') {
        currentLine++;
        if (currentLine === lineNumber) {
          return position;
        }
      }
    }
  }

  return null;
}

/**
 * Post summary comment
 */
async function postSummaryComment(prNumber, reviews, pr) {
  let summary = '## 🤖 AI Code Review\n\n';
  summary += `Reviewed ${reviews.length} file(s) in this PR.\n\n`;

  // Count issues by category
  const categories = {};
  let totalIssues = 0;

  for (const review of reviews) {
    for (const issue of review.issues) {
      categories[issue.category] = (categories[issue.category] || 0) + 1;
      totalIssues++;
    }
  }

  if (totalIssues > 0) {
    summary += '### Issues Found\n\n';
    for (const [category, count] of Object.entries(categories)) {
      summary += `- **${category}**: ${count}\n`;
    }
    summary += '\n';
  } else {
    summary += '✓ No significant issues found.\n\n';
  }

  // Add individual file reviews
  summary += '### File Reviews\n\n';
  for (const review of reviews) {
    summary += `#### ${review.file}\n\n`;

    // Extract just the summary section from the review
    const summaryMatch = review.content.match(/(?:^|\n)(?:## )?Summary:?\s*([^\n]+)/i);
    if (summaryMatch) {
      summary += summaryMatch[1].trim() + '\n\n';
    }

    if (review.issues.length > 0) {
      summary += `${review.issues.length} issue(s) - see inline comments\n\n`;
    } else {
      summary += 'No issues found ✓\n\n';
    }
  }

  // Add cost info
  summary += `---\n*Cost: $${totalCost.toFixed(2)} | Model: ${config.model}*\n`;

  await postComment(prNumber, summary);
}

/**
 * Post a comment on the PR
 */
async function postComment(prNumber, body) {
  await octokit.rest.issues.createComment({
    owner: context.repo.owner,
    repo: context.repo.repo,
    issue_number: prNumber,
    body,
  });
}

/**
 * Update PR labels based on reviews
 */
async function updateLabels(prNumber, reviews) {
  const labelsToAdd = new Set();

  // Collect all review text
  const allText = reviews.map(r => r.content.toLowerCase()).join(' ');

  // Check for label keywords
  for (const [label, keywords] of Object.entries(config.auto_labels)) {
    for (const keyword of keywords) {
      if (allText.includes(keyword.toLowerCase())) {
        labelsToAdd.add(label);
        break;
      }
    }
  }

  if (labelsToAdd.size > 0) {
    const labels = Array.from(labelsToAdd);
    core.info(`Adding labels: ${labels.join(', ')}`);

    try {
      await octokit.rest.issues.addLabels({
        owner: context.repo.owner,
        repo: context.repo.repo,
        issue_number: prNumber,
        labels,
      });
    } catch (error) {
      core.warning(`Failed to add labels: ${error.message}`);
    }
  }
}

// Run the review
reviewPullRequest().catch(error => {
  core.setFailed(error.message);
  process.exit(1);
});
