# GitHub Test Branch and PR Cleanup Script

## Overview

This script automatically identifies and cleans up GitHub PRs and branches that match test naming patterns. It uses parallel processing with retry logic to handle rate limiting and provides a safe, user-confirmed cleanup process.

## Features

- **Dual Pattern Matching**: Identifies branches that:
  - End with timestamp-hash format: `-1758652260_60884125`
  - Start with `test_airflow_` prefix
- **Parallel Processing**: Uses ThreadPoolExecutor with batch size of 10 for faster operations
- **Retry Logic**: Built-in exponential backoff for rate limiting and server errors
- **Safe Operation**: Shows detailed preview and requires user confirmation
- **Comprehensive Logging**: Clear output showing progress and results

## Prerequisites

1. Environment variables in `.env` file:
   ```
   AIRFLOW_GITHUB_TOKEN=your_github_token_here
   AIRFLOW_REPO=owner/repository-name
   ```

2. Required Python packages (already in requirements.txt):
   - `PyGithub==2.5.0`
   - `python-dotenv==1.0.1`

## Usage

```bash
# From project root directory
python scripts/cleanup_test_branches_and_prs.py

# Or make executable and run directly
chmod +x scripts/cleanup_test_branches_and_prs.py
./scripts/cleanup_test_branches_and_prs.py
```

## What Gets Cleaned Up

### Branches Matching These Patterns:
1. **Timestamp-Hash Pattern**: `*-{digits}_{alphanumeric}`
   - Examples: `test_branch-1758652260_60884125`, `feature-1758651615_ed254e3b`

2. **Airflow Test Pattern**: `test_airflow_*`
   - Examples: `test_airflow_simple_dag`, `test_airflow_workflow_123`

### Operations Performed:
- **PRs**: Closes (but doesn't delete) PRs where source or destination branches match patterns
- **Branches**: Deletes matching branches (both standalone and PR source branches)

## Safety Features

- **Preview Mode**: Shows exactly what will be cleaned up before proceeding
- **User Confirmation**: Requires explicit "yes" to proceed
- **Error Handling**: Continues processing even if individual operations fail
- **Thread-Safe**: Uses locks for counters in parallel processing
- **Retry Logic**: Handles rate limiting and temporary failures

## Sample Output

```
🚀 GitHub Test Branch and PR Cleanup Script (with Parallel Processing)
================================================================================
📋 Loading environment configuration...
   Repository: owner/repo-name
🔑 Authenticating with GitHub...
   ✅ Connected to repository: owner/repo-name
   ⚡ Using parallel processing with batch size: 10

🔍 Fetching all open PRs from repository...
📋 Found 3 open PRs, checking for test branch patterns...
  ✅ Found PR #123: 'Test feature implementation'
     Source: test_airflow_feature_branch ✓
     Dest: main ✗

🌲 Fetching all branches to identify test branches...
  ✅ Found test branch: test_airflow_simple_dag
  ✅ Found test branch: old_feature-1758651615_ed254e3b

================================================================================
🧹 CLEANUP SUMMARY
================================================================================

📝 Found 1 PRs to clean up:
  PR #123: Test feature implementation
    URL: https://github.com/owner/repo/pull/123
    Source branch: test_airflow_feature_branch (matches pattern)
    Destination branch: main 

🌲 Found 2 test branches:
    - test_airflow_simple_dag
    - old_feature-1758651615_ed254e3b

================================================================================
⚠️  CONFIRMATION REQUIRED
================================================================================

Do you want to proceed with the cleanup? (yes/no): yes

================================================================================
🧹 STARTING CLEANUP (with parallel processing)
================================================================================

📝 Closing 1 PRs in parallel (batch size: 10)...
  ✅ PR #123 closed successfully

🌲 Deleting 3 branches in parallel (batch size: 10)...
  ✅ Branch test_airflow_feature_branch deleted successfully
  ✅ Branch test_airflow_simple_dag deleted successfully  
  ✅ Branch old_feature-1758651615_ed254e3b deleted successfully

================================================================================
🎉 CLEANUP COMPLETED
================================================================================
📝 PRs: 1 successful, 0 failed
🌲 Branches: 3 successful, 0 failed
```

## Error Handling

The script includes robust error handling for:

- **Rate Limiting**: Automatic retry with proper wait times
- **Server Errors**: Exponential backoff for 5xx errors
- **Missing Branches**: Graceful handling of already-deleted branches
- **API Failures**: Detailed error reporting without stopping the entire process

## Parallel Processing Details

- **Batch Size**: 10 concurrent operations
- **Thread Safety**: Uses locks for shared counters
- **Retry Strategy**: Each operation retries up to 3 times with exponential backoff
- **Rate Limit Handling**: Respects GitHub's rate limiting with intelligent wait times
