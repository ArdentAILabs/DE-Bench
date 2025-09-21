# CI Failure Fetcher

A utility for automatically fetching and analyzing GitHub Actions CI failure information in the DE-Bench project.

## Overview

The CI Failure Fetcher provides functionality to:
- Parse GitHub Actions URLs to extract repository and run information
- Fetch workflow run and job details via GitHub API
- Download and parse job logs to identify specific failure causes
- Generate human-readable summaries of failures
- Integrate with existing test infrastructure to automatically capture CI failures

## Quick Start

### Basic Usage

```python
from utils.ci_failure_fetcher import CIFailureFetcher

# Initialize with GitHub token (or use environment variables)
fetcher = CIFailureFetcher(github_token="your_token")

# Fetch failure information from a GitHub Actions URL
failure_info = fetcher.fetch_failure_info(
    "https://github.com/owner/repo/actions/runs/123/job/456"
)

# Generate a human-readable summary
summary = fetcher.summarize_failures(failure_info)
print(summary)
```

### Environment Variables

The fetcher will automatically use these environment variables if no token is provided:
- `GITHUB_TOKEN`
- `AIRFLOW_GITHUB_TOKEN`

### Integration with Tests

```python
from utils.ci_integration_example import ci_integration

# In your test failure handler
if test_failed and ci_integration.is_github_actions():
    ci_info = ci_integration.handle_test_failure(test_name, error_message)
```

## Features

### URL Parsing
Automatically parses GitHub Actions URLs to extract:
- Repository owner and name
- Workflow run ID
- Job ID (if specified)

### Failure Analysis
Analyzes job logs to identify:
- Error messages and warnings
- Python exceptions with full tracebacks
- Test failures
- Exit codes
- Failed workflow steps

### Summary Generation
Creates human-readable summaries including:
- Workflow and job status
- Key error messages
- Exception details
- Test failure counts
- Exit codes

## API Reference

### CIFailureFetcher

#### `__init__(github_token=None)`
Initialize the fetcher with optional GitHub token.

#### `fetch_failure_info(github_actions_url)`
Fetch comprehensive failure information from a GitHub Actions URL.

**Returns:** Dictionary containing workflow, job, and failure details.

#### `summarize_failures(failure_info)`
Generate a human-readable summary of failure information.

**Returns:** Formatted string summary.

#### `parse_github_actions_url(url)`
Parse a GitHub Actions URL to extract components.

**Returns:** Dictionary with owner, repo, run_id, and job_id.

## Testing

Run the test script to verify functionality:

```bash
python test_ci_fetcher.py
```

This will:
1. Parse the example GitHub Actions URL
2. Fetch failure information
3. Generate and display a summary
4. Save detailed information to a JSON file

## Integration Pattern

The intended integration pattern for DE-Bench tests:

1. **Detection**: Automatically detect when running in GitHub Actions
2. **Capture**: On test failure, construct the current workflow URL
3. **Fetch**: Use the fetcher to get detailed failure information
4. **Store**: Save failure details for analysis
5. **Report**: Include CI failure info in test results

This enables automatic capture of CI failure context without manual intervention.

## Example Output

```
🚨 CI Failure Summary for ArdentAILabs/Airflow-Test
============================================================
📋 Workflow: Test Workflow
🔗 URL: https://github.com/ArdentAILabs/Airflow-Test/actions/runs/123
📊 Status: completed / failure
🌿 Branch: main
📅 Created: 2024-01-15T10:30:00Z

🔧 Job 1: test-job
   Status: completed / failure
   URL: https://github.com/ArdentAILabs/Airflow-Test/actions/runs/123/job/456
   ❌ Errors: 3
      • Error: Command failed with exit code 1
      • Error: Test assertion failed
   🐍 Python Exceptions: 1
      • AssertionError: Expected value did not match
   🧪 Test Failures: 2
      • FAILED test_example.py::test_function
   🚪 Exit Codes: 1
```

## Files

- `ci_failure_fetcher.py` - Main fetcher utility
- `ci_integration_example.py` - Integration patterns and examples
- `test_ci_fetcher.py` - Test script
- `README_ci_failure_fetcher.md` - This documentation
