"""
CI Failure Fetcher Utility

This module provides functionality to fetch GitHub Actions CI failure information
from GitHub Actions URLs. It can be used to automatically gather failure details
when tests fail in CI environments.

Example Usage:
    from utils.ci_failure_fetcher import CIFailureFetcher
    
    fetcher = CIFailureFetcher(github_token="your_token")
    failure_info = fetcher.fetch_failure_info(
        "https://github.com/ArdentAILabs/Airflow-Test/actions/runs/17871109954/job/50824720484"
    )
    print(failure_info)
"""

import os
import re
import json
from typing import Dict, Any, Optional, List
from urllib.parse import urlparse, parse_qs
import requests
from github import Github, GithubException


class CIFailureFetcher:
    """
    A utility class to fetch CI failure information from GitHub Actions.
    """
    
    def __init__(self, github_token: Optional[str] = None):
        """
        Initialize the CI Failure Fetcher.
        
        Args:
            github_token: GitHub personal access token. If not provided, 
                         will try to get from GITHUB_TOKEN or AIRFLOW_GITHUB_TOKEN env vars.
        """
        self.github_token = github_token or os.getenv("GITHUB_TOKEN") or os.getenv("AIRFLOW_GITHUB_TOKEN")
        if not self.github_token:
            raise ValueError("GitHub token is required. Set GITHUB_TOKEN env var or pass token directly.")
        
        self.github_client = Github(self.github_token)
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"token {self.github_token}",
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "DE-Bench-CI-Fetcher"
        })
    
    def parse_github_actions_url(self, url: str) -> Dict[str, str]:
        """
        Parse a GitHub Actions URL to extract repository, run ID, and job ID.
        
        Args:
            url: GitHub Actions URL (e.g., https://github.com/owner/repo/actions/runs/123/job/456)
            
        Returns:
            Dictionary containing owner, repo, run_id, and job_id
            
        Example:
            >>> fetcher = CIFailureFetcher("token")
            >>> info = fetcher.parse_github_actions_url(
            ...     "https://github.com/ArdentAILabs/Airflow-Test/actions/runs/17871109954/job/50824720484"
            ... )
            >>> print(info)
            {'owner': 'ArdentAILabs', 'repo': 'Airflow-Test', 'run_id': '17871109954', 'job_id': '50824720484'}
        """
        # Parse the URL
        parsed = urlparse(url)
        path_parts = parsed.path.strip('/').split('/')
        
        if len(path_parts) < 6 or 'actions' not in path_parts:
            raise ValueError(f"Invalid GitHub Actions URL format: {url}")
        
        try:
            owner = path_parts[0]
            repo = path_parts[1]
            
            # Find the actions, runs, and job indices
            actions_idx = path_parts.index('actions')
            runs_idx = path_parts.index('runs', actions_idx)
            run_id = path_parts[runs_idx + 1]
            
            job_id = None
            if 'job' in path_parts:
                job_idx = path_parts.index('job')
                job_id = path_parts[job_idx + 1]
            
            return {
                'owner': owner,
                'repo': repo,
                'run_id': run_id,
                'job_id': job_id
            }
        except (IndexError, ValueError) as e:
            raise ValueError(f"Could not parse GitHub Actions URL: {url}. Error: {e}")
    
    def fetch_workflow_run_info(self, owner: str, repo: str, run_id: str) -> Dict[str, Any]:
        """
        Fetch workflow run information from GitHub API.
        
        Args:
            owner: Repository owner
            repo: Repository name
            run_id: Workflow run ID
            
        Returns:
            Dictionary containing workflow run information
        """
        try:
            repository = self.github_client.get_repo(f"{owner}/{repo}")
            workflow_run = repository.get_workflow_run(int(run_id))
            
            return {
                'id': workflow_run.id,
                'name': workflow_run.name,
                'display_title': workflow_run.display_title,
                'status': workflow_run.status,
                'conclusion': workflow_run.conclusion,
                'url': workflow_run.html_url,
                'created_at': workflow_run.created_at.isoformat() if workflow_run.created_at else None,
                'updated_at': workflow_run.updated_at.isoformat() if workflow_run.updated_at else None,
                'head_branch': workflow_run.head_branch,
                'head_sha': workflow_run.head_sha,
                'event': workflow_run.event,
                'workflow_id': workflow_run.workflow_id,
                'run_number': workflow_run.run_number,
                'run_attempt': workflow_run.run_attempt,
            }
        except GithubException as e:
            raise Exception(f"Failed to fetch workflow run {run_id}: {e}")
    
    def fetch_job_info(self, owner: str, repo: str, job_id: str) -> Dict[str, Any]:
        """
        Fetch job information from GitHub API.
        
        Args:
            owner: Repository owner
            repo: Repository name
            job_id: Job ID
            
        Returns:
            Dictionary containing job information
        """
        url = f"https://api.github.com/repos/{owner}/{repo}/actions/jobs/{job_id}"
        
        try:
            response = self.session.get(url)
            response.raise_for_status()
            job_data = response.json()
            
            return {
                'id': job_data.get('id'),
                'name': job_data.get('name'),
                'status': job_data.get('status'),
                'conclusion': job_data.get('conclusion'),
                'url': job_data.get('html_url'),
                'started_at': job_data.get('started_at'),
                'completed_at': job_data.get('completed_at'),
                'runner_name': job_data.get('runner_name'),
                'runner_group_name': job_data.get('runner_group_name'),
                'steps': job_data.get('steps', [])
            }
        except requests.RequestException as e:
            raise Exception(f"Failed to fetch job {job_id}: {e}")
    
    def fetch_job_logs(self, owner: str, repo: str, job_id: str) -> str:
        """
        Fetch job logs from GitHub API.
        
        Args:
            owner: Repository owner
            repo: Repository name
            job_id: Job ID
            
        Returns:
            Raw log content as string
        """
        url = f"https://api.github.com/repos/{owner}/{repo}/actions/jobs/{job_id}/logs"
        
        try:
            response = self.session.get(url)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            raise Exception(f"Failed to fetch logs for job {job_id}: {e}")
    
    def parse_failure_logs(self, logs: str) -> Dict[str, Any]:
        """
        Parse failure logs to extract key error information.
        
        Args:
            logs: Raw log content
            
        Returns:
            Dictionary containing parsed failure information
        """
        failure_info = {
            'errors': [],
            'warnings': [],
            'failed_steps': [],
            'exit_codes': [],
            'python_exceptions': [],
            'test_failures': []
        }
        
        lines = logs.split('\n')
        current_step = None
        
        for i, line in enumerate(lines):
            # Track current step
            step_match = re.search(r'##\[group\](.+)', line)
            if step_match:
                current_step = step_match.group(1).strip()
            
            # Look for errors
            if re.search(r'##\[error\]|ERROR:|Error:', line, re.IGNORECASE):
                failure_info['errors'].append({
                    'line_number': i + 1,
                    'step': current_step,
                    'message': line.strip()
                })
            
            # Look for warnings
            if re.search(r'##\[warning\]|WARNING:|Warning:', line, re.IGNORECASE):
                failure_info['warnings'].append({
                    'line_number': i + 1,
                    'step': current_step,
                    'message': line.strip()
                })
            
            # Look for exit codes
            exit_code_match = re.search(r'Process completed with exit code (\d+)', line)
            if exit_code_match:
                failure_info['exit_codes'].append({
                    'line_number': i + 1,
                    'step': current_step,
                    'exit_code': int(exit_code_match.group(1)),
                    'message': line.strip()
                })
            
            # Look for Python exceptions
            if re.search(r'Traceback \(most recent call last\):', line):
                # Collect the full traceback
                traceback_lines = [line]
                j = i + 1
                while j < len(lines) and (lines[j].startswith('  ') or 
                                         re.search(r'^[A-Za-z]\w*Error:', lines[j]) or
                                         re.search(r'^[A-Za-z]\w*Exception:', lines[j])):
                    traceback_lines.append(lines[j])
                    j += 1
                
                failure_info['python_exceptions'].append({
                    'line_number': i + 1,
                    'step': current_step,
                    'traceback': '\n'.join(traceback_lines)
                })
            
            # Look for test failures
            if re.search(r'FAILED|FAIL:|AssertionError|Test failed', line, re.IGNORECASE):
                failure_info['test_failures'].append({
                    'line_number': i + 1,
                    'step': current_step,
                    'message': line.strip()
                })
        
        return failure_info
    
    def fetch_failure_info(self, github_actions_url: str) -> Dict[str, Any]:
        """
        Fetch comprehensive failure information from a GitHub Actions URL.
        
        Args:
            github_actions_url: Full GitHub Actions URL
            
        Returns:
            Dictionary containing all failure information
            
        Example:
            >>> fetcher = CIFailureFetcher()
            >>> info = fetcher.fetch_failure_info(
            ...     "https://github.com/ArdentAILabs/Airflow-Test/actions/runs/17871109954/job/50824720484"
            ... )
            >>> print(json.dumps(info, indent=2))
        """
        print(f"🔍 Fetching CI failure info from: {github_actions_url}")
        
        # Parse the URL
        url_info = self.parse_github_actions_url(github_actions_url)
        print(f"📊 Parsed URL info: {url_info}")
        
        # Fetch workflow run info
        print(f"📋 Fetching workflow run info...")
        workflow_info = self.fetch_workflow_run_info(
            url_info['owner'], 
            url_info['repo'], 
            url_info['run_id']
        )
        
        result = {
            'url': github_actions_url,
            'repository': f"{url_info['owner']}/{url_info['repo']}",
            'workflow_run': workflow_info,
            'jobs': []
        }
        
        # If specific job ID is provided, fetch that job
        if url_info['job_id']:
            print(f"🔧 Fetching specific job info: {url_info['job_id']}")
            job_info = self.fetch_job_info(
                url_info['owner'], 
                url_info['repo'], 
                url_info['job_id']
            )
            
            print(f"📜 Fetching job logs...")
            try:
                job_logs = self.fetch_job_logs(
                    url_info['owner'], 
                    url_info['repo'], 
                    url_info['job_id']
                )
                
                print(f"🔍 Parsing failure logs...")
                parsed_failures = self.parse_failure_logs(job_logs)
                
                job_info['logs'] = job_logs
                job_info['parsed_failures'] = parsed_failures
                
            except Exception as e:
                print(f"⚠️ Could not fetch logs: {e}")
                job_info['logs'] = None
                job_info['parsed_failures'] = None
            
            result['jobs'].append(job_info)
        else:
            # Fetch all jobs for the workflow run
            print(f"🔧 Fetching all jobs for workflow run...")
            try:
                repository = self.github_client.get_repo(f"{url_info['owner']}/{url_info['repo']}")
                workflow_run = repository.get_workflow_run(int(url_info['run_id']))
                
                for job in workflow_run.jobs():
                    job_info = {
                        'id': job.id,
                        'name': job.name,
                        'status': job.status,
                        'conclusion': job.conclusion,
                        'url': job.html_url,
                        'started_at': job.started_at.isoformat() if job.started_at else None,
                        'completed_at': job.completed_at.isoformat() if job.completed_at else None,
                        'steps': [
                            {
                                'name': step.name,
                                'status': step.status,
                                'conclusion': step.conclusion,
                                'number': step.number,
                                'started_at': step.started_at.isoformat() if step.started_at else None,
                                'completed_at': step.completed_at.isoformat() if step.completed_at else None,
                            }
                            for step in job.steps
                        ]
                    }
                    
                    # Fetch logs for failed jobs
                    if job.conclusion == 'failure':
                        try:
                            job_logs = self.fetch_job_logs(
                                url_info['owner'], 
                                url_info['repo'], 
                                str(job.id)
                            )
                            parsed_failures = self.parse_failure_logs(job_logs)
                            job_info['logs'] = job_logs
                            job_info['parsed_failures'] = parsed_failures
                        except Exception as e:
                            print(f"⚠️ Could not fetch logs for job {job.id}: {e}")
                            job_info['logs'] = None
                            job_info['parsed_failures'] = None
                    
                    result['jobs'].append(job_info)
                    
            except Exception as e:
                print(f"⚠️ Could not fetch all jobs: {e}")
        
        print(f"✅ Successfully fetched CI failure info")
        return result
    
    def summarize_failures(self, failure_info: Dict[str, Any]) -> str:
        """
        Create a human-readable summary of the failure information.
        
        Args:
            failure_info: Failure information dictionary from fetch_failure_info()
            
        Returns:
            Human-readable summary string
        """
        summary_lines = []
        summary_lines.append(f"🚨 CI Failure Summary for {failure_info['repository']}")
        summary_lines.append("=" * 60)
        
        # Workflow info
        workflow = failure_info['workflow_run']
        summary_lines.append(f"📋 Workflow: {workflow['name']}")
        summary_lines.append(f"🔗 URL: {workflow['url']}")
        summary_lines.append(f"📊 Status: {workflow['status']} / {workflow['conclusion']}")
        summary_lines.append(f"🌿 Branch: {workflow['head_branch']}")
        summary_lines.append(f"📅 Created: {workflow['created_at']}")
        summary_lines.append("")
        
        # Job summaries
        for i, job in enumerate(failure_info['jobs']):
            summary_lines.append(f"🔧 Job {i+1}: {job['name']}")
            summary_lines.append(f"   Status: {job['status']} / {job['conclusion']}")
            summary_lines.append(f"   URL: {job['url']}")
            
            if job.get('parsed_failures'):
                failures = job['parsed_failures']
                
                if failures['errors']:
                    summary_lines.append(f"   ❌ Errors: {len(failures['errors'])}")
                    for error in failures['errors'][:3]:  # Show first 3 errors
                        summary_lines.append(f"      • {error['message'][:100]}...")
                
                if failures['python_exceptions']:
                    summary_lines.append(f"   🐍 Python Exceptions: {len(failures['python_exceptions'])}")
                    for exc in failures['python_exceptions'][:2]:  # Show first 2 exceptions
                        lines = exc['traceback'].split('\n')
                        last_line = [line for line in lines if line.strip()][-1] if lines else "Unknown error"
                        summary_lines.append(f"      • {last_line}")
                
                if failures['test_failures']:
                    summary_lines.append(f"   🧪 Test Failures: {len(failures['test_failures'])}")
                    for test_fail in failures['test_failures'][:3]:  # Show first 3 test failures
                        summary_lines.append(f"      • {test_fail['message'][:100]}...")
                
                if failures['exit_codes']:
                    exit_codes = [str(ec['exit_code']) for ec in failures['exit_codes']]
                    summary_lines.append(f"   🚪 Exit Codes: {', '.join(set(exit_codes))}")
            
            summary_lines.append("")
        
        return '\n'.join(summary_lines)


def test_ci_failure_fetcher():
    """
    Test function to demonstrate the CI Failure Fetcher functionality.
    """
    print("🧪 Testing CI Failure Fetcher...")
    
    # Test URL from the user
    test_url = "https://github.com/ArdentAILabs/Airflow-Test/actions/runs/17871109954/job/50824720484"
    
    try:
        # Initialize fetcher
        fetcher = CIFailureFetcher()
        
        # Test URL parsing
        print("\n1. Testing URL parsing...")
        url_info = fetcher.parse_github_actions_url(test_url)
        print(f"   Parsed info: {json.dumps(url_info, indent=2)}")
        
        # Test fetching failure info
        print("\n2. Testing failure info fetching...")
        failure_info = fetcher.fetch_failure_info(test_url)
        
        # Print summary
        print("\n3. Failure Summary:")
        print(fetcher.summarize_failures(failure_info))
        
        # Save detailed info to file
        output_file = "/tmp/ci_failure_info.json"
        with open(output_file, 'w') as f:
            json.dump(failure_info, f, indent=2, default=str)
        print(f"\n📁 Detailed failure info saved to: {output_file}")
        
        return failure_info
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    # Run the test
    test_ci_failure_fetcher()
