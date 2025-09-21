"""
Example of how to integrate CI Failure Fetcher into existing tests.

This module demonstrates the pattern for automatically detecting and logging
CI failures when tests fail in GitHub Actions environments.
"""

import os
import json
from typing import Optional, Dict, Any
from utils.ci_failure_fetcher import CIFailureFetcher


class CIFailureIntegration:
    """
    Integration helper for automatically detecting and logging CI failures.
    """
    
    def __init__(self):
        self.fetcher = None
        self._initialize_fetcher()
    
    def _initialize_fetcher(self):
        """Initialize the CI failure fetcher if we're in a CI environment."""
        try:
            # Check if we're in GitHub Actions
            if self.is_github_actions():
                self.fetcher = CIFailureFetcher()
                print("🔧 CI Failure Fetcher initialized for GitHub Actions")
            else:
                print("ℹ️ Not in GitHub Actions environment, CI failure detection disabled")
        except Exception as e:
            print(f"⚠️ Could not initialize CI failure fetcher: {e}")
    
    def is_github_actions(self) -> bool:
        """Check if we're running in GitHub Actions."""
        return os.getenv('GITHUB_ACTIONS') == 'true'
    
    def get_current_workflow_info(self) -> Optional[Dict[str, str]]:
        """Get current workflow information from GitHub Actions environment variables."""
        if not self.is_github_actions():
            return None
        
        return {
            'repository': os.getenv('GITHUB_REPOSITORY'),
            'run_id': os.getenv('GITHUB_RUN_ID'),
            'run_number': os.getenv('GITHUB_RUN_NUMBER'),
            'workflow': os.getenv('GITHUB_WORKFLOW'),
            'job': os.getenv('GITHUB_JOB'),
            'action': os.getenv('GITHUB_ACTION'),
            'actor': os.getenv('GITHUB_ACTOR'),
            'ref': os.getenv('GITHUB_REF'),
            'sha': os.getenv('GITHUB_SHA'),
            'server_url': os.getenv('GITHUB_SERVER_URL', 'https://github.com'),
        }
    
    def construct_actions_url(self, job_id: Optional[str] = None) -> Optional[str]:
        """Construct GitHub Actions URL for current workflow run."""
        workflow_info = self.get_current_workflow_info()
        if not workflow_info or not workflow_info['repository'] or not workflow_info['run_id']:
            return None
        
        base_url = f"{workflow_info['server_url']}/{workflow_info['repository']}/actions/runs/{workflow_info['run_id']}"
        
        if job_id:
            return f"{base_url}/job/{job_id}"
        return base_url
    
    def handle_test_failure(self, test_name: str, error_info: str, job_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Handle a test failure by fetching CI information if available.
        
        Args:
            test_name: Name of the failed test
            error_info: Error information from the test
            job_id: Optional specific job ID
            
        Returns:
            CI failure information if available, None otherwise
        """
        if not self.fetcher or not self.is_github_actions():
            print(f"⚠️ Test '{test_name}' failed, but CI failure detection not available")
            return None
        
        try:
            # Construct the GitHub Actions URL
            actions_url = self.construct_actions_url(job_id)
            if not actions_url:
                print(f"⚠️ Could not construct GitHub Actions URL for failed test '{test_name}'")
                return None
            
            print(f"🔍 Test '{test_name}' failed, fetching CI information...")
            print(f"🔗 Actions URL: {actions_url}")
            
            # Fetch failure information
            failure_info = self.fetcher.fetch_failure_info(actions_url)
            
            # Add test-specific information
            failure_info['test_failure'] = {
                'test_name': test_name,
                'error_info': error_info,
                'detected_at': self.get_current_workflow_info()
            }
            
            # Generate summary
            summary = self.fetcher.summarize_failures(failure_info)
            print(f"📋 CI Failure Summary for '{test_name}':")
            print(summary)
            
            # Save to file for later analysis
            output_file = f"ci_failure_{test_name.replace('/', '_').replace('::', '_')}.json"
            with open(output_file, 'w') as f:
                json.dump(failure_info, f, indent=2, default=str)
            print(f"💾 Detailed CI failure info saved to: {output_file}")
            
            return failure_info
            
        except Exception as e:
            print(f"❌ Error handling test failure for '{test_name}': {e}")
            return None


# Global instance for easy access
ci_integration = CIFailureIntegration()


def enhanced_pytest_runtest_logreport(report):
    """
    Enhanced version of pytest_runtest_logreport that includes CI failure detection.
    
    This function can be added to conftest.py to automatically detect and log
    CI failures when tests fail in GitHub Actions.
    """
    if report.when == "call" and report.failed:
        # Get test information
        test_name = report.nodeid
        error_info = str(report.longrepr) if report.longrepr else "Unknown error"
        
        # Handle the test failure with CI detection
        ci_failure_info = ci_integration.handle_test_failure(test_name, error_info)
        
        # Add CI failure info to the report if available
        if ci_failure_info:
            report.user_properties.append(("ci_failure_info", ci_failure_info))
    
    # Continue with original logic
    if report.when == "call":
        # Initialize variables with default values
        model_runtime = None
        user_query = None
        test_steps = None
        run_trace_id = None
        ci_failure_info = None
        
        # Get values from user_properties if they exist
        for name, value in report.user_properties:
            if name == "model_runtime":
                model_runtime = value
            elif name == "user_query":
                user_query = value
            elif name == "test_steps":
                test_steps = value
            elif name == "run_trace_id":
                run_trace_id = value
            elif name == "ci_failure_info":
                ci_failure_info = value

        test_result = {
            "nodeid": report.nodeid,
            "user_query": user_query,
            "outcome": report.outcome,
            "duration": report.duration,
            "model_runtime": model_runtime,
            "run_trace_id": run_trace_id,
            "longrepr": str(report.longrepr) if report.failed else None,
            "test_steps": test_steps,
            "ci_failure_info": ci_failure_info,  # Add CI failure info
        }
        
        # Store the test result (this would be added to your existing test_results list)
        print(f"📊 Test result with CI info: {test_result['nodeid']} - {test_result['outcome']}")


def example_test_with_ci_detection():
    """
    Example of how to use CI failure detection in individual tests.
    """
    try:
        # Your test logic here
        assert False, "This is a simulated test failure"
        
    except Exception as e:
        # If we're in CI and the test fails, automatically fetch CI info
        if ci_integration.is_github_actions():
            ci_integration.handle_test_failure("example_test", str(e))
        raise  # Re-raise the exception so the test still fails


if __name__ == "__main__":
    # Test the integration
    print("🧪 Testing CI Integration...")
    
    # Simulate being in GitHub Actions (for testing purposes)
    os.environ['GITHUB_ACTIONS'] = 'true'
    os.environ['GITHUB_REPOSITORY'] = 'ArdentAILabs/Airflow-Test'
    os.environ['GITHUB_RUN_ID'] = '17871109954'
    os.environ['GITHUB_WORKFLOW'] = 'Test Workflow'
    
    integration = CIFailureIntegration()
    
    # Test URL construction
    url = integration.construct_actions_url('50824720484')
    print(f"🔗 Constructed URL: {url}")
    
    # Test failure handling
    integration.handle_test_failure("test_example", "Simulated failure", '50824720484')
