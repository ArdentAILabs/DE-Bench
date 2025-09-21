#!/usr/bin/env python3
"""
Test script for the CI Failure Fetcher utility.

This script demonstrates how to use the CIFailureFetcher to grab CI failure information
from GitHub Actions URLs. This will be the pattern used to integrate CI failure detection
into the test suite.

Usage:
    python test_ci_fetcher.py
"""

import os
import json
from dotenv import load_dotenv
from utils.ci_failure_fetcher import CIFailureFetcher

# Load environment variables
load_dotenv()


def main():
    """
    Main test function to demonstrate CI failure fetching.
    """
    print("🚀 DE-Bench CI Failure Fetcher Test")
    print("=" * 50)
    
    # The failing GitHub Actions URL from the user
    test_url = "https://github.com/ArdentAILabs/Airflow-Test/actions/runs/17871109954/job/50824720484"
    
    print(f"🔗 Testing with URL: {test_url}")
    print()
    
    try:
        # Initialize the fetcher
        print("🔧 Initializing CI Failure Fetcher...")
        fetcher = CIFailureFetcher()
        print("✅ Fetcher initialized successfully")
        print()
        
        # Parse the URL first
        print("📝 Parsing GitHub Actions URL...")
        url_info = fetcher.parse_github_actions_url(test_url)
        print("✅ URL parsed successfully:")
        print(f"   Repository: {url_info['owner']}/{url_info['repo']}")
        print(f"   Run ID: {url_info['run_id']}")
        print(f"   Job ID: {url_info['job_id']}")
        print()
        
        # Fetch the failure information
        print("📊 Fetching CI failure information...")
        failure_info = fetcher.fetch_failure_info(test_url)
        print("✅ Failure information fetched successfully")
        print()
        
        # Generate and display summary
        print("📋 Generating failure summary...")
        summary = fetcher.summarize_failures(failure_info)
        print(summary)
        print()
        
        # Save detailed information
        output_file = "ci_failure_details.json"
        print(f"💾 Saving detailed failure info to {output_file}...")
        with open(output_file, 'w') as f:
            json.dump(failure_info, f, indent=2, default=str)
        print(f"✅ Detailed information saved to {output_file}")
        print()
        
        # Show how this could be integrated into tests
        print("🔮 Integration Pattern for Tests:")
        print("-" * 40)
        print("# In your test files, you could add:")
        print("if ci_failed:")
        print("    fetcher = CIFailureFetcher()")
        print("    failure_info = fetcher.fetch_failure_info(ci_url)")
        print("    summary = fetcher.summarize_failures(failure_info)")
        print("    # Log or store the failure information")
        print("    print(summary)")
        print()
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
