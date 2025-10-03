import os

# Test configuration for Snowflake Agent Zero-Copy Cloning test
# This follows the DE-Bench pattern for Ardent system configuration

User_Input = """
Create a development environments workflow using Snowflake's Zero-Copy Cloning capabilities of our production database.

Requirements:
1. Implement zero-copy cloning for development environments:
   - Create a DEV database clone from production
   - Create a STAGING database clone from production
   - Create a TEST database clone from production
   - Verify that clones are instant and don't consume additional storage initially
2. Demonstrate clone independence by making changes to data in the DEV clone and verifying that production data remains unchanged
3. Implement clone management workflow like:
   - Create a procedure to refresh development clones from production
   - Implement clone metadata tracking
   - Set up automated clone lifecycle management
4. Validate the solution by confirming zero-copy behavior (instant creation, minimal storage), testing data isolation between environments, and verifying performance characteristics of cloned databases
"""

# Configuration will be generated dynamically by create_config function
