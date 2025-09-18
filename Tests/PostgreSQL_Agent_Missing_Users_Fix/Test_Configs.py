import os

# AI Agent task - non-prescriptive, focuses on problem identification
User_Input = """
You have a PostgreSQL database with user and subscription data that has a data consistency issue.

Business reports are showing inconsistent user counts - some users seem to be missing from subscription-related queries, even though they exist in the users table. This is causing problems with analytics and user metrics.

Please analyze the database structure, identify what's causing users to disappear from reports, and implement a proper solution that ensures all users are always included in business analytics.

Test your solution to verify that all users are properly represented in queries.
"""

# Configuration will be generated dynamically by create_config function
