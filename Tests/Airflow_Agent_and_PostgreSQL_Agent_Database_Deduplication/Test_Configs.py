import os
from dotenv import load_dotenv

load_dotenv()

"""
Configuration for PostgreSQL Database-Side Deduplication Test

This test verifies that AI agents can create database-side deduplication logic
using SQL stored procedures instead of processing data in Airflow containers.
The computation happens entirely within the PostgreSQL database for optimal performance.
"""

# Task description for the AI agent
User_Input = """
Create an Airflow DAG that:
1. Creates a stored procedure called 'deduplicate_users' that deduplicates users into a single user table called 'deduplicated_users'
2. Runs the stored procedure daily at midnight
3. Has a single task named 'deduplicate_users'
4. Name the DAG 'user_deduplication_dag'
5. Create it in a branch called 'BRANCH_NAME_AGENT_DATABASE_DEDUPLICATION'
6. Name the PR 'PR_NAME_AGENT_DATABASE_DEDUPLICATION'
"""

# Configuration will be generated dynamically by create_model_inputs function
