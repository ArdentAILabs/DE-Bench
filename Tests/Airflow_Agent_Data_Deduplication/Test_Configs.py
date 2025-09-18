import os
from dotenv import load_dotenv

load_dotenv()

User_Input = """
Create an Airflow DAG that:
1. Deduplicate users into a single user table called 'deduplicated_users'
3. Runs daily at midnight
4. Has a single task named 'deduplicate_users'
5. Name the DAG 'user_deduplication_dag'
6. Create it in a branch called 'BRANCH_NAME'
7. Name the PR 'PR_NAME'
"""

# Configuration will be generated dynamically by create_model_inputs function
