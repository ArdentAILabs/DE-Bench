import os
from dotenv import load_dotenv

load_dotenv()

# The task/query for the AI agent to solve
User_Input = """
Create an end-to-end Airflow DAG that implements a workflow history tracking system with the following requirements:

GOAL: Keep a history of every workflow JSON definition for audit and analytics.

EXTRACT: From PostgreSQL database extract data from:
- workflows table (contains workflow JSON definitions)  
- workflow_nodes table (contains individual workflow steps/nodes)

TRANSFORM: Flatten DAG structure and create normalized data:
- Convert workflow JSON definitions into workflow_edges table format (from_node, to_node relationships)
- Normalize the schema to separate workflow metadata from edge relationships
- Track version changes and timestamps for audit purposes

LOAD: Into Snowflake warehouse create two main tables:
- workflow_definitions table (workflow metadata and JSON definitions)
- workflow_edges table (flattened from_node to_node relationships)

USE CASE: Enable querying for:
- Adoption analysis of different step types across workflows
- Customer-specific usage patterns and analytics
- Version differences tracking for workflow evolution
- Historical audit trail of workflow changes

Create the DAG with these specifications:
- DAG name: 'DAG_NAME'
- Created it in a branch called: 'BRANCH_NAME'
- Name the PR: 'PR_NAME'
- Schedule: Daily at 2 AM UTC
- Create all necessary tasks for extract, transform, and load operations
- Include proper error handling and logging
- Ensure data quality checks between each step
- Use PythonOperator for custom ETL logic
- Implement incremental loading based on updated_at timestamps
"""

# System configuration for the test environment
Configs = {
    "services": {
        "postgreSQL": {
            "hostname": os.getenv("POSTGRES_HOSTNAME"),
            "port": os.getenv("POSTGRES_PORT"),
            "username": os.getenv("POSTGRES_USERNAME"),
            "password": os.getenv("POSTGRES_PASSWORD"),
            "databases": [{"name": "workflow_source_db"}],  # Will be updated with actual database name from fixture
        },
        "snowflake": {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "role": os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
            "database": "TEST_DB",  # Will be overridden by fixture
            "schema": "TEST_SCHEMA"  # Will be overridden by fixture
        },
        "airflow": {
            "github_token": os.getenv("AIRFLOW_GITHUB_TOKEN"),
            "repo": os.getenv("AIRFLOW_REPO"),
            "dag_path": os.getenv("AIRFLOW_DAG_PATH"),
            "requirements_path": os.getenv("AIRFLOW_REQUIREMENTS_PATH"),
            "host": os.getenv("AIRFLOW_HOST", "http://localhost:8080"),
            "username": os.getenv("AIRFLOW_USERNAME", "airflow"),
            "password": os.getenv("AIRFLOW_PASSWORD", "airflow"),
            "api_token": os.getenv("AIRFLOW_API_TOKEN"),
        },
    }
}