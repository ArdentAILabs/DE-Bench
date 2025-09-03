import os

User_Input = """
Create an Airflow DAG that:
1. Extracts workflow data from PostgreSQL database 'workflow_db', table 'workflows'
2. Transforms the JSON workflow definitions into analytics format
3. Loads the results into Snowflake database 'analytics_db', table 'workflow_analytics'
4. Runs daily at 3:00 AM UTC
5. Name it workflow_analytics_etl
6. Name the branch feature/workflow_analytics_etl
7. Call the PR Add_Workflow_Analytics_ETL
8. Use these DAG settings:
    - retries: 2
    - retry_delay: 5 minutes

Transform the workflow_definition JSON to extract:
- workflow_id, workflow_name, dag_id, description
- node_count (count of nodes in the JSON)
- created_by, customer_id, department
"""

Configs = {
    "services": {
        "airflow": {
            "github_token": os.getenv("AIRFLOW_GITHUB_TOKEN"),
            "repo": os.getenv("AIRFLOW_REPO"),
            "dag_path": os.getenv("AIRFLOW_DAG_PATH"),
            "host": os.getenv("AIRFLOW_HOST"),
            "username": os.getenv("AIRFLOW_USERNAME"),
            "password": os.getenv("AIRFLOW_PASSWORD"),
            "api_token": os.getenv("AIRFLOW_API_TOKEN"),
            "requirements_path": os.getenv("AIRFLOW_REQUIREMENTS_PATH"),
        },
        "postgreSQL": {
            "hostname": os.getenv("POSTGRES_HOSTNAME"),
            "port": os.getenv("POSTGRES_PORT"),
            "username": os.getenv("POSTGRES_USERNAME"),
            "password": os.getenv("POSTGRES_PASSWORD"),
            "databases": [{"name": "workflow_db"}],  # Will be overridden by fixture
        },
        "snowflake": {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "role": os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
            "database": "ANALYTICS_DB",  # Will be overridden by fixture
            "schema": "WORKFLOW_ANALYTICS"  # Will be overridden by fixture
        },
    }
}
