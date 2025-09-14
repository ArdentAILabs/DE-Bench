import os

User_Input = """
Create an Airflow DAG that:
1. Extracts human-in-loop events from PostgreSQL database 'workflow_db', table 'interventions'
2. Transforms the data to categorize interventions by type:
   - validation_error: Data validation failures requiring human review
   - step_error: Workflow step execution errors requiring intervention
   - external_api_fail: External API failures requiring manual retry or alternative approach
3. Loads the categorized data into PostgreSQL database 'workflow_db', table 'ops_queue' for live UI dashboard
4. Sends Slack/webhook notifications for high-priority interventions using SLACK_APP_URL from environment variables
5. Runs every 5 minutes for real-time ops monitoring
6. Name it hil_ops_dashboard_etl
7. Name the branch BRANCH_NAME
8. Call the PR PR_NAME
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
        }
    }
}
