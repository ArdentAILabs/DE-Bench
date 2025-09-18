import os

User_Input = """
Create an Airflow DAG that:
1. Extracts sales data from Postgres database 'sales_db', table 'transactions'
2. Calculates daily profit for each user
3. Stores results in MySQL database 'analytics_db', table 'daily_profits'
4. runs daily at 12:00 AM UTC
5. Name it sales_profit_pipeline
6. Name the branch BRANCH_NAME
7. Call the PR PR_NAME
8. Use these DAG settings:
    - retries: 1
    - retry_delay: 20 seconds
"""

# Configuration will be generated dynamically by create_model_inputs function
