import os

# AI Agent task for MySQL star schema migration and data warehouse design
User_Input = """
Migrate the current operational schema to a new data warehouse using a star schema pattern with the following requirements:

1. Analyze the existing data, relationships and business logic:
2. Create New Data Warehouse named 'data_warehouse' that follows the star schema pattern while following best practices and contains the following tables:
  a. fact_etl_jobs
  b. dim_jobs
  c. dim_time
  d. dim_status
  e. dim_categories
3. Validate there was no loss of data during the migration as well as the integrity of the data
"""

# Configuration will be generated dynamically by create_config function
