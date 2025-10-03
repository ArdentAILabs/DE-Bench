import os

# Test configuration for Snowflake Agent COPY INTO S3 Schema Drift test
# This follows the DE-Bench pattern for Ardent system configuration

User_Input = """
Create a robust COPY INTO pipeline in Snowflake that can handle schema drift from S3 data files.

1. Create a staging table that can accommodate schema evolution
2. Set up a COPY INTO command using MATCH_BY_COLUMN_NAME to handle column order differences
3. Implement error handling for missing columns and data type mismatches
4. Create a robust data loading process that won't break when new columns are added
5. Test the pipeline with both the initial schema and an evolved schema
"""

# Configuration will be generated dynamically by create_config function
