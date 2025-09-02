import os

# Test configuration for Snowflake Agent Add Record test
# This follows the DE-Bench pattern for Ardent system configuration

User_Input = """
I need to add a new user record to the Snowflake users table. 

The new user details are:
- Name: David Wilson  
- Email: david.wilson@newuser.com
- Age: 35
- City: Austin
- State: TX
- Active: True
- Initial purchases: 0.00

Please add this user to the USERS table in Snowflake and verify it was added successfully.
"""

Configs = {
    "services": {
        "snowflake": {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "role": os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
            "database": "TEST_DB",  # Will be overridden by fixture
            "schema": "TEST_SCHEMA"  # Will be overridden by fixture
        }
    }
}
