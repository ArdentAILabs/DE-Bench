import os

# Test configuration for Snowflake Agent Add Record test
# This follows the DE-Bench pattern for Ardent system configuration

User_Input = """
I need to add a new user record to the Snowflake users table. 

The new user details are:
- Name: Sarah Johnson  
- Email: sarah.johnson@newuser.com
- Age: 35
- City: Austin
- State: TX
- Active: True
- Initial purchases: 0.00

Please add this user to the USERS table in Snowflake and verify it was added successfully.
"""

# Configuration will be generated dynamically by create_config function
