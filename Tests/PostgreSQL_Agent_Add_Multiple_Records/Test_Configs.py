import os

# AI Agent task for PostgreSQL database manipulation
User_Input = """
Your task is to connect to a PostgreSQL database and insert a new record spanning multiple related tables.

Steps:
1. Use the PostgreSQL connection parameters provided in the environment variables:
   - DB_HOST
   - DB_PORT
   - DB_NAME
   - DB_USER
   - DB_PASSWORD

2. Insert data in the following order to maintain referential integrity:
   a. Add a new user to the 'users' table with:
      - name: 'Alice Green'
      - email: 'alice@example.com'
      - age: 28

   b. Create a linked customer record in the 'customers' table for this user with:
      - phone: '111-222-3333'
      - address: '101 Elm St, Springfield'

   c. Create a new order for this customer in the 'orders' table with:
      - total_amount: 320.00
      - status: 'Processing'

   d. Add a payment for this order in the 'payments' table with:
      - amount: 320.00
      - method: 'Credit Card'
      - status: 'Completed'

3. After insertion, query the joined data (users → customers → orders → payments) to verify that all records were created correctly.

4. Print the full customer record including user details, order, and payment as confirmation.
"""

# Configuration will be generated dynamically by create_config function
