import os

# AI Agent task for MySQL transaction isolation and phantom read testing
User_Input = """
Analyze the existing MySQL database with a 'transactions' table containing financial transaction data for different accounts and demonstrate understanding of MySQL's transaction isolation levels, specifically focusing on REPEATABLE READ and phantom read scenarios.

1. Analyze the existing transaction data by querying the transactions for different accounts and calculating account balances (credits - debits)
2. Demonstrate transaction isolation behavior:
   a) Start a transaction with REPEATABLE READ isolation level
   b) Query the sum/balance of transactions for account 1001
   c) In a separate connection, insert a new transaction for account 1001
   d) Re-query the sum in the original transaction
   e) Show that the sum remains consistent (no phantom read)
   f) Commit the transaction and show the updated sum
3. Test gap locking behavior by attempting to insert a record that falls within that range from another connection
"""

# Configuration will be generated dynamically by create_config function
