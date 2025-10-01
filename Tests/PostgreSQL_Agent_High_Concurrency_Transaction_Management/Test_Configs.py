import os

# AI Agent task for PostgreSQL high-concurrency transaction management
User_Input = """
Build a payment processing system for a fintech company (similar to Stripe) that must handle millions of concurrent transactions while maintaining ACID guarantees. This system requires sophisticated transaction management, deadlock handling, and concurrency control. This payment platform processes credit card transactions, wallet transfers, and merchant payouts simultaneously. Any data inconsistency could result in financial losses, regulatory violations, or customer disputes.

Critical Requirements:
1. **Transaction Isolation**: Implement proper isolation levels to prevent dirty reads, phantom reads, and lost updates
2. **Deadlock Management**: Handle deadlock detection and retry logic gracefully  
3. **Consistency Guarantees**: Ensure account balances never go negative and all transfers are atomic
4. **Concurrency Control**: Support simultaneous operations without data corruption
5. **Audit Trail**: Maintain complete transaction history for compliance

Your Task:
Design and implement a transaction processing system that handles these concurrent scenarios:
a) Account Balance Updates, multiple simultaneous transactions on the same account, cross-account transfers that must be atomic (debit A, credit B), balance inquiries during ongoing transactions
b) Deadlock Scenarios, user A transfers to User B while User B transfers to User A (classic deadlock), multiple users attempting transfers in different orders, retry logic with exponential backoff
c) Isolation Level Testing, demonstrate different isolation levels (READ COMMITTED, SERIALIZABLE), show how phantom reads are prevented in financial calculations, prevent lost updates in concurrent balance modifications
d) Performance Under Load, handle high-volume concurrent transactions, optimize with proper indexing and lock granularity, maintain consistent performance during peak loads
"""

# Configuration will be generated dynamically by create_config function
