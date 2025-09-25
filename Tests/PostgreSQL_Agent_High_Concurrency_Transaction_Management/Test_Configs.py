import os

# AI Agent task for PostgreSQL high-concurrency transaction management
User_Input = """
You're building a payment processing system for a fintech company (similar to Stripe) that must handle millions of concurrent transactions while maintaining ACID guarantees. This system requires sophisticated transaction management, deadlock handling, and concurrency control.

Business Context: Your payment platform processes credit card transactions, wallet transfers, and merchant payouts simultaneously. Any data inconsistency could result in financial losses, regulatory violations, or customer disputes.

Critical Requirements:
1. **Transaction Isolation**: Implement proper isolation levels to prevent dirty reads, phantom reads, and lost updates
2. **Deadlock Management**: Handle deadlock detection and retry logic gracefully  
3. **Consistency Guarantees**: Ensure account balances never go negative and all transfers are atomic
4. **Concurrency Control**: Support simultaneous operations without data corruption
5. **Audit Trail**: Maintain complete transaction history for compliance

Your Task:
Design and implement a transaction processing system that handles these concurrent scenarios:

a) **Account Balance Updates**:
   - Multiple simultaneous deposits/withdrawals on the same account
   - Cross-account transfers that must be atomic (debit A, credit B)
   - Balance inquiries during ongoing transactions

b) **Deadlock Scenarios**:
   - User A transfers to User B while User B transfers to User A (classic deadlock)
   - Multiple users attempting transfers in different orders
   - Implement retry logic with exponential backoff

c) **Isolation Level Testing**:
   - Demonstrate different isolation levels (READ COMMITTED, SERIALIZABLE)
   - Show how phantom reads are prevented in financial calculations
   - Prevent lost updates in concurrent balance modifications

d) **Performance Under Load**:
   - Handle high-volume concurrent transactions
   - Optimize with proper indexing and lock granularity
   - Maintain consistent performance during peak loads

Schema Requirements:
- Accounts table with balance tracking
- Transactions table with proper audit trail
- Transaction status management (PENDING, COMPLETED, FAILED, ROLLED_BACK)
- Proper constraints to prevent negative balances

Test Scenarios to Implement:
1. Concurrent transfers between same accounts in opposite directions
2. Mass balance updates with deadlock recovery
3. High-volume transaction processing with isolation
4. Audit trail verification for compliance

Your solution should demonstrate production-ready patterns used by payment processors like Stripe, PayPal, or Square for handling financial transactions at scale.
"""

# Configuration will be generated dynamically by create_config function
