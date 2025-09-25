import os

# AI Agent task for PostgreSQL idempotent upsert pipeline with conflict handling
User_Input = """
You need to implement a crash-resistant, idempotent data pipeline for a customer dimension table that can safely rerun after failures without creating duplicates.

Scenario: You're working for a streaming music platform that processes customer data from multiple sources. Your ETL pipeline must handle:
1. New customers being added
2. Existing customer updates (email changes, subscription tiers, etc.)
3. Pipeline crashes and reruns without creating duplicates
4. Partial batch failures requiring replay of specific records

Requirements:
1. Create a dimension table for customers with proper primary keys
2. Implement an idempotent upsert pattern using PostgreSQL's INSERT ... ON CONFLICT DO UPDATE
3. Handle the following test scenarios:
   - Initial load of customer records
   - Update existing customers with new information
   - Rerun the same batch (should be idempotent - no duplicates)
   - Handle conflicts gracefully when source systems disagree

Test Data Scenarios:
- Load initial customers: Alice (Premium), Bob (Free), Carol (Premium)
- Update Alice's email and tier to Enterprise  
- Rerun the same update (should be idempotent)
- Load new customer Dave (Free) in the same pipeline run

Your solution should demonstrate production-ready patterns used by companies like Spotify/Netflix for dimensional data loading and crash recovery.

Show that your pipeline is truly idempotent by running operations multiple times and validating the end state remains consistent.
"""

# Configuration will be generated dynamically by create_config function
