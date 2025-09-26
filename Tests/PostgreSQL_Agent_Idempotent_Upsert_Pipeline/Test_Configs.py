import os

# AI Agent task for PostgreSQL idempotent upsert pipeline with conflict handling
User_Input = """
You need to implement a crash-resistant, idempotent data pipeline for a customer dimension table that can safely rerun after failures without creating duplicates.

Create a customer dimension table and an ETL pipeline that is crash-resistant, idempotent, and can safely rerun after failures without creating duplicates. It should handle the following scenarios:
1. New customers being added
2. Existing customer updates (email changes, subscription tiers, etc.)
3. Pipeline crashes and reruns without creating duplicates
4. Partial batch failures requiring replay of specific records
5. Rerunning the same batch (should be idempotent - no duplicates)
6. Handling conflicts gracefully when source systems disagree

Test Data Scenarios:
1. Update Alice's email and tier to Enterprise
2. Rerun the same update (should be idempotent)
3. Load new customer Dave (Free) in the same pipeline run
"""

# Configuration will be generated dynamically by create_config function
