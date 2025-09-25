import os

# AI Agent task for PostgreSQL JSONB ingestion with schema evolution handling
User_Input = """
You're working for an e-commerce platform (similar to Shopify) that receives product catalogs from thousands of merchants in varying JSON formats. Your challenge is to build a flexible ingestion system that handles schema drift without breaking downstream analytics.

Current Problem: Product data arrives in inconsistent JSON formats from different merchant systems. New fields appear regularly, existing fields change types, and some merchants send nested structures while others use flat formats.

Your Task:
1. Design a robust JSONB-based ingestion system that can handle schema evolution
2. Create tables to store both raw JSON and normalized/extracted key fields
3. Implement flexible extraction logic that adapts to schema changes
4. Handle the following real-world scenarios:

Test Data Scenarios to Handle:
a) Initial merchant data (V1 format):
   - Simple product JSON: {id, name, price, category}

b) Schema evolution (V2 format):
   - Added fields: {description, tags[], inventory_count}
   - Price becomes nested: {price: {amount, currency}}

c) Further evolution (V3 format):
   - New nested structure: {specs: {dimensions, weight, materials[]}}
   - Optional promotional data: {promotions: [{type, discount, valid_until}]}

d) Variant handling:
   - Different merchants use different field names (price vs cost vs amount)
   - Some send arrays, others send comma-separated strings

Requirements:
1. Store raw JSONB for full fidelity
2. Extract key business fields (id, name, price) reliably across all schema versions
3. Use PostgreSQL JSONB operators and GIN indexing for performance
4. Implement schema validation and evolution tracking
5. Handle missing fields gracefully (don't break on schema drift)
6. Create views or functions for consistent downstream access

Demonstrate that your solution can:
- Ingest all three schema versions
- Query products consistently despite format differences
- Add new schema versions without code changes
- Maintain performance with proper indexing

This mirrors real production challenges where upstream systems evolve independently and data engineers must build resilient ingestion layers.
"""

# Configuration will be generated dynamically by create_config function
