import os

# AI Agent task for PostgreSQL JSONB ingestion with schema evolution handling
User_Input = """
Build a flexible product catalog ingestion system for an e-commerce platform (similar to Shopify) that receives JSON data from thousands of merchants in varying formats. The system must handle schema drift and evolution without breaking downstream analytics.

Critical Requirements:
1. **Schema Evolution Handling**: Adapt to new fields, changing data types, and structural changes
2. **Format Flexibility**: Handle both nested and flat JSON structures from different merchants
3. **Data Integrity**: Store raw JSONB while extracting key business fields reliably
4. **Performance**: Use JSONB operators and GIN indexing for efficient querying
5. **Backward Compatibility**: Maintain consistent downstream access despite upstream changes

Your Task:
Design a JSONB-based ingestion system that handles these evolution scenarios:
a) Initial format: Simple product JSON with basic fields (id, name, price, category)
b) Schema V2: Added fields (description, tags, inventory) and nested price structure
c) Schema V3: Complex nested specs and promotional data structures
d) Merchant Variants: Different field names and data formats (arrays vs strings)
"""

# Configuration will be generated dynamically by create_config function
