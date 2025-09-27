import os

# Test configuration for Snowflake Agent Change Data Capture Pipeline test
# This follows the DE-Bench pattern for Ardent system configuration

User_Input = """
Build a comprehensive CDC pipeline using Snowflake's native Streams and Tasks, without external tools like Kafka or Debezium.

The scenario:
- We have multiple source systems (CRM, ERP, E-commerce) that need real-time data synchronization
- Financial institutions require maintaining synchronized customer data across systems  
- All changes must be captured, transformed, and propagated with full audit trails
- The solution must be serverless and automatically handle schema evolution

Requirements:
1. Create source tables representing different business systems like CRM_CUSTOMERS, ERP_TRANSACTIONS, ECOMMERCE_ORDERS, INVENTORY_UPDATES
2. Implement comprehensive CDC architecture by creating streams on all source tables and operations with proper audit trails and versioning, validation and error handling
3. Create automated Tasks for real-time processing like customer data synchronization task (handles customer profile changes), Financial transaction processing task (handles payment and refund flows), Inventory synchronization task (manages stock levels across systems), Data quality monitoring task (validates data integrity)
4. Build CDC management and monitoring by creating streams on all source tables and operations with proper audit trails and versioning, validation and error handling
5. Implement advanced CDC features like SCD Type 2, Cross-system referential integrity maintenance, Conflict resolution for concurrent updates, Recovery and replay capabilities for failed processing
"""

# Configuration will be generated dynamically by create_config function
