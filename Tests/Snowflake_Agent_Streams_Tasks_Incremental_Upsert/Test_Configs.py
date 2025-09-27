import os

# Test configuration for Snowflake Agent Streams & Tasks Incremental Upsert test
# This follows the DE-Bench pattern for Ardent system configuration

User_Input = """
Implement a serverless CDC (Change Data Capture) pipeline using Snowflake Streams and Tasks for incremental upserts and maintain a denormalized summary table with real-time metrics and changes should be processed automatically without external schedulers.

Requirements:
1. Create a source tables like ORDERS, ORDER_SUMMARY, STREAM_PROCESSING_LOG
2. Set up a Stream on the ORDERS table to capture all changes (DML)
3. Create a Task that processes the stream data and performs incremental upserts on ORDER_SUMMARY
5. Configure the Task to run automatically when new data is available in the stream
4. Implement proper error handling and idempotency
5. Test the pipeline by inserting, updating, and deleting records in ORDERS
6. Verify that ORDER_SUMMARY is maintained correctly in near real-time
"""

# Configuration will be generated dynamically by create_config function
