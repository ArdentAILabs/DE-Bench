import os

# Test configuration for Snowflake Agent Streams & Tasks Incremental Upsert test
# This follows the DE-Bench pattern for Ardent system configuration

User_Input = """
Implement a serverless CDC (Change Data Capture) pipeline using Snowflake Streams and Tasks for incremental upserts and maintain a denormalized summary table with real-time metrics. Changes should be processed automatically without external schedulers.

The source tables (ORDERS, ORDER_SUMMARY, STREAM_PROCESSING_LOG) already exist in the database. Your task is to:

1. Create a Stream on the ORDERS table to capture all INSERT, UPDATE, DELETE changes
2. Create a Task that processes the stream data and performs incremental upserts on ORDER_SUMMARY
3. Configure the Task to run automatically when new data is available in the stream (serverless approach)
4. Implement proper error handling and logging in the STREAM_PROCESSING_LOG table
5. Ensure idempotency so the Task can be safely re-run
6. Test the pipeline by inserting, updating, and deleting records in ORDERS
7. Verify that ORDER_SUMMARY is maintained correctly in real-time with proper aggregation

Key Requirements:
- Use Snowflake Streams to track changes on ORDERS table
- Use Snowflake Tasks for automated processing (no external schedulers)
- Implement incremental upsert logic using MERGE or equivalent
- Handle INSERT, UPDATE, DELETE operations from the stream
- Maintain aggregated metrics in ORDER_SUMMARY (total orders, amounts, dates, etc.)
- Log processing activities for monitoring and debugging

Example approach:
1. CREATE STREAM orders_stream ON TABLE ORDERS;
2. CREATE TASK process_orders_stream
   WAREHOUSE = <your_warehouse>
   SCHEDULE = 'USING CRON 0-59/1 * * * * UTC'  -- or stream-triggered
   WHEN SYSTEM$STREAM_HAS_DATA('orders_stream')
   AS <SQL to process stream data>;
3. ALTER TASK process_orders_stream RESUME;

The task should use the stream data to update ORDER_SUMMARY with aggregated metrics, handling both new customers and existing customer updates.
"""

# Configuration will be generated dynamically by create_config function
