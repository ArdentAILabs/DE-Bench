import os

# AI Agent task for MySQL advanced partitioning for time-series analytics
User_Input = """

Implement an advanced partitioning strategy for the time-series data in the `sensor_readings` table:

1. Analyze the existing sensor data
2. Implement RANGE partitioning by month while using logical partition names like p_2025_09, p_2025_10, etc.
3. Create optimized indexes for time-series queries
4. Create a stored procedure for automatic partition management to manage partitions for a 6 month window
6. Create a summary/aggregation table for daily aggregates by sensor_id: avg_temperature, min/max humidity, pressure readings while using the same partitioning strategy
"""

# Configuration will be generated dynamically by create_config function
