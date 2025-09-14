import os

User_Input = """
Create a sales fact table in the PostgreSQL database. 

The database contains many tables with various types of data. You need to:
1. Analyze the existing database schema to identify which tables are relevant for creating a sales fact table
2. Determine the appropriate relationships between tables
3. Create a sales_fact table with proper foreign key constraints
4. Populate the table with realistic sales data that demonstrates the relationships

Requirements for the sales_fact table:
- Must have a primary key (sales_id)
- Must include transaction_id, customer_id, product_id as foreign keys
- Must include quantity, unit_price, total_amount, sale_date
- Must have proper foreign key constraints to ensure data integrity
- Must be populated with realistic data that references existing records

Do not specify which exact tables to use - analyze the schema and make intelligent decisions about which tables are most appropriate for a sales fact table.
"""

Configs = {
    "services": {
        "postgreSQL": {
            "hostname": os.getenv("POSTGRES_HOSTNAME"),
            "port": os.getenv("POSTGRES_PORT"),
            "username": os.getenv("POSTGRES_USERNAME"),
            "password": os.getenv("POSTGRES_PASSWORD"),
            "databases": [{"name": "stress_test_db"}],  # Will be overridden by fixture
        },
    }
}
