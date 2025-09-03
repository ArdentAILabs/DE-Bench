-- Simple Snowflake schema for workflow analytics
-- Following the pattern from working Snowflake tests
-- Variables: {{DB}}, {{SCHEMA}}, {{WAREHOUSE}}, {{ROLE}}

USE ROLE {{ROLE}};
USE WAREHOUSE {{WAREHOUSE}};
USE DATABASE {{DB}};
USE SCHEMA {{SCHEMA}};

-- Create simple workflow analytics table
CREATE OR REPLACE TABLE workflow_analytics (
    workflow_id NUMBER,
    workflow_name VARCHAR(255),
    dag_id VARCHAR(255),
    description TEXT,
    node_count NUMBER,
    created_by VARCHAR(100),
    customer_id VARCHAR(100),
    department VARCHAR(100),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Verify schema created
SELECT 'Snowflake analytics schema setup complete' AS status;
