-- Snowflake schema for workflow observability
-- Following the pattern from working Snowflake tests
-- Variables: {{DB}}, {{SCHEMA}}, {{WAREHOUSE}}, {{ROLE}}

USE ROLE {{ROLE}};
USE WAREHOUSE {{WAREHOUSE}};
USE DATABASE {{DB}};
USE SCHEMA {{SCHEMA}};

-- Create workflow_step_events table for long-term retention of execution events
CREATE OR REPLACE TABLE workflow_step_events (
    -- Primary identifiers
    workflow_run_id NUMBER,
    step_run_id NUMBER,
    step_id VARCHAR(255),
    
    -- Workflow metadata
    workflow_name VARCHAR(255),
    organization_id VARCHAR(100),
    customer_id VARCHAR(100),
    department VARCHAR(100),
    workflow_version VARCHAR(50),
    
    -- Step metadata
    step_name VARCHAR(255),
    step_type VARCHAR(100),
    step_category VARCHAR(100),
    
    -- Execution timing
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    step_duration_seconds NUMBER,
    run_duration_seconds NUMBER,
    
    -- Execution status and results
    status VARCHAR(20),
    error_message TEXT,
    
    -- Resource usage metrics
    cpu_percent NUMBER,
    memory_mb NUMBER,
    
    -- ETL metadata
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
);

-- Verify schema created
SELECT 'Snowflake workflow observability schema setup complete' AS status;