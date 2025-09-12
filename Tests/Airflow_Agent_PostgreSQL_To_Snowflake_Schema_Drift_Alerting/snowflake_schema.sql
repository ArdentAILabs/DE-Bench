-- Snowflake schema for schema drift alerting
-- Following the pattern from working Snowflake tests
-- Variables: {{DB}}, {{SCHEMA}}, {{WAREHOUSE}}, {{ROLE}}

USE ROLE {{ROLE}};
USE WAREHOUSE {{WAREHOUSE}};
USE DATABASE {{DB}};
USE SCHEMA {{SCHEMA}};

-- Create schema_drift_events table for tracking schema drift detection
CREATE OR REPLACE TABLE schema_drift_events (
    -- Primary identifiers
    step_id VARCHAR(255),
    step_run_id NUMBER,
    workflow_run_id VARCHAR(255),
    
    -- Workflow metadata
    workflow_name VARCHAR(255),
    organization_id VARCHAR(100),
    customer_id VARCHAR(100),
    department VARCHAR(100),
    step_type VARCHAR(100),
    
    -- Schema comparison results
    drift_detected BOOLEAN,
    drift_type VARCHAR(50),
    severity_level VARCHAR(20),
    
    -- Expected schemas (from step_definitions)
    expected_input_schema TEXT,
    expected_output_schema TEXT,
    
    -- Actual schemas (from workflow_step_runs)
    actual_input_schema TEXT,
    actual_output_schema TEXT,
    
    -- Schema differences
    schema_diff TEXT,
    
    -- Detection metadata
    detection_timestamp TIMESTAMP,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Verify schema created
SELECT 'Snowflake schema drift alerting schema setup complete' AS status;