-- PostgreSQL schema for schema drift alerting test
-- Following the pattern from working tests

-- Create step_definitions table to store expected schemas for each step
CREATE TABLE step_definitions (
    step_id VARCHAR(255) PRIMARY KEY,
    step_name VARCHAR(255) NOT NULL,
    workflow_name VARCHAR(255) NOT NULL,
    organization_id VARCHAR(100) NOT NULL,
    customer_id VARCHAR(100),
    department VARCHAR(100),
    step_type VARCHAR(100),
    expected_input_schema JSONB NOT NULL,
    expected_output_schema JSONB NOT NULL,
    version VARCHAR(50) DEFAULT '1.0.0',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Create workflow_step_runs table to store actual runtime I/O schemas
CREATE TABLE workflow_step_runs (
    step_run_id SERIAL PRIMARY KEY,
    step_id VARCHAR(255) NOT NULL REFERENCES step_definitions(step_id),
    workflow_run_id VARCHAR(255) NOT NULL,
    workflow_name VARCHAR(255) NOT NULL,
    organization_id VARCHAR(100) NOT NULL,
    customer_id VARCHAR(100),
    department VARCHAR(100),
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(20) NOT NULL CHECK (status IN ('running', 'success', 'failed', 'skipped')),
    actual_input_schema JSONB,
    actual_output_schema JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Insert sample step definitions with expected schemas
INSERT INTO step_definitions (step_id, step_name, workflow_name, organization_id, customer_id, department, step_type, expected_input_schema, expected_output_schema) VALUES
(
    'extract_customer_data',
    'Extract Customer Data',
    'customer_etl_pipeline',
    'org_001',
    'acme_corp',
    'marketing',
    'api_extract',
    '{
        "type": "object",
        "properties": {
            "api_endpoint": {"type": "string"},
            "auth_token": {"type": "string"},
            "date_range": {"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}}}
        },
        "required": ["api_endpoint", "auth_token"]
    }',
    '{
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "customer_id": {"type": "string"},
                "name": {"type": "string"},
                "email": {"type": "string"},
                "created_at": {"type": "string", "format": "date-time"}
            },
            "required": ["customer_id", "name", "email"]
        }
    }'
),
(
    'transform_customer_data',
    'Transform Customer Data',
    'customer_etl_pipeline',
    'org_001',
    'acme_corp',
    'marketing',
    'data_transform',
    '{
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "customer_id": {"type": "string"},
                "name": {"type": "string"},
                "email": {"type": "string"},
                "created_at": {"type": "string", "format": "date-time"}
            }
        }
    }',
    '{
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "customer_id": {"type": "string"},
                "full_name": {"type": "string"},
                "email_address": {"type": "string"},
                "signup_date": {"type": "string", "format": "date"},
                "is_active": {"type": "boolean"}
            }
        }
    }'
),
(
    'extract_ml_features',
    'Extract ML Features',
    'ml_training_pipeline',
    'org_002',
    'tech_startup',
    'data_science',
    'feature_extraction',
    '{
        "type": "object",
        "properties": {
            "data_source": {"type": "string"},
            "feature_config": {"type": "object"}
        }
    }',
    '{
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "feature_id": {"type": "string"},
                "feature_vector": {"type": "array", "items": {"type": "number"}},
                "metadata": {"type": "object"}
            }
        }
    }'
);

-- Insert sample workflow step runs with actual schemas (some with drift, some without)
INSERT INTO workflow_step_runs (step_id, workflow_run_id, workflow_name, organization_id, customer_id, department, start_time, end_time, status, actual_input_schema, actual_output_schema) VALUES
-- No drift cases
(
    'extract_customer_data',
    'run_001',
    'customer_etl_pipeline',
    'org_001',
    'acme_corp',
    'marketing',
    '2024-01-15 02:00:00',
    '2024-01-15 02:05:30',
    'success',
    '{
        "api_endpoint": "https://api.acme.com/customers",
        "auth_token": "token123",
        "date_range": {"start": "2024-01-01", "end": "2024-01-15"}
    }',
    '[
        {"customer_id": "C001", "name": "John Doe", "email": "john@acme.com", "created_at": "2024-01-10T10:00:00Z"},
        {"customer_id": "C002", "name": "Jane Smith", "email": "jane@acme.com", "created_at": "2024-01-12T14:30:00Z"}
    ]'
),
-- Input schema drift case (missing required field)
(
    'extract_customer_data',
    'run_002',
    'customer_etl_pipeline',
    'org_001',
    'acme_corp',
    'marketing',
    '2024-01-15 14:00:00',
    '2024-01-15 14:05:20',
    'success',
    '{
        "api_endpoint": "https://api.acme.com/customers",
        "date_range": {"start": "2024-01-01", "end": "2024-01-15"}
    }',
    '[
        {"customer_id": "C003", "name": "Bob Johnson", "email": "bob@acme.com", "created_at": "2024-01-14T09:15:00Z"}
    ]'
),
-- Output schema drift case (extra field)
(
    'transform_customer_data',
    'run_003',
    'customer_etl_pipeline',
    'org_001',
    'acme_corp',
    'marketing',
    '2024-01-15 02:05:30',
    '2024-01-15 02:12:15',
    'success',
    '[
        {"customer_id": "C001", "name": "John Doe", "email": "john@acme.com", "created_at": "2024-01-10T10:00:00Z"}
    ]',
    '[
        {"customer_id": "C001", "full_name": "John Doe", "email_address": "john@acme.com", "signup_date": "2024-01-10", "is_active": true, "premium_status": "gold"}
    ]'
),
-- No drift case for ML features
(
    'extract_ml_features',
    'run_004',
    'ml_training_pipeline',
    'org_002',
    'tech_startup',
    'data_science',
    '2024-01-15 00:00:00',
    '2024-01-15 00:15:30',
    'success',
    '{
        "data_source": "customer_behavior",
        "feature_config": {"window_size": 30, "features": ["purchase_frequency", "avg_order_value"]}
    }',
    '[
        {"feature_id": "F001", "feature_vector": [0.8, 150.5, 0.3], "metadata": {"extraction_time": "2024-01-15T00:15:00Z"}}
    ]'
),
-- Both input and output drift case
(
    'extract_ml_features',
    'run_005',
    'ml_training_pipeline',
    'org_002',
    'tech_startup',
    'data_science',
    '2024-01-15 12:00:00',
    '2024-01-15 12:20:45',
    'success',
    '{
        "data_source": "customer_behavior",
        "feature_config": {"window_size": 30, "features": ["purchase_frequency", "avg_order_value"]},
        "new_parameter": "experimental_mode"
    }',
    '[
        {"feature_id": "F002", "feature_vector": [0.6, 200.0], "metadata": {"extraction_time": "2024-01-15T12:20:00Z", "model_version": "v2.1"}}
    ]'
);

-- Verify data loaded
SELECT 'PostgreSQL schema drift alerting schema setup complete' AS status;
SELECT COUNT(*) AS step_definitions_count FROM step_definitions;
SELECT COUNT(*) AS workflow_step_runs_count FROM workflow_step_runs;
