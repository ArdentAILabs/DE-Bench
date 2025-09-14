-- PostgreSQL schema for workflow observability test
-- Following the pattern from working tests

-- Create workflow_runs table to track workflow execution instances
CREATE TABLE workflow_runs (
    workflow_run_id SERIAL PRIMARY KEY,
    workflow_name VARCHAR(255) NOT NULL,
    organization_id VARCHAR(100) NOT NULL,
    customer_id VARCHAR(100),
    department VARCHAR(100),
    workflow_version VARCHAR(50),
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(20) NOT NULL CHECK (status IN ('running', 'success', 'failed', 'cancelled')),
    error_message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create workflow_step_runs table to track individual step executions
CREATE TABLE workflow_step_runs (
    step_run_id SERIAL PRIMARY KEY,
    workflow_run_id INTEGER NOT NULL REFERENCES workflow_runs(workflow_run_id),
    step_id VARCHAR(255) NOT NULL,
    step_name VARCHAR(255),
    step_type VARCHAR(100),
    step_category VARCHAR(100),
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(20) NOT NULL CHECK (status IN ('running', 'success', 'failed', 'skipped')),
    error_message TEXT,
    resource_usage JSONB, -- Store CPU, memory usage if available
    created_at TIMESTAMP DEFAULT NOW()
);

-- Insert sample workflow execution data
INSERT INTO workflow_runs (workflow_name, organization_id, customer_id, department, workflow_version, start_time, end_time, status, error_message) VALUES
(
    'customer_etl_pipeline',
    'org_001',
    'acme_corp',
    'marketing',
    'v1.2.3',
    '2024-01-15 02:00:00',
    '2024-01-15 02:15:30',
    'success',
    NULL
),
(
    'customer_etl_pipeline',
    'org_001',
    'acme_corp',
    'marketing',
    'v1.2.3',
    '2024-01-15 14:00:00',
    '2024-01-15 14:12:45',
    'success',
    NULL
),
(
    'ml_training_pipeline',
    'org_002',
    'tech_startup',
    'data_science',
    'v2.1.0',
    '2024-01-15 00:00:00',
    '2024-01-15 01:45:20',
    'failed',
    'Model training failed due to insufficient memory'
),
(
    'data_validation_pipeline',
    'org_001',
    'acme_corp',
    'data_engineering',
    'v1.0.5',
    '2024-01-15 08:30:00',
    '2024-01-15 08:35:15',
    'success',
    NULL
);

-- Insert sample workflow step execution data
INSERT INTO workflow_step_runs (workflow_run_id, step_id, step_name, step_type, step_category, start_time, end_time, status, error_message, resource_usage) VALUES
-- Customer ETL Pipeline Run 1 (successful)
(1, 'extract_api', 'Extract Customer Data', 'api_extract', 'extraction', '2024-01-15 02:00:00', '2024-01-15 02:05:30', 'success', NULL, '{"cpu_percent": 45, "memory_mb": 512}'),
(1, 'transform_data', 'Transform Customer Data', 'data_transform', 'transformation', '2024-01-15 02:05:30', '2024-01-15 02:12:15', 'success', NULL, '{"cpu_percent": 78, "memory_mb": 1024}'),
(1, 'load_warehouse', 'Load to Data Warehouse', 'database_load', 'loading', '2024-01-15 02:12:15', '2024-01-15 02:15:30', 'success', NULL, '{"cpu_percent": 32, "memory_mb": 256}'),

-- Customer ETL Pipeline Run 2 (successful)
(2, 'extract_api', 'Extract Customer Data', 'api_extract', 'extraction', '2024-01-15 14:00:00', '2024-01-15 14:04:20', 'success', NULL, '{"cpu_percent": 42, "memory_mb": 480}'),
(2, 'transform_data', 'Transform Customer Data', 'data_transform', 'transformation', '2024-01-15 14:04:20', '2024-01-15 14:10:30', 'success', NULL, '{"cpu_percent": 85, "memory_mb": 1152}'),
(2, 'load_warehouse', 'Load to Data Warehouse', 'database_load', 'loading', '2024-01-15 14:10:30', '2024-01-15 14:12:45', 'success', NULL, '{"cpu_percent": 28, "memory_mb": 320}'),

-- ML Training Pipeline Run (failed)
(3, 'extract_features', 'Extract ML Features', 'feature_extraction', 'extraction', '2024-01-15 00:00:00', '2024-01-15 00:15:30', 'success', NULL, '{"cpu_percent": 65, "memory_mb": 2048}'),
(3, 'train_model', 'Train ML Model', 'ml_training', 'training', '2024-01-15 00:15:30', '2024-01-15 01:30:45', 'failed', 'Out of memory during model training', '{"cpu_percent": 95, "memory_mb": 8192}'),
(3, 'deploy_model', 'Deploy Model', 'ml_deployment', 'deployment', '2024-01-15 01:30:45', '2024-01-15 01:30:45', 'skipped', 'Skipped due to training failure', NULL),

-- Data Validation Pipeline Run (successful)
(4, 'validate_schema', 'Validate Data Schema', 'data_validation', 'validation', '2024-01-15 08:30:00', '2024-01-15 08:32:10', 'success', NULL, '{"cpu_percent": 25, "memory_mb": 128}'),
(4, 'check_quality', 'Check Data Quality', 'quality_check', 'validation', '2024-01-15 08:32:10', '2024-01-15 08:35:15', 'success', NULL, '{"cpu_percent": 35, "memory_mb": 256}');

-- Verify data loaded
SELECT 'PostgreSQL workflow observability schema setup complete' AS status;
SELECT COUNT(*) AS workflow_runs_count FROM workflow_runs;
SELECT COUNT(*) AS workflow_step_runs_count FROM workflow_step_runs;
