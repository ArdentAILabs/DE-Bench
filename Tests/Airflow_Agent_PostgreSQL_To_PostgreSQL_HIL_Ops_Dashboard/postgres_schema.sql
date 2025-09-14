-- PostgreSQL schema for human-in-loop ops dashboard test
-- Following the pattern from working tests

-- Create interventions table to store human-in-loop events
CREATE TABLE interventions (
    intervention_id SERIAL PRIMARY KEY,
    workflow_run_id VARCHAR(255) NOT NULL,
    step_id VARCHAR(255) NOT NULL,
    workflow_name VARCHAR(255) NOT NULL,
    organization_id VARCHAR(100) NOT NULL,
    customer_id VARCHAR(100),
    department VARCHAR(100),
    intervention_type VARCHAR(100) NOT NULL,
    error_message TEXT NOT NULL,
    context_data JSONB,
    priority_level VARCHAR(20) DEFAULT 'medium' CHECK (priority_level IN ('low', 'medium', 'high', 'critical')),
    requires_immediate_attention BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW(),
    resolved_at TIMESTAMP,
    resolved_by VARCHAR(100),
    status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'in_progress', 'resolved', 'dismissed'))
);

-- Create ops_queue table for live UI dashboard
CREATE TABLE ops_queue (
    queue_id SERIAL PRIMARY KEY,
    intervention_id INTEGER NOT NULL REFERENCES interventions(intervention_id),
    workflow_run_id VARCHAR(255) NOT NULL,
    step_id VARCHAR(255) NOT NULL,
    workflow_name VARCHAR(255) NOT NULL,
    organization_id VARCHAR(100) NOT NULL,
    customer_id VARCHAR(100),
    department VARCHAR(100),
    category VARCHAR(50) NOT NULL CHECK (category IN ('validation_error', 'step_error', 'external_api_fail')),
    priority_level VARCHAR(20) NOT NULL,
    error_message TEXT NOT NULL,
    context_data JSONB,
    assigned_to VARCHAR(100),
    status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'in_progress', 'resolved', 'dismissed')),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    slack_notification_sent BOOLEAN DEFAULT FALSE,
    slack_notification_sent_at TIMESTAMP
);

-- Insert sample intervention data
INSERT INTO interventions (workflow_run_id, step_id, workflow_name, organization_id, customer_id, department, intervention_type, error_message, context_data, priority_level, requires_immediate_attention) VALUES
-- Validation error cases
(
    'run_001',
    'validate_customer_data',
    'customer_etl_pipeline',
    'org_001',
    'acme_corp',
    'marketing',
    'data_validation_failure',
    'Customer email format validation failed for 15 records',
    '{"failed_records": 15, "validation_rule": "email_format", "sample_errors": ["invalid@", "no-at-symbol.com"]}',
    'high',
    TRUE
),
(
    'run_002',
    'validate_order_data',
    'order_processing_pipeline',
    'org_001',
    'acme_corp',
    'sales',
    'data_validation_failure',
    'Order amount validation failed - negative values detected',
    '{"failed_records": 3, "validation_rule": "positive_amount", "sample_errors": ["-150.00", "-25.50"]}',
    'medium',
    FALSE
),

-- Step error cases
(
    'run_003',
    'transform_product_data',
    'product_catalog_pipeline',
    'org_002',
    'tech_startup',
    'data_engineering',
    'transformation_error',
    'Data transformation failed due to missing required field',
    '{"error_type": "missing_field", "field_name": "product_category", "affected_records": 45}',
    'high',
    TRUE
),
(
    'run_004',
    'aggregate_sales_data',
    'sales_reporting_pipeline',
    'org_002',
    'tech_startup',
    'analytics',
    'aggregation_error',
    'Sales aggregation failed due to data type mismatch',
    '{"error_type": "type_mismatch", "expected_type": "numeric", "actual_type": "string", "field": "sales_amount"}',
    'medium',
    FALSE
),

-- External API failure cases
(
    'run_005',
    'fetch_weather_data',
    'weather_analytics_pipeline',
    'org_003',
    'weather_company',
    'data_science',
    'external_api_failure',
    'Weather API returned 503 Service Unavailable',
    '{"api_endpoint": "https://api.weather.com/v1/current", "status_code": 503, "retry_count": 3}',
    'critical',
    TRUE
),
(
    'run_006',
    'sync_payment_data',
    'payment_reconciliation_pipeline',
    'org_001',
    'acme_corp',
    'finance',
    'external_api_failure',
    'Payment gateway API timeout after 30 seconds',
    '{"api_endpoint": "https://payments.acme.com/api/sync", "timeout_seconds": 30, "retry_count": 2}',
    'high',
    TRUE
),

-- Low priority cases
(
    'run_007',
    'backup_database',
    'backup_pipeline',
    'org_001',
    'acme_corp',
    'infrastructure',
    'backup_warning',
    'Backup completed but with warnings about disk space',
    '{"warning_type": "disk_space", "available_space_gb": 5.2, "recommended_space_gb": 10}',
    'low',
    FALSE
);

-- Insert some resolved interventions for testing
INSERT INTO interventions (workflow_run_id, step_id, workflow_name, organization_id, customer_id, department, intervention_type, error_message, context_data, priority_level, requires_immediate_attention, status, resolved_at, resolved_by) VALUES
(
    'run_008',
    'validate_user_data',
    'user_onboarding_pipeline',
    'org_002',
    'tech_startup',
    'product',
    'data_validation_failure',
    'User phone number validation failed',
    '{"failed_records": 2, "validation_rule": "phone_format"}',
    'medium',
    FALSE,
    'resolved',
    '2024-01-15 10:30:00',
    'ops_team_member_1'
);

-- Verify data loaded
SELECT 'PostgreSQL HIL ops dashboard schema setup complete' AS status;
SELECT COUNT(*) AS interventions_count FROM interventions;
SELECT COUNT(*) AS interventions_by_status FROM interventions GROUP BY status;
SELECT COUNT(*) AS interventions_by_priority FROM interventions GROUP BY priority_level;
