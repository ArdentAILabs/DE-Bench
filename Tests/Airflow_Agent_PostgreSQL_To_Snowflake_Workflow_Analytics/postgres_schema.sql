-- Simple PostgreSQL schema for workflow analytics test
-- Following the pattern from working tests

-- Create workflows table with basic structure
CREATE TABLE workflows (
    workflow_id SERIAL PRIMARY KEY,
    workflow_name VARCHAR(255) NOT NULL,
    workflow_definition JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    created_by VARCHAR(100),
    customer_id VARCHAR(100),
    department VARCHAR(100)
);

-- Insert sample workflow data (simplified)
INSERT INTO workflows (workflow_name, workflow_definition, created_by, customer_id, department) VALUES
(
    'customer_etl_pipeline',
    '{
        "dag_id": "customer_etl_pipeline",
        "description": "Extract customer data and load to warehouse",
        "schedule_interval": "0 2 * * *",
        "nodes": [
            {"node_id": "extract_api", "node_type": "api_extract"},
            {"node_id": "transform_data", "node_type": "data_transform"},
            {"node_id": "load_warehouse", "node_type": "database_load"}
        ],
        "edges": [
            {"from": "extract_api", "to": "transform_data"},
            {"from": "transform_data", "to": "load_warehouse"}
        ]
    }',
    'data_team',
    'acme_corp',
    'marketing'
),
(
    'ml_training_pipeline',
    '{
        "dag_id": "ml_training_pipeline", 
        "description": "Train ML model for predictions",
        "schedule_interval": "0 0 * * 0",
        "nodes": [
            {"node_id": "extract_features", "node_type": "feature_extraction"},
            {"node_id": "train_model", "node_type": "ml_training"},
            {"node_id": "deploy_model", "node_type": "ml_deployment"}
        ],
        "edges": [
            {"from": "extract_features", "to": "train_model"},
            {"from": "train_model", "to": "deploy_model"}
        ]
    }',
    'ml_team',
    'tech_startup', 
    'data_science'
);

-- Verify data loaded
SELECT 'PostgreSQL workflow schema setup complete' AS status;
SELECT COUNT(*) AS workflow_count FROM workflows;
