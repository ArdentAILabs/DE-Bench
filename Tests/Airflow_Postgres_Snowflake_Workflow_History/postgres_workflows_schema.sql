-- PostgreSQL schema for workflow source data
-- This creates the source tables that contain workflow definitions and nodes

-- Create workflows table to store workflow definitions
CREATE TABLE IF NOT EXISTS workflows (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    definition JSONB NOT NULL,
    version VARCHAR(50) NOT NULL DEFAULT '1.0',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by VARCHAR(100) NOT NULL,
    status VARCHAR(50) DEFAULT 'active',
    customer_id VARCHAR(100) NOT NULL,
    tags JSONB DEFAULT '[]'::jsonb
);

-- Create workflow_nodes table to store individual workflow steps
CREATE TABLE IF NOT EXISTS workflow_nodes (
    id SERIAL PRIMARY KEY,
    workflow_id INTEGER REFERENCES workflows(id) ON DELETE CASCADE,
    node_id VARCHAR(100) NOT NULL,
    node_name VARCHAR(255) NOT NULL,
    node_type VARCHAR(100) NOT NULL,
    node_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    position_x INTEGER DEFAULT 0,
    position_y INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create index for better query performance
CREATE INDEX IF NOT EXISTS idx_workflows_updated_at ON workflows(updated_at);
CREATE INDEX IF NOT EXISTS idx_workflows_customer_id ON workflows(customer_id);
CREATE INDEX IF NOT EXISTS idx_workflows_status ON workflows(status);
CREATE INDEX IF NOT EXISTS idx_workflow_nodes_workflow_id ON workflow_nodes(workflow_id);
CREATE INDEX IF NOT EXISTS idx_workflow_nodes_node_type ON workflow_nodes(node_type);
CREATE INDEX IF NOT EXISTS idx_workflow_nodes_updated_at ON workflow_nodes(updated_at);

-- Insert sample workflow data for testing
INSERT INTO workflows (name, description, definition, version, created_by, customer_id, tags) VALUES 
(
    'Customer Data Pipeline',
    'ETL pipeline for processing customer data from CRM to data warehouse',
    '{
        "nodes": [
            {
                "id": "extract_crm",
                "type": "data_extract",
                "config": {"source": "salesforce_crm", "table": "customers"}
            },
            {
                "id": "validate_data", 
                "type": "data_validation",
                "config": {"rules": ["not_null", "email_format"]}
            },
            {
                "id": "transform_customer",
                "type": "data_transform", 
                "config": {"transformations": ["normalize_names", "standardize_phone"]}
            },
            {
                "id": "load_warehouse",
                "type": "data_load",
                "config": {"destination": "snowflake", "table": "dim_customers"}
            }
        ],
        "edges": [
            {"from": "extract_crm", "to": "validate_data"},
            {"from": "validate_data", "to": "transform_customer"},
            {"from": "transform_customer", "to": "load_warehouse"}
        ]
    }'::jsonb,
    '1.0',
    'data_engineer@company.com',
    'customer_001',
    '["etl", "crm", "customer_data"]'::jsonb
),
(
    'Sales Analytics Pipeline',
    'Real-time pipeline for sales metrics and reporting',
    '{
        "nodes": [
            {
                "id": "stream_sales_data",
                "type": "stream_source",
                "config": {"source": "kafka", "topic": "sales_events"}
            },
            {
                "id": "enrich_sales",
                "type": "data_enrichment",
                "config": {"lookup_tables": ["products", "customers", "territories"]}
            },
            {
                "id": "aggregate_metrics",
                "type": "aggregation",
                "config": {"metrics": ["revenue_by_region", "top_products", "customer_lifetime_value"]}
            },
            {
                "id": "update_dashboard",
                "type": "dashboard_update",
                "config": {"dashboard_id": "sales_executive_dashboard"}
            },
            {
                "id": "send_alerts",
                "type": "notification",
                "config": {"conditions": ["revenue_threshold", "anomaly_detection"]}
            }
        ],
        "edges": [
            {"from": "stream_sales_data", "to": "enrich_sales"},
            {"from": "enrich_sales", "to": "aggregate_metrics"}, 
            {"from": "aggregate_metrics", "to": "update_dashboard"},
            {"from": "aggregate_metrics", "to": "send_alerts"}
        ]
    }'::jsonb,
    '2.1',
    'analytics_team@company.com',
    'customer_001',
    '["streaming", "analytics", "sales", "real_time"]'::jsonb
),
(
    'Financial Reporting Workflow',
    'Monthly financial data processing and reporting',
    '{
        "nodes": [
            {
                "id": "extract_financial",
                "type": "data_extract",
                "config": {"sources": ["erp_system", "bank_api", "expense_tracker"]}
            },
            {
                "id": "reconcile_accounts",
                "type": "financial_reconciliation", 
                "config": {"accounts": ["revenue", "expenses", "assets", "liabilities"]}
            },
            {
                "id": "calculate_metrics",
                "type": "financial_calculation",
                "config": {"metrics": ["profit_loss", "cash_flow", "balance_sheet_items"]}
            },
            {
                "id": "generate_reports",
                "type": "report_generation",
                "config": {"formats": ["pdf", "excel"], "recipients": ["cfo@company.com", "accounting@company.com"]}
            },
            {
                "id": "archive_data",
                "type": "data_archival",
                "config": {"retention_period": "7_years", "storage": "s3_compliance_bucket"}
            }
        ],
        "edges": [
            {"from": "extract_financial", "to": "reconcile_accounts"},
            {"from": "reconcile_accounts", "to": "calculate_metrics"},
            {"from": "calculate_metrics", "to": "generate_reports"},
            {"from": "calculate_metrics", "to": "archive_data"}
        ]
    }'::jsonb,
    '1.5',
    'finance_team@company.com',
    'customer_002',
    '["finance", "reporting", "compliance", "monthly"]'::jsonb
);

-- Insert corresponding workflow nodes
INSERT INTO workflow_nodes (workflow_id, node_id, node_name, node_type, node_config, position_x, position_y) VALUES
-- Nodes for Customer Data Pipeline (workflow_id = 1)
(1, 'extract_crm', 'Extract CRM Data', 'data_extract', '{"source": "salesforce_crm", "table": "customers"}'::jsonb, 100, 100),
(1, 'validate_data', 'Validate Customer Data', 'data_validation', '{"rules": ["not_null", "email_format"]}'::jsonb, 300, 100), 
(1, 'transform_customer', 'Transform Customer Data', 'data_transform', '{"transformations": ["normalize_names", "standardize_phone"]}'::jsonb, 500, 100),
(1, 'load_warehouse', 'Load to Data Warehouse', 'data_load', '{"destination": "snowflake", "table": "dim_customers"}'::jsonb, 700, 100),

-- Nodes for Sales Analytics Pipeline (workflow_id = 2)
(2, 'stream_sales_data', 'Stream Sales Data', 'stream_source', '{"source": "kafka", "topic": "sales_events"}'::jsonb, 100, 200),
(2, 'enrich_sales', 'Enrich Sales Data', 'data_enrichment', '{"lookup_tables": ["products", "customers", "territories"]}'::jsonb, 300, 200),
(2, 'aggregate_metrics', 'Aggregate Sales Metrics', 'aggregation', '{"metrics": ["revenue_by_region", "top_products", "customer_lifetime_value"]}'::jsonb, 500, 200),
(2, 'update_dashboard', 'Update Sales Dashboard', 'dashboard_update', '{"dashboard_id": "sales_executive_dashboard"}'::jsonb, 700, 150),
(2, 'send_alerts', 'Send Alert Notifications', 'notification', '{"conditions": ["revenue_threshold", "anomaly_detection"]}'::jsonb, 700, 250),

-- Nodes for Financial Reporting Workflow (workflow_id = 3)
(3, 'extract_financial', 'Extract Financial Data', 'data_extract', '{"sources": ["erp_system", "bank_api", "expense_tracker"]}'::jsonb, 100, 300),
(3, 'reconcile_accounts', 'Reconcile Account Data', 'financial_reconciliation', '{"accounts": ["revenue", "expenses", "assets", "liabilities"]}'::jsonb, 300, 300),
(3, 'calculate_metrics', 'Calculate Financial Metrics', 'financial_calculation', '{"metrics": ["profit_loss", "cash_flow", "balance_sheet_items"]}'::jsonb, 500, 300),
(3, 'generate_reports', 'Generate Financial Reports', 'report_generation', '{"formats": ["pdf", "excel"], "recipients": ["cfo@company.com", "accounting@company.com"]}'::jsonb, 700, 280),
(3, 'archive_data', 'Archive Financial Data', 'data_archival', '{"retention_period": "7_years", "storage": "s3_compliance_bucket"}'::jsonb, 700, 320);

-- Create a trigger to automatically update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_workflows_updated_at BEFORE UPDATE ON workflows
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_workflow_nodes_updated_at BEFORE UPDATE ON workflow_nodes  
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();