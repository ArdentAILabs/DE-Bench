-- Snowflake schema for workflow history data warehouse
-- Variables: {{DB}}, {{SCHEMA}}, {{WAREHOUSE}}, {{ROLE}}

-- Set context
USE ROLE {{ROLE}};
USE WAREHOUSE {{WAREHOUSE}};
USE DATABASE {{DB}};
USE SCHEMA {{SCHEMA}};

-- Create workflow_definitions table for storing workflow metadata and JSON definitions
CREATE OR REPLACE TABLE workflow_definitions (
    id NUMBER(38,0) IDENTITY(1,1) PRIMARY KEY,
    source_workflow_id NUMBER NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    definition VARIANT NOT NULL,  -- JSON definition stored as VARIANT
    version VARCHAR(50) NOT NULL DEFAULT '1.0',
    created_at TIMESTAMP_NTZ NOT NULL,
    updated_at TIMESTAMP_NTZ NOT NULL,
    created_by VARCHAR(100) NOT NULL,
    status VARCHAR(50) DEFAULT 'active',
    customer_id VARCHAR(100) NOT NULL,
    tags VARIANT DEFAULT parse_json('[]'),
    -- Audit fields
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    etl_batch_id VARCHAR(100),
    source_system VARCHAR(50) DEFAULT 'postgres_workflows'
);

-- Create workflow_edges table for storing flattened DAG edges (from_node, to_node relationships)
CREATE OR REPLACE TABLE workflow_edges (
    id NUMBER(38,0) IDENTITY(1,1) PRIMARY KEY,
    source_workflow_id NUMBER NOT NULL,
    workflow_name VARCHAR(255) NOT NULL,
    from_node_id VARCHAR(100) NOT NULL,
    to_node_id VARCHAR(100) NOT NULL,
    from_node_name VARCHAR(255),
    to_node_name VARCHAR(255),
    from_node_type VARCHAR(100),
    to_node_type VARCHAR(100),
    edge_order NUMBER,  -- Order of this edge in the workflow
    -- Metadata about the workflow this edge belongs to
    workflow_version VARCHAR(50),
    customer_id VARCHAR(100) NOT NULL,
    workflow_created_at TIMESTAMP_NTZ NOT NULL,
    workflow_updated_at TIMESTAMP_NTZ NOT NULL,
    -- Audit fields  
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    etl_batch_id VARCHAR(100),
    source_system VARCHAR(50) DEFAULT 'postgres_workflows'
);

-- Create workflow_nodes table for storing individual node details
CREATE OR REPLACE TABLE workflow_nodes (
    id NUMBER(38,0) IDENTITY(1,1) PRIMARY KEY,
    source_workflow_id NUMBER NOT NULL,
    source_node_id NUMBER,
    workflow_name VARCHAR(255) NOT NULL,
    node_id VARCHAR(100) NOT NULL,
    node_name VARCHAR(255) NOT NULL,
    node_type VARCHAR(100) NOT NULL,
    node_config VARIANT NOT NULL DEFAULT parse_json('{}'),
    position_x NUMBER DEFAULT 0,
    position_y NUMBER DEFAULT 0,
    -- Workflow context
    workflow_version VARCHAR(50),
    customer_id VARCHAR(100) NOT NULL,
    workflow_created_at TIMESTAMP_NTZ NOT NULL,
    workflow_updated_at TIMESTAMP_NTZ NOT NULL,
    node_created_at TIMESTAMP_NTZ NOT NULL,
    node_updated_at TIMESTAMP_NTZ NOT NULL,
    -- Audit fields
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    etl_batch_id VARCHAR(100),
    source_system VARCHAR(50) DEFAULT 'postgres_workflows'
);

-- Create etl_audit_log table for tracking ETL operations
CREATE OR REPLACE TABLE etl_audit_log (
    id NUMBER(38,0) IDENTITY(1,1) PRIMARY KEY,
    batch_id VARCHAR(100) NOT NULL,
    etl_run_date DATE NOT NULL,
    etl_start_time TIMESTAMP_NTZ NOT NULL,
    etl_end_time TIMESTAMP_NTZ,
    source_system VARCHAR(50) NOT NULL,
    target_tables ARRAY,  -- List of tables loaded in this batch
    records_extracted NUMBER,
    records_transformed NUMBER,
    records_loaded NUMBER,
    status VARCHAR(50) DEFAULT 'running',  -- running, completed, failed
    error_message TEXT,
    airflow_dag_id VARCHAR(255),
    airflow_task_id VARCHAR(255),
    airflow_run_id VARCHAR(255)
);

-- Create indexes for better query performance
-- Indexes on workflow_definitions
CREATE INDEX IF NOT EXISTS idx_workflow_definitions_customer_id ON workflow_definitions(customer_id);
CREATE INDEX IF NOT EXISTS idx_workflow_definitions_status ON workflow_definitions(status);
CREATE INDEX IF NOT EXISTS idx_workflow_definitions_updated_at ON workflow_definitions(updated_at);
CREATE INDEX IF NOT EXISTS idx_workflow_definitions_loaded_at ON workflow_definitions(loaded_at);
CREATE INDEX IF NOT EXISTS idx_workflow_definitions_source_id ON workflow_definitions(source_workflow_id);

-- Indexes on workflow_edges  
CREATE INDEX IF NOT EXISTS idx_workflow_edges_source_workflow_id ON workflow_edges(source_workflow_id);
CREATE INDEX IF NOT EXISTS idx_workflow_edges_customer_id ON workflow_edges(customer_id);
CREATE INDEX IF NOT EXISTS idx_workflow_edges_from_node_type ON workflow_edges(from_node_type);
CREATE INDEX IF NOT EXISTS idx_workflow_edges_to_node_type ON workflow_edges(to_node_type);
CREATE INDEX IF NOT EXISTS idx_workflow_edges_loaded_at ON workflow_edges(loaded_at);

-- Indexes on workflow_nodes
CREATE INDEX IF NOT EXISTS idx_workflow_nodes_source_workflow_id ON workflow_nodes(source_workflow_id);
CREATE INDEX IF NOT EXISTS idx_workflow_nodes_customer_id ON workflow_nodes(customer_id);
CREATE INDEX IF NOT EXISTS idx_workflow_nodes_node_type ON workflow_nodes(node_type);
CREATE INDEX IF NOT EXISTS idx_workflow_nodes_loaded_at ON workflow_nodes(loaded_at);

-- Indexes on etl_audit_log
CREATE INDEX IF NOT EXISTS idx_etl_audit_log_batch_id ON etl_audit_log(batch_id);
CREATE INDEX IF NOT EXISTS idx_etl_audit_log_run_date ON etl_audit_log(etl_run_date);
CREATE INDEX IF NOT EXISTS idx_etl_audit_log_status ON etl_audit_log(status);

-- Create views for analytics and reporting

-- View for workflow step type adoption analysis
CREATE OR REPLACE VIEW v_step_type_adoption AS
SELECT 
    node_type,
    COUNT(DISTINCT source_workflow_id) as workflow_count,
    COUNT(*) as total_usage_count,
    COUNT(DISTINCT customer_id) as customer_count,
    MIN(loaded_at) as first_seen_date,
    MAX(loaded_at) as last_seen_date
FROM workflow_nodes
GROUP BY node_type
ORDER BY workflow_count DESC;

-- View for customer usage patterns
CREATE OR REPLACE VIEW v_customer_usage_patterns AS
SELECT 
    customer_id,
    COUNT(DISTINCT source_workflow_id) as total_workflows,
    COUNT(DISTINCT CASE WHEN status = 'active' THEN source_workflow_id END) as active_workflows,
    COUNT(DISTINCT node_type) as unique_step_types_used,
    AVG(ARRAY_SIZE(parse_json(workflow_edges.workflow_name))) as avg_workflow_complexity,
    MIN(workflow_created_at) as first_workflow_date,
    MAX(workflow_updated_at) as last_modified_date
FROM workflow_definitions wd
LEFT JOIN (
    SELECT source_workflow_id, workflow_name, COUNT(*) as edge_count
    FROM workflow_edges 
    GROUP BY source_workflow_id, workflow_name
) we ON wd.source_workflow_id = we.source_workflow_id
GROUP BY customer_id
ORDER BY total_workflows DESC;

-- View for workflow version differences and evolution
CREATE OR REPLACE VIEW v_workflow_evolution AS  
SELECT 
    name as workflow_name,
    customer_id,
    COUNT(DISTINCT version) as version_count,
    LISTAGG(DISTINCT version, ', ') WITHIN GROUP (ORDER BY version) as all_versions,
    MIN(created_at) as first_version_date,
    MAX(updated_at) as latest_version_date,
    DATEDIFF(day, MIN(created_at), MAX(updated_at)) as evolution_days
FROM workflow_definitions
GROUP BY name, customer_id
HAVING COUNT(DISTINCT version) > 1
ORDER BY version_count DESC, evolution_days DESC;

-- View for edge relationship patterns
CREATE OR REPLACE VIEW v_edge_patterns AS
SELECT 
    from_node_type,
    to_node_type,
    COUNT(*) as pattern_frequency,
    COUNT(DISTINCT customer_id) as customers_using_pattern,
    COUNT(DISTINCT source_workflow_id) as workflows_using_pattern,
    ROUND(AVG(edge_order), 2) as avg_position_in_workflow
FROM workflow_edges
GROUP BY from_node_type, to_node_type
ORDER BY pattern_frequency DESC;

-- Comments for documentation
COMMENT ON TABLE workflow_definitions IS 'Stores workflow metadata and complete JSON definitions for audit and analytics';
COMMENT ON TABLE workflow_edges IS 'Flattened DAG relationships showing from_node to to_node connections for analysis';  
COMMENT ON TABLE workflow_nodes IS 'Individual workflow step/node details with configuration and positioning';
COMMENT ON TABLE etl_audit_log IS 'ETL operation tracking and monitoring for data lineage and debugging';

COMMENT ON VIEW v_step_type_adoption IS 'Analytics view showing adoption patterns of different workflow step types across customers';
COMMENT ON VIEW v_customer_usage_patterns IS 'Customer-specific usage analytics including workflow complexity and activity metrics';  
COMMENT ON VIEW v_workflow_evolution IS 'Tracks workflow version changes and evolution patterns over time';
COMMENT ON VIEW v_edge_patterns IS 'Common workflow connection patterns and their usage frequency across customers';