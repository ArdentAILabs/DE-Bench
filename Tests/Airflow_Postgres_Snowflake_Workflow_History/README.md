# Airflow Postgres Snowflake Workflow History ETL Test

This test validates the creation of a comprehensive Airflow DAG that implements a workflow history tracking system using PostgreSQL as source and Snowflake as destination.

## Test Overview

The test validates an end-to-end ETL pipeline that:
1. **Extracts** workflow definitions and nodes from PostgreSQL tables
2. **Transforms** workflow JSON definitions into flattened edge relationships 
3. **Loads** normalized data into Snowflake data warehouse tables
4. **Enables** analytics queries for workflow adoption, customer usage, and version tracking

## Test Architecture

```
PostgreSQL (Source)          Airflow ETL DAG              Snowflake (Destination)
├── workflows               ├── extract_postgres_task    ├── workflow_definitions
├── workflow_nodes          ├── transform_edges_task     ├── workflow_edges  
                           ├── load_snowflake_task      ├── workflow_nodes
                           └── audit_logging_task       └── etl_audit_log
```

## Validation Steps

The test validates:

1. **✅ PostgreSQL Source Setup**: Confirms workflows and workflow_nodes tables exist with sample data
2. **✅ Snowflake Destination Setup**: Verifies target tables and analytics views are created
3. **✅ Airflow DAG Creation**: Agent creates production-ready DAG with proper naming and structure
4. **✅ DAG Task Structure**: Validates ETL tasks exist with proper extract/transform/load patterns
5. **✅ Data Transformation Logic**: Tests workflow JSON flattening to edge relationships
6. **✅ Analytics Views**: Validates queryable views for adoption analysis and usage patterns

## Source Data Structure

**PostgreSQL Tables:**
- `workflows`: Contains workflow metadata and JSON definitions
- `workflow_nodes`: Contains individual workflow step details

**Sample Workflows:**
- Customer Data Pipeline (ETL workflow)
- Sales Analytics Pipeline (streaming workflow) 
- Financial Reporting Workflow (batch reporting)

## Target Data Structure

**Snowflake Tables:**
- `workflow_definitions`: Normalized workflow metadata
- `workflow_edges`: Flattened DAG relationships (from_node → to_node)
- `workflow_nodes`: Individual node configurations
- `etl_audit_log`: ETL operation tracking

**Analytics Views:**
- `v_step_type_adoption`: Step type usage across workflows
- `v_customer_usage_patterns`: Customer-specific usage analytics
- `v_workflow_evolution`: Version change tracking
- `v_edge_patterns`: Common workflow connection patterns

## Use Cases Enabled

The resulting data warehouse enables queries for:

1. **Step Type Adoption Analysis**:
   ```sql
   SELECT node_type, workflow_count, customer_count 
   FROM v_step_type_adoption 
   ORDER BY workflow_count DESC;
   ```

2. **Customer Usage Patterns**:
   ```sql
   SELECT customer_id, total_workflows, unique_step_types_used
   FROM v_customer_usage_patterns
   WHERE total_workflows > 5;
   ```

3. **Workflow Evolution Tracking**:
   ```sql
   SELECT workflow_name, version_count, evolution_days
   FROM v_workflow_evolution
   ORDER BY version_count DESC;
   ```

4. **Edge Pattern Analysis**:
   ```sql
   SELECT from_node_type, to_node_type, pattern_frequency
   FROM v_edge_patterns
   WHERE customers_using_pattern > 1;
   ```

## Running the Test

```bash
# Run with default mode (Ardent)
pytest Tests/Airflow_Postgres_Snowflake_Workflow_History/test_airflow_postgres_snowflake_workflow_history.py -v

# Run with Claude Code mode
pytest Tests/Airflow_Postgres_Snowflake_Workflow_History/test_airflow_postgres_snowflake_workflow_history.py -v --mode=Claude_Code

# Run with OpenAI Codex mode
pytest Tests/Airflow_Postgres_Snowflake_Workflow_History/test_airflow_postgres_snowflake_workflow_history.py -v --mode=OpenAI_Codex
```

## Environment Requirements

### Core Framework Variables (Required for all tests)
- `SUPABASE_URL`: Your Supabase project URL
- `SUPABASE_SERVICE_ROLE_KEY`: Supabase service role key (for user management)
- `SUPABASE_JWT_SECRET`: JWT secret for token generation
- `ARDENT_BASE_URL`: Your Ardent backend base URL (Ardent mode only)

### PostgreSQL Variables
- `POSTGRES_HOSTNAME`: PostgreSQL host
- `POSTGRES_PORT`: PostgreSQL port (default: 5432)
- `POSTGRES_USERNAME`: PostgreSQL username
- `POSTGRES_PASSWORD`: PostgreSQL password

### Snowflake Variables
- `SNOWFLAKE_ACCOUNT`: Your Snowflake account identifier (e.g., abc12345.us-west-2)
- `SNOWFLAKE_USER`: Snowflake username
- `SNOWFLAKE_PASSWORD`: Snowflake password
- `SNOWFLAKE_WAREHOUSE`: Snowflake warehouse name
- `SNOWFLAKE_ROLE`: Snowflake role (optional, defaults to SYSADMIN)

### Airflow Variables
- `AIRFLOW_GITHUB_TOKEN`: GitHub token for DAG management
- `AIRFLOW_REPO`: GitHub repository URL for storing DAGs
- `AIRFLOW_DAG_PATH`: Path to DAGs in repository (default: dags/)
- `AIRFLOW_HOST`: Airflow webserver URL (default: http://localhost:8080)
- `AIRFLOW_USERNAME`: Airflow username (default: airflow)
- `AIRFLOW_PASSWORD`: Airflow password (default: airflow)

## What This Test Validates

1. **Agent Capability**: Multi-service orchestration with complex data transformation requirements
2. **Technical Integration**: Full ETL pipeline spanning PostgreSQL → Airflow → Snowflake
3. **Real-world Scenario**: Enterprise workflow auditing and analytics system implementation
4. **Data Engineering Best Practices**: Proper schema design, indexing, and analytics view creation
5. **Production Readiness**: Error handling, logging, audit trails, and incremental loading patterns

## Test Difficulty: 7/10

This test represents a complex, production-grade data engineering scenario requiring:
- Multi-database integration and schema design
- JSON data transformation and normalization  
- Analytics view creation and query optimization
- Airflow DAG development with proper task orchestration
- Error handling and audit logging implementation