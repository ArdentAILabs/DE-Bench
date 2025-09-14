# Load Bulk Tables Feature

Both PostgreSQL fixtures now support a `load_bulk` flag that will load the `bulk_tables.sql` file before loading regular tables or SQL files.

## Usage Examples

### For `postgres_resource` fixture (SQL file based):

```python
@pytest.mark.parametrize("postgres_resource", [{
    "resource_id": "test_with_bulk_tables",
    "load_bulk": True,  # This will load bulk_tables.sql first
    "databases": [
        {
            "name": "test_db",
            "sql_file": "schema.sql"  # This will be loaded after bulk_tables.sql
        }
    ]
}], indirect=True)
def test_with_bulk_tables(postgres_resource):
    # Test code here
    pass
```

### For `legacy_postgres_resource` fixture (JSON template based):

```python
@pytest.mark.parametrize("legacy_postgres_resource", [{
    "resource_id": "test_with_bulk_tables",
    "load_bulk": True,  # This will load bulk_tables.sql first
    "databases": [
        {
            "name": "test_db",
            "tables": [
                {
                    "name": "custom_table",
                    "columns": [
                        {"name": "id", "type": "SERIAL", "primary_key": True},
                        {"name": "name", "type": "VARCHAR(100)", "not_null": True}
                    ],
                    "data": [{"name": "test"}]
                }
            ]
        }
    ]
}], indirect=True)
def test_with_bulk_tables(legacy_postgres_resource):
    # Test code here
    pass
```

## What happens when `load_bulk: True`:

1. Database is created
2. `bulk_tables.sql` is loaded (creates 60+ distraction tables with sample data)
3. Regular tables/SQL files are loaded
4. Test runs with all tables available

## What's in `bulk_tables.sql`:

- 60+ distraction tables including:
  - Employee tables (employees, departments, payroll, etc.)
  - Inventory tables (warehouses, stock_movements, etc.)
  - Marketing tables (campaigns, advertisements, etc.)
  - Financial tables (accounts, budgets, invoices, etc.)
  - IT tables (servers, applications, security_logs, etc.)
  - HR tables (job_positions, interviews, training, etc.)
  - Operations tables (maintenance, equipment, quality_checks, etc.)
  - Customer service tables (support_tickets, feedback, etc.)
  - And many more...

This is perfect for stress testing agents that need to analyze complex schemas and identify relevant tables for specific tasks.
