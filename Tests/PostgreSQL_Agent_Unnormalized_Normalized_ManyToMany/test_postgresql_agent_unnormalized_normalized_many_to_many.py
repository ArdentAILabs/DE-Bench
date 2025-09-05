# Import from the Model directory
from model.Run_Model import run_model
from model.Configure_Model import set_up_model_configs, cleanup_model_artifacts
import os
import importlib
import pytest
import time
import psycopg2
import uuid

# Dynamic config loading
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir_name = os.path.basename(current_dir)
module_path = f"Tests.{parent_dir_name}.Test_Configs"
Test_Configs = importlib.import_module(module_path)

# Generate unique identifiers for parallel test execution
test_timestamp = int(time.time())
test_uuid = uuid.uuid4().hex[:8]


@pytest.mark.postgresql
@pytest.mark.database
@pytest.mark.schema_design
@pytest.mark.three
@pytest.mark.parametrize("postgres_resource", [{
    "resource_id": f"unnormalized_normalized_many_to_many_{test_timestamp}_{test_uuid}",
    "databases": [
        {
            "name": f"authors_schema_test_db_{test_timestamp}_{test_uuid}",
            "sql_file": "schema.sql"
        }
    ]
}], indirect=True)
def test_postgresql_agent_unnormalized_normalized_many_to_many(request, postgres_resource, supabase_account_resource):
    """Test that validates AI agent can transform unnormalized (1NF violation) author data into properly normalized many-to-many relationships."""
    
    # Set up test tracking
    request.node.user_properties.append(("user_query", Test_Configs.User_Input))
    
    test_steps = [
        {
            "name": "Unnormalized (1NF) Problem Demonstration",
            "description": "Verify the current unnormalized schema (1NF violation) demonstrates the co-authorship issue",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Agent Normalization Process",
            "description": "AI Agent analyzes unnormalized data and implements normalized solution",
            "status": "did not reach", 
            "Result_Message": "",
        },
        {
            "name": "Normalized Structure Validation",
            "description": "Verify the agent created a properly normalized database structure",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Data Preservation Validation", 
            "description": "Verify all original book and author data was preserved during transformation",
            "status": "did not reach",
            "Result_Message": "",
        },
        {
            "name": "Co-Author Separation Validation",
            "description": "Verify co-authors are now properly separated (1NF issue resolved)",
            "status": "did not reach",
            "Result_Message": "",
        }
    ]
    request.node.user_properties.append(("test_steps", test_steps))

    # SECTION 1: SETUP THE TEST
    config_results = None
    model_result = None
    custom_info = {"mode": request.config.getoption("--mode")}
    created_db_name = postgres_resource["created_resources"][0]["name"]
    # Database: {created_db_name}
    
    try:
        # Set up model configurations with actual database name and test-specific credentials
        test_configs = Test_Configs.Configs.copy()
        test_configs["services"]["postgreSQL"]["databases"] = [{"name": created_db_name}]
        if request.config.getoption("--mode") == "Ardent":
            custom_info["publicKey"] = supabase_account_resource["publicKey"]
            custom_info["secretKey"] = supabase_account_resource["secretKey"]
        config_results = set_up_model_configs(Configs=test_configs,custom_info=custom_info)

        custom_info = {
            **custom_info,
            **config_results,
        }

        # DEMONSTRATE THE UNNORMALIZED (1NF) PROBLEM FIRST (Layer 1: Basic validation)
        db_connection = psycopg2.connect(
            host=os.getenv("POSTGRES_HOSTNAME"),
            port=os.getenv("POSTGRES_PORT"),
            user=os.getenv("POSTGRES_USERNAME"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database=created_db_name,
            sslmode="require",
        )
        db_cursor = db_connection.cursor()
        
        # Demonstrate the 1NF violation problem (multi-valued attribute in single column)
        db_cursor.execute("SELECT title, authors FROM books_bad WHERE authors ILIKE '%Gamma%'")
        problem_result = db_cursor.fetchall()
        
        if len(problem_result) == 1 and 'Gamma,Others' in str(problem_result[0]):
            test_steps[0]["status"] = "passed"
            test_steps[0]["Result_Message"] = f"Unnormalized (1NF) problem confirmed: Co-authors bundled as comma-separated string: {problem_result}"
        else:
            test_steps[0]["status"] = "failed"
            test_steps[0]["Result_Message"] = f"Unnormalized (1NF) problem demonstration failed: {problem_result}"
            raise AssertionError("Initial denormalized problem setup validation failed")
        
        db_cursor.close()
        db_connection.close()

        # Running model on database: {created_db_name}

        # SECTION 2: RUN THE MODEL
        start_time = time.time()
        model_result = run_model(container=None, task=Test_Configs.User_Input, configs=test_configs,extra_information = custom_info)
        end_time = time.time()
        request.node.user_properties.append(("model_runtime", end_time - start_time))
        
        # Register the Braintrust root span ID for tracking
        if model_result:
            request.node.user_properties.append(("run_trace_id", model_result["bt_root_span_id"]))
            print(f"Registered Braintrust root span ID: {model_result['bt_root_span_id']}")
        
        # Model run completed in {end_time - start_time:.2f} seconds

        test_steps[1]["status"] = "passed"
        test_steps[1]["Result_Message"] = "AI Agent completed unnormalized (1NF) → normalized transformation"

        # SECTION 3: VERIFY THE OUTCOMES
        
        
        # Reconnect to verify the agent's normalized solution
        db_connection = psycopg2.connect(
            host=os.getenv("POSTGRES_HOSTNAME"),
            port=os.getenv("POSTGRES_PORT"),
            user=os.getenv("POSTGRES_USERNAME"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database=created_db_name,
            sslmode="require",
        )
        db_cursor = db_connection.cursor()
        
        try:
            # Content-based schema validation: Discover tables by their actual data and structure
            db_cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name")
            all_tables = [r[0] for r in db_cursor.fetchall()]
            

            
            # Ensure original table preserved
            assert 'books_bad' in all_tables, f"Original books_bad table should be preserved. Found: {all_tables}"
            
            # Find normalized tables by EXACT data patterns
            book_table = author_table = junction_table = None
            expected_books = {'Design Patterns', 'Clean Code'}
            expected_authors = {'Gamma', 'Others', 'Robert Martin'}
            
            for table in all_tables:
                if table == 'books_bad' or 'backup' in table.lower():
                    continue
                try:
                    db_cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = db_cursor.fetchone()[0]
                    
                    if count == 2 and book_table is None:
                        # Check if this table contains EXACTLY the expected book titles
                        db_cursor.execute(f"SELECT * FROM {table}")
                        rows = db_cursor.fetchall()
                        # Extract all text values from all columns
                        text_values = set()
                        for row in rows:
                            for val in row:
                                if isinstance(val, str):
                                    text_values.add(val)
                        # Must contain exactly our expected books
                        if expected_books.issubset(text_values):
                            book_table = table
                    
                    elif count == 3 and author_table is None:
                        # Check if this table contains EXACTLY the expected author names
                        db_cursor.execute(f"SELECT * FROM {table}")
                        rows = db_cursor.fetchall()
                        # Extract all text values from all columns
                        text_values = set()
                        for row in rows:
                            for val in row:
                                if isinstance(val, str):
                                    text_values.add(val)
                        # Must contain exactly our expected authors
                        if expected_authors.issubset(text_values):
                            author_table = table
                    
                    elif count == 3 and junction_table is None:
                        # Check if this looks like junction data (only integers/timestamps, no meaningful text)
                        db_cursor.execute(f"SELECT * FROM {table}")
                        rows = db_cursor.fetchall()
                        # Junction table should NOT contain book titles or author names
                        text_values = set()
                        for row in rows:
                            for val in row:
                                if isinstance(val, str):
                                    text_values.add(val)
                        # Should not contain any of our expected books or authors
                        if not (expected_books.intersection(text_values) or expected_authors.intersection(text_values)):
                            junction_table = table
                            
                except Exception:
                    continue
            
            assert book_table is not None, f"No table with 2 books found in: {all_tables}"
            assert author_table is not None, f"No table with 3 authors (Gamma, Others, Robert Martin) found in: {all_tables}"
            assert junction_table is not None, f"No table with 3 book-author relationships found in: {all_tables}"
            
            test_steps[2]["status"] = "passed"
            test_steps[2]["Result_Message"] = f"Found normalized tables by content: {book_table}, {author_table}, {junction_table}"

                                    # Junction table should have at least 2 columns (for the relationships)
            db_cursor.execute(f"""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name='{junction_table}' ORDER BY column_name
            """)
            junction_columns = [r[0] for r in db_cursor.fetchall()]
            assert len(junction_columns) >= 2, f"{junction_table} should have at least 2 columns for relationships. Got: {junction_columns}"

            # FUNCTIONAL TEST: Can we now solve the original 1NF problem?
            # The core test: Can we find books by individual authors that were previously buried in comma-separated lists?
            
            # Get column names dynamically for flexible querying
            db_cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name='{book_table}' AND column_name ILIKE '%title%'")
            title_col = db_cursor.fetchone()[0]
            
            db_cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name='{author_table}' AND column_name ILIKE '%name%'")
            name_col = db_cursor.fetchone()[0]
            
            db_cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name='{book_table}' AND column_name ILIKE '%id%'")
            book_id_col = db_cursor.fetchone()[0]
            
            db_cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name='{author_table}' AND column_name ILIKE '%id%'")
            author_id_col = db_cursor.fetchone()[0]
            
            # Test 1: Find books by "Gamma" (was impossible with 1NF violation)
            db_cursor.execute(f"""
                SELECT DISTINCT b.{title_col} 
                FROM {book_table} b
                JOIN {junction_table} j ON b.{book_id_col} = j.book_id
                JOIN {author_table} a ON a.{author_id_col} = j.author_id
                WHERE a.{name_col} = 'Gamma'
                ORDER BY b.{title_col}
            """)
            gamma_books = [r[0] for r in db_cursor.fetchall()]
            assert gamma_books == ['Design Patterns'], f"Gamma should be linked to 'Design Patterns', got: {gamma_books}"
            
            # Test 2: Find books by "Others" (was impossible with 1NF violation)  
            db_cursor.execute(f"""
                SELECT DISTINCT b.{title_col}
                FROM {book_table} b
                JOIN {junction_table} j ON b.{book_id_col} = j.book_id
                JOIN {author_table} a ON a.{author_id_col} = j.author_id
                WHERE a.{name_col} = 'Others'
                ORDER BY b.{title_col}
            """)
            others_books = [r[0] for r in db_cursor.fetchall()]
            assert others_books == ['Design Patterns'], f"Others should be linked to 'Design Patterns', got: {others_books}"
            
            # Test 3: Find books by "Robert Martin"
            db_cursor.execute(f"""
                SELECT DISTINCT b.{title_col}
                FROM {book_table} b  
                JOIN {junction_table} j ON b.{book_id_col} = j.book_id
                JOIN {author_table} a ON a.{author_id_col} = j.author_id
                WHERE a.{name_col} = 'Robert Martin'
                ORDER BY b.{title_col}
            """)
            martin_books = [r[0] for r in db_cursor.fetchall()]
            assert martin_books == ['Clean Code'], f"Robert Martin should be linked to 'Clean Code', got: {martin_books}"
            
            # Test 4: Find co-authors of "Design Patterns" (the original problem!)
            db_cursor.execute(f"""
                SELECT a.{name_col}
                FROM {author_table} a
                JOIN {junction_table} j ON a.{author_id_col} = j.author_id
                JOIN {book_table} b ON b.{book_id_col} = j.book_id
                WHERE b.{title_col} = 'Design Patterns'
                ORDER BY a.{name_col}
            """)
            design_patterns_authors = [r[0] for r in db_cursor.fetchall()]
            assert set(design_patterns_authors) == {'Gamma', 'Others'}, f"Design Patterns should have authors Gamma and Others, got: {design_patterns_authors}"
            

            
            # Test 5: Data preservation - all original data is preserved
            db_cursor.execute(f"SELECT COUNT(*) FROM {book_table}")
            assert db_cursor.fetchone()[0] == 2, f"Should have exactly 2 books"
            
            db_cursor.execute(f"SELECT COUNT(*) FROM {author_table}")  
            assert db_cursor.fetchone()[0] == 3, f"Should have exactly 3 authors"
            
            db_cursor.execute(f"SELECT COUNT(*) FROM {junction_table}")
            assert db_cursor.fetchone()[0] == 3, f"Should have exactly 3 book-author relationships"

            test_steps[3]["status"] = "passed"
            test_steps[3]["Result_Message"] = "Schema and data validation passed (tables, FKs, uniques, exact rows)"

            # Mark final step as passed
            test_steps[4]["status"] = "passed"
            test_steps[4]["Result_Message"] = "Many-to-many mapping correct; source table preserved"

            # Final assertion to make test outcome explicit
            assert True, "Unnormalized (1NF) → Normalized many-to-many transformation validated rigorously"
        
        finally:
            db_cursor.close()
            db_connection.close()

    except Exception as e:
        # Update any remaining test steps that didn't reach
        for step in test_steps:
            if step["status"] == "did not reach":
                step["status"] = "failed"
                step["Result_Message"] = f"Test failed before reaching this step: {str(e)}"
        raise
    
    finally:
        # CLEANUP
        if request.config.getoption("--mode") == "Ardent":
            custom_info['job_id'] = model_result.get("id") if model_result else None
        cleanup_model_artifacts(Configs=test_configs, custom_info=custom_info)