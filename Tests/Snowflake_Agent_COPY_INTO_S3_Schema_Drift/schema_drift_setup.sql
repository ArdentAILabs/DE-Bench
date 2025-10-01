-- schema_drift_setup.sql - Snowflake schema for testing schema drift with COPY INTO
-- Variables: {{DB}}, {{SCHEMA}}, {{WAREHOUSE}}, {{ROLE}}
-- S3 variables: {{BUCKET_URL}}, {{S3_KEY}}, {{AWS_ACCESS_KEY}}, {{AWS_SECRET_KEY}}

USE ROLE {{ROLE}};
USE WAREHOUSE {{WAREHOUSE}};

-- Ensure we're using the correct database and schema
USE DATABASE {{DB}};
USE SCHEMA {{SCHEMA}};

-- Create file format for CSV with schema evolution support
CREATE OR REPLACE FILE FORMAT CSV_SCHEMA_EVOLUTION_FORMAT 
TYPE=CSV 
FIELD_DELIMITER=',' 
RECORD_DELIMITER='\n' 
SKIP_HEADER=1 
ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE
TRIM_SPACE=TRUE
NULL_IF=('NULL', 'null', '');

-- Create temporary stage for S3 access
CREATE OR REPLACE STAGE CUSTOMER_DATA_STAGE
  URL='{{BUCKET_URL}}'
  CREDENTIALS=(AWS_KEY_ID='{{AWS_ACCESS_KEY}}' AWS_SECRET_KEY='{{AWS_SECRET_KEY}}')
  FILE_FORMAT=CSV_SCHEMA_EVOLUTION_FORMAT;

-- Create staging table with flexible schema for evolution
CREATE OR REPLACE TABLE CUSTOMER_STAGING (
    CUSTOMER_ID NUMBER,
    NAME VARCHAR(100),
    EMAIL VARCHAR(255),
    SIGNUP_DATE DATE,
    PHONE VARCHAR(20),
    ADDRESS VARCHAR(500),
    CUSTOMER_TYPE VARCHAR(50),
    LOYALTY_POINTS NUMBER,
    LOAD_TIMESTAMP TIMESTAMP_NTZ
);

-- Create production table with core required fields
-- Note: PRIMARY KEY is informational only in Snowflake (not enforced)
CREATE OR REPLACE TABLE CUSTOMERS (
    CUSTOMER_ID NUMBER,
    NAME VARCHAR(100),
    EMAIL VARCHAR(255),
    SIGNUP_DATE DATE,
    PHONE VARCHAR(20),
    ADDRESS VARCHAR(500),
    CUSTOMER_TYPE VARCHAR(50),
    LOYALTY_POINTS NUMBER,
    CREATED_AT TIMESTAMP_NTZ,
    UPDATED_AT TIMESTAMP_NTZ
);

-- Create initial sample data files in staging (simulate first file schema)
INSERT INTO CUSTOMER_STAGING (CUSTOMER_ID, NAME, EMAIL, SIGNUP_DATE, LOAD_TIMESTAMP)
VALUES
(1, 'John Smith', 'john.smith@email.com', '2024-01-15', CURRENT_TIMESTAMP()),
(2, 'Jane Doe', 'jane.doe@email.com', '2024-01-20', CURRENT_TIMESTAMP()),
(3, 'Bob Wilson', 'bob.wilson@email.com', '2024-01-25', CURRENT_TIMESTAMP());

-- Create a view to handle schema evolution gracefully
CREATE OR REPLACE VIEW CUSTOMER_UNIFIED_VIEW AS
SELECT 
    CUSTOMER_ID,
    NAME,
    EMAIL,
    SIGNUP_DATE,
    COALESCE(PHONE, 'N/A') AS PHONE,
    COALESCE(ADDRESS, 'Unknown') AS ADDRESS,
    COALESCE(CUSTOMER_TYPE, 'STANDARD') AS CUSTOMER_TYPE,
    COALESCE(LOYALTY_POINTS, 0) AS LOYALTY_POINTS,
    LOAD_TIMESTAMP
FROM CUSTOMER_STAGING;

-- Test initial setup
SELECT 'Initial setup complete - ready for schema drift testing' AS status;
SELECT COUNT(*) AS initial_record_count FROM CUSTOMER_STAGING;
