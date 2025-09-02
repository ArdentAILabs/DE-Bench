-- users_schema.sql - Snowflake schema for users table with S3 parquet data loading
-- Variables: {{DB}}, {{SCHEMA}}, {{WAREHOUSE}}, {{ROLE}}
-- S3 variables: {{BUCKET_URL}}, {{S3_KEY}}, {{AWS_ACCESS_KEY}}, {{AWS_SECRET_KEY}}

USE ROLE {{ROLE}};
USE WAREHOUSE {{WAREHOUSE}};

-- Ensure we're using the correct database and schema
USE DATABASE {{DB}};
USE SCHEMA {{SCHEMA}};

-- Create file format for Parquet
CREATE OR REPLACE FILE FORMAT PARQUET_STD TYPE=PARQUET;

-- Create temporary stage for S3 access
CREATE OR REPLACE TEMP STAGE _temp_stage
  URL='{{BUCKET_URL}}'
  CREDENTIALS=(AWS_KEY_ID='{{AWS_ACCESS_KEY}}' AWS_SECRET_KEY='{{AWS_SECRET_KEY}}');

-- Create table with explicit schema (safer than INFER_SCHEMA)
CREATE OR REPLACE TABLE USERS (
    USER_ID NUMBER,
    FIRST_NAME VARCHAR(100),
    LAST_NAME VARCHAR(100),
    EMAIL VARCHAR(255),
    AGE NUMBER,
    CITY VARCHAR(100),
    STATE VARCHAR(2),
    SIGNUP_DATE TIMESTAMP,  -- Changed from DATE to TIMESTAMP to handle parquet data
    IS_ACTIVE BOOLEAN,
    TOTAL_PURCHASES DECIMAL(10,2)
);

-- Load data from S3 parquet file
COPY INTO USERS
FROM @_temp_stage
FILES = ('{{S3_KEY}}')
FILE_FORMAT=(FORMAT_NAME=PARQUET_STD)
MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
ON_ERROR=ABORT_STATEMENT;

-- Verify data loaded
SELECT COUNT(*) AS total_users FROM USERS;
SELECT 'Schema setup complete' AS status;
