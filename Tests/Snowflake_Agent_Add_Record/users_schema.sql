-- users_schema.sql - Snowflake schema for users table with data loading
-- Variables: {{DB}}, {{SCHEMA}}, {{WAREHOUSE}}, {{ROLE}}
-- Optional S3 variables: {{BUCKET_URL}}, {{S3_KEY}}, {{AWS_KEY_ID}}, {{AWS_SECRET_KEY}}

USE ROLE {{ROLE}};
USE WAREHOUSE {{WAREHOUSE}};

-- Ensure we're using the correct database and schema
USE DATABASE {{DB}};
USE SCHEMA {{SCHEMA}};

-- Create file format for Parquet
CREATE OR REPLACE FILE FORMAT PARQUET_STD TYPE=PARQUET;

-- Create users table with proper schema
CREATE OR REPLACE TABLE USERS (
    USER_ID NUMBER PRIMARY KEY,
    FIRST_NAME VARCHAR(100) NOT NULL,
    LAST_NAME VARCHAR(100) NOT NULL,
    EMAIL VARCHAR(255) NOT NULL UNIQUE,
    AGE NUMBER,
    CITY VARCHAR(100),
    STATE VARCHAR(2),
    SIGNUP_DATE DATE,
    IS_ACTIVE BOOLEAN DEFAULT TRUE,
    TOTAL_PURCHASES DECIMAL(10,2) DEFAULT 0.00,
    CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Insert some initial test data
INSERT INTO USERS (USER_ID, FIRST_NAME, LAST_NAME, EMAIL, AGE, CITY, STATE, SIGNUP_DATE, IS_ACTIVE, TOTAL_PURCHASES) VALUES 
    (1, 'Alice', 'Smith', 'alice.smith@example.com', 25, 'New York', 'NY', '2024-01-15', TRUE, 150.50),
    (2, 'Bob', 'Jones', 'bob.jones@example.com', 32, 'Los Angeles', 'CA', '2024-02-20', TRUE, 89.99),
    (3, 'Carol', 'Brown', 'carol.brown@example.com', 28, 'Chicago', 'IL', '2024-03-10', FALSE, 0.00);

-- Optional: Load from S3 if S3 config is provided
-- This section will only work if S3 variables are substituted
-- CREATE TEMP STAGE _temp_stage
--   URL='{{BUCKET_URL}}'
--   CREDENTIALS=(AWS_KEY_ID='{{AWS_KEY_ID}}' AWS_SECRET_KEY='{{AWS_SECRET_KEY}}');
-- 
-- COPY INTO USERS
-- FROM @_temp_stage
-- FILES = ('{{S3_KEY}}')
-- FILE_FORMAT=(FORMAT_NAME=PARQUET_STD)
-- ON_ERROR=CONTINUE;

-- Verify data loaded
SELECT COUNT(*) AS total_users FROM USERS;
SELECT 'Schema setup complete' AS status;
