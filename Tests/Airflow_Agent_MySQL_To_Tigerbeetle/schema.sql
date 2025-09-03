-- Create tables for Plaid and Finch access tokens
CREATE TABLE IF NOT EXISTS plaid_access_tokens (
    company_id VARCHAR(50) PRIMARY KEY,
    access_token VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS finch_access_tokens (
    company_id VARCHAR(50) PRIMARY KEY,
    access_token VARCHAR(255) NOT NULL
);

-- Insert test data
INSERT IGNORE INTO plaid_access_tokens (company_id, access_token) 
VALUES ('123', 'test_plaid_token');

INSERT IGNORE INTO finch_access_tokens (company_id, access_token) 
VALUES ('123', 'test_finch_token');
