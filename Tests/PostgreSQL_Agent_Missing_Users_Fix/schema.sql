-- Create users table
CREATE TABLE users (
    id INT PRIMARY KEY,
    name TEXT NOT NULL
);

-- Create subscriptions table (intentionally bad design - no FK constraints)
CREATE TABLE subscriptions (
    user_id INT,  -- No PK, no FK constraints
    plan TEXT     -- Free text, not normalized
);

-- Insert test data
INSERT INTO users (id, name) VALUES
(1, 'Alice'),
(2, 'Bob'),
(3, 'Carol');  -- Carol has no subscription!

INSERT INTO subscriptions (user_id, plan) VALUES
(1, 'Pro'),
(2, 'Basic');
-- Carol (user_id 3) has no subscription row at all
