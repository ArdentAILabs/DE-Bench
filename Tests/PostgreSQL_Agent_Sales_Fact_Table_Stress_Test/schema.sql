-- ============================================================================
-- CORE SALES TABLES (These should be identified by the agent)
-- ============================================================================

-- Transactions table
CREATE TABLE transactions (
    transaction_id SERIAL PRIMARY KEY,
    transaction_date DATE NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL,
    payment_method VARCHAR(50),
    store_location VARCHAR(100),
    cashier_id INTEGER,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Customers table
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE,
    phone VARCHAR(20),
    address TEXT,
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(10),
    customer_since DATE,
    loyalty_points INTEGER DEFAULT 0
);

-- Products table
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    subcategory VARCHAR(100),
    unit_price DECIMAL(10,2) NOT NULL,
    cost_price DECIMAL(10,2),
    supplier_id INTEGER,
    sku VARCHAR(100) UNIQUE,
    description TEXT,
    in_stock BOOLEAN DEFAULT TRUE
);

-- Insert sample data for core tables
INSERT INTO transactions (transaction_date, total_amount, payment_method, store_location, cashier_id) VALUES
('2024-01-15', 125.50, 'credit_card', 'Downtown Store', 1),
('2024-01-15', 89.99, 'cash', 'Mall Location', 2),
('2024-01-16', 234.75, 'debit_card', 'Airport Store', 1),
('2024-01-16', 67.25, 'credit_card', 'Downtown Store', 3),
('2024-01-17', 156.80, 'cash', 'Mall Location', 2);

INSERT INTO customers (first_name, last_name, email, phone, address, city, state, zip_code, customer_since, loyalty_points) VALUES
('John', 'Doe', 'john.doe@email.com', '555-0101', '123 Main St', 'New York', 'NY', '10001', '2023-06-15', 150),
('Jane', 'Smith', 'jane.smith@email.com', '555-0102', '456 Oak Ave', 'Los Angeles', 'CA', '90210', '2023-08-20', 275),
('Bob', 'Johnson', 'bob.johnson@email.com', '555-0103', '789 Pine Rd', 'Chicago', 'IL', '60601', '2023-09-10', 80),
('Alice', 'Brown', 'alice.brown@email.com', '555-0104', '321 Elm St', 'Houston', 'TX', '77001', '2023-11-05', 200),
('Charlie', 'Wilson', 'charlie.wilson@email.com', '555-0105', '654 Maple Dr', 'Phoenix', 'AZ', '85001', '2023-12-01', 120);

INSERT INTO products (product_name, category, subcategory, unit_price, cost_price, supplier_id, sku, description, in_stock) VALUES
('Wireless Headphones', 'Electronics', 'Audio', 99.99, 45.00, 1, 'WH-001', 'High-quality wireless headphones', TRUE),
('Coffee Maker', 'Appliances', 'Kitchen', 89.99, 35.00, 2, 'CM-002', 'Programmable coffee maker', TRUE),
('Running Shoes', 'Clothing', 'Footwear', 129.99, 55.00, 3, 'RS-003', 'Comfortable running shoes', TRUE),
('Laptop Stand', 'Electronics', 'Accessories', 49.99, 20.00, 1, 'LS-004', 'Adjustable laptop stand', TRUE),
('Water Bottle', 'Sports', 'Hydration', 24.99, 10.00, 4, 'WB-005', 'Insulated water bottle', TRUE);
