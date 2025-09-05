-- Create users table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    age INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create customers table (linked to users)
CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    phone VARCHAR(20),
    address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create orders table (linked to customers)
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(50) DEFAULT 'Pending'
);

-- Create payments table (linked to orders)
CREATE TABLE payments (
    id SERIAL PRIMARY KEY,
    order_id INT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    payment_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    amount DECIMAL(10, 2) NOT NULL,
    method VARCHAR(50) NOT NULL, -- e.g., 'Credit Card', 'PayPal'
    status VARCHAR(50) DEFAULT 'Completed'
);

-- Insert initial test data
INSERT INTO users (name, email, age) VALUES
('John Doe', 'john@example.com', 30),
('Jane Smith', 'jane@example.com', 25),
('Bob Johnson', 'bob@example.com', 35);

-- Insert customers (linking to users)
INSERT INTO customers (user_id, phone, address) VALUES
(1, '123-456-7890', '123 Main St, Springfield'),
(2, '555-123-4567', '456 Oak Ave, Shelbyville'),
(3, '987-654-3210', '789 Pine Rd, Capital City');

-- Insert orders
INSERT INTO orders (customer_id, total_amount, status) VALUES
(1, 250.00, 'Shipped'),
(1, 100.00, 'Pending'),
(2, 75.50, 'Processing'),
(3, 500.00, 'Delivered');

-- Insert payments
INSERT INTO payments (order_id, amount, method, status) VALUES
(1, 250.00, 'Credit Card', 'Completed'),
(2, 50.00, 'PayPal', 'Completed'),
(2, 50.00, 'PayPal', 'Refunded'),
(3, 75.50, 'Credit Card', 'Completed'),
(4, 500.00, 'Bank Transfer', 'Completed');
