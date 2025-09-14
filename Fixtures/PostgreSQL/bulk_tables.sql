-- ============================================================================
-- DISTRACTION TABLES (60+ tables to test schema analysis)
-- ============================================================================

-- Employee tables
CREATE TABLE employees (
    employee_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    department VARCHAR(100),
    hire_date DATE,
    salary DECIMAL(10,2)
);

CREATE TABLE departments (
    department_id SERIAL PRIMARY KEY,
    department_name VARCHAR(100),
    manager_id INTEGER,
    budget DECIMAL(12,2)
);

CREATE TABLE employee_benefits (
    benefit_id SERIAL PRIMARY KEY,
    employee_id INTEGER,
    benefit_type VARCHAR(100),
    amount DECIMAL(10,2),
    start_date DATE
);

CREATE TABLE payroll (
    payroll_id SERIAL PRIMARY KEY,
    employee_id INTEGER,
    pay_period_start DATE,
    pay_period_end DATE,
    gross_pay DECIMAL(10,2),
    net_pay DECIMAL(10,2)
);

CREATE TABLE time_off_requests (
    request_id SERIAL PRIMARY KEY,
    employee_id INTEGER,
    start_date DATE,
    end_date DATE,
    reason VARCHAR(255),
    status VARCHAR(50)
);

-- Inventory tables
CREATE TABLE inventory (
    inventory_id SERIAL PRIMARY KEY,
    product_id INTEGER,
    warehouse_id INTEGER,
    quantity_on_hand INTEGER,
    reorder_level INTEGER,
    last_updated TIMESTAMP
);

CREATE TABLE warehouses (
    warehouse_id SERIAL PRIMARY KEY,
    warehouse_name VARCHAR(100),
    location VARCHAR(255),
    capacity INTEGER,
    manager_id INTEGER
);

CREATE TABLE stock_movements (
    movement_id SERIAL PRIMARY KEY,
    product_id INTEGER,
    warehouse_id INTEGER,
    movement_type VARCHAR(50),
    quantity INTEGER,
    movement_date TIMESTAMP
);

CREATE TABLE suppliers (
    supplier_id SERIAL PRIMARY KEY,
    supplier_name VARCHAR(255),
    contact_person VARCHAR(100),
    phone VARCHAR(20),
    email VARCHAR(255),
    address TEXT
);

CREATE TABLE purchase_orders (
    po_id SERIAL PRIMARY KEY,
    supplier_id INTEGER,
    order_date DATE,
    expected_delivery DATE,
    total_amount DECIMAL(10,2),
    status VARCHAR(50)
);

-- Marketing tables
CREATE TABLE campaigns (
    campaign_id SERIAL PRIMARY KEY,
    campaign_name VARCHAR(255),
    start_date DATE,
    end_date DATE,
    budget DECIMAL(10,2),
    target_audience VARCHAR(255)
);

CREATE TABLE advertisements (
    ad_id SERIAL PRIMARY KEY,
    campaign_id INTEGER,
    ad_type VARCHAR(100),
    platform VARCHAR(100),
    cost DECIMAL(10,2),
    impressions INTEGER
);

CREATE TABLE customer_segments (
    segment_id SERIAL PRIMARY KEY,
    segment_name VARCHAR(100),
    criteria TEXT,
    customer_count INTEGER
);

CREATE TABLE promotions (
    promotion_id SERIAL PRIMARY KEY,
    promotion_name VARCHAR(255),
    discount_percentage DECIMAL(5,2),
    start_date DATE,
    end_date DATE,
    applicable_products TEXT
);

CREATE TABLE loyalty_programs (
    program_id SERIAL PRIMARY KEY,
    program_name VARCHAR(100),
    points_per_dollar DECIMAL(5,2),
    redemption_rate DECIMAL(5,2),
    active BOOLEAN
);

-- Financial tables
CREATE TABLE accounts (
    account_id SERIAL PRIMARY KEY,
    account_name VARCHAR(255),
    account_type VARCHAR(100),
    balance DECIMAL(15,2),
    currency VARCHAR(3)
);

CREATE TABLE transactions_financial (
    transaction_id SERIAL PRIMARY KEY,
    account_id INTEGER,
    transaction_type VARCHAR(100),
    amount DECIMAL(15,2),
    description TEXT,
    transaction_date TIMESTAMP
);

CREATE TABLE budgets (
    budget_id SERIAL PRIMARY KEY,
    department_id INTEGER,
    fiscal_year INTEGER,
    allocated_amount DECIMAL(12,2),
    spent_amount DECIMAL(12,2)
);

CREATE TABLE invoices (
    invoice_id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    invoice_date DATE,
    due_date DATE,
    total_amount DECIMAL(10,2),
    status VARCHAR(50)
);

CREATE TABLE payments (
    payment_id SERIAL PRIMARY KEY,
    invoice_id INTEGER,
    payment_date DATE,
    amount DECIMAL(10,2),
    payment_method VARCHAR(50)
);

-- IT tables
CREATE TABLE servers (
    server_id SERIAL PRIMARY KEY,
    server_name VARCHAR(100),
    ip_address VARCHAR(15),
    operating_system VARCHAR(100),
    cpu_cores INTEGER,
    memory_gb INTEGER
);

CREATE TABLE applications (
    app_id SERIAL PRIMARY KEY,
    app_name VARCHAR(255),
    version VARCHAR(50),
    server_id INTEGER,
    status VARCHAR(50),
    last_deployed TIMESTAMP
);

CREATE TABLE user_accounts (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(100),
    email VARCHAR(255),
    role VARCHAR(100),
    last_login TIMESTAMP,
    active BOOLEAN
);

CREATE TABLE security_logs (
    log_id SERIAL PRIMARY KEY,
    user_id INTEGER,
    action VARCHAR(100),
    ip_address VARCHAR(15),
    timestamp TIMESTAMP,
    success BOOLEAN
);

CREATE TABLE backups (
    backup_id SERIAL PRIMARY KEY,
    server_id INTEGER,
    backup_type VARCHAR(100),
    size_gb DECIMAL(10,2),
    backup_date TIMESTAMP,
    status VARCHAR(50)
);

-- HR tables
CREATE TABLE job_positions (
    position_id SERIAL PRIMARY KEY,
    title VARCHAR(255),
    department_id INTEGER,
    salary_range_min DECIMAL(10,2),
    salary_range_max DECIMAL(10,2),
    requirements TEXT
);

CREATE TABLE interviews (
    interview_id SERIAL PRIMARY KEY,
    candidate_name VARCHAR(255),
    position_id INTEGER,
    interview_date DATE,
    interviewer_id INTEGER,
    rating INTEGER,
    notes TEXT
);

CREATE TABLE training_programs (
    program_id SERIAL PRIMARY KEY,
    program_name VARCHAR(255),
    duration_hours INTEGER,
    cost DECIMAL(10,2),
    instructor VARCHAR(255),
    description TEXT
);

CREATE TABLE performance_reviews (
    review_id SERIAL PRIMARY KEY,
    employee_id INTEGER,
    review_period_start DATE,
    review_period_end DATE,
    overall_rating INTEGER,
    goals_met INTEGER,
    goals_total INTEGER
);

CREATE TABLE certifications (
    cert_id SERIAL PRIMARY KEY,
    employee_id INTEGER,
    certification_name VARCHAR(255),
    issuing_organization VARCHAR(255),
    issue_date DATE,
    expiry_date DATE
);

-- Operations tables
CREATE TABLE maintenance_schedules (
    schedule_id SERIAL PRIMARY KEY,
    equipment_id INTEGER,
    maintenance_type VARCHAR(100),
    scheduled_date DATE,
    technician_id INTEGER,
    estimated_duration INTEGER
);

CREATE TABLE equipment (
    equipment_id SERIAL PRIMARY KEY,
    equipment_name VARCHAR(255),
    equipment_type VARCHAR(100),
    location VARCHAR(255),
    purchase_date DATE,
    warranty_expiry DATE
);

CREATE TABLE work_orders (
    work_order_id SERIAL PRIMARY KEY,
    equipment_id INTEGER,
    priority VARCHAR(50),
    description TEXT,
    assigned_to INTEGER,
    status VARCHAR(50)
);

CREATE TABLE quality_checks (
    check_id SERIAL PRIMARY KEY,
    product_id INTEGER,
    check_type VARCHAR(100),
    result VARCHAR(50),
    inspector_id INTEGER,
    check_date TIMESTAMP
);

CREATE TABLE safety_incidents (
    incident_id SERIAL PRIMARY KEY,
    employee_id INTEGER,
    incident_date DATE,
    incident_type VARCHAR(100),
    severity VARCHAR(50),
    description TEXT
);

-- Customer service tables
CREATE TABLE support_tickets (
    ticket_id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    issue_type VARCHAR(100),
    priority VARCHAR(50),
    status VARCHAR(50),
    created_date TIMESTAMP
);

CREATE TABLE customer_feedback (
    feedback_id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    rating INTEGER,
    comments TEXT,
    feedback_date TIMESTAMP,
    category VARCHAR(100)
);

CREATE TABLE chat_sessions (
    session_id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    agent_id INTEGER,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    resolution_status VARCHAR(50)
);

CREATE TABLE knowledge_base (
    article_id SERIAL PRIMARY KEY,
    title VARCHAR(255),
    content TEXT,
    category VARCHAR(100),
    last_updated TIMESTAMP,
    views INTEGER
);

CREATE TABLE faq (
    faq_id SERIAL PRIMARY KEY,
    question TEXT,
    answer TEXT,
    category VARCHAR(100),
    helpful_votes INTEGER
);

-- Sales and marketing analytics tables
CREATE TABLE website_analytics (
    analytics_id SERIAL PRIMARY KEY,
    page_url VARCHAR(500),
    visitors INTEGER,
    page_views INTEGER,
    bounce_rate DECIMAL(5,2),
    date DATE
);

CREATE TABLE social_media_posts (
    post_id SERIAL PRIMARY KEY,
    platform VARCHAR(100),
    content TEXT,
    post_date TIMESTAMP,
    likes INTEGER,
    shares INTEGER
);

CREATE TABLE email_campaigns (
    campaign_id SERIAL PRIMARY KEY,
    subject_line VARCHAR(255),
    recipient_count INTEGER,
    open_rate DECIMAL(5,2),
    click_rate DECIMAL(5,2),
    sent_date TIMESTAMP
);

CREATE TABLE conversion_funnels (
    funnel_id SERIAL PRIMARY KEY,
    funnel_name VARCHAR(255),
    step_name VARCHAR(255),
    step_order INTEGER,
    conversion_rate DECIMAL(5,2)
);

CREATE TABLE a_b_tests (
    test_id SERIAL PRIMARY KEY,
    test_name VARCHAR(255),
    variant_a_visitors INTEGER,
    variant_b_visitors INTEGER,
    variant_a_conversions INTEGER,
    variant_b_conversions INTEGER
);

-- Product development tables
CREATE TABLE product_features (
    feature_id SERIAL PRIMARY KEY,
    product_id INTEGER,
    feature_name VARCHAR(255),
    description TEXT,
    development_status VARCHAR(50),
    estimated_effort INTEGER
);

CREATE TABLE user_stories (
    story_id SERIAL PRIMARY KEY,
    product_id INTEGER,
    story_title VARCHAR(255),
    description TEXT,
    priority VARCHAR(50),
    story_points INTEGER
);

CREATE TABLE bug_reports (
    bug_id SERIAL PRIMARY KEY,
    product_id INTEGER,
    bug_title VARCHAR(255),
    description TEXT,
    severity VARCHAR(50),
    status VARCHAR(50),
    reported_by INTEGER
);

CREATE TABLE releases (
    release_id SERIAL PRIMARY KEY,
    product_id INTEGER,
    version_number VARCHAR(50),
    release_date DATE,
    release_notes TEXT,
    status VARCHAR(50)
);

CREATE TABLE user_feedback (
    feedback_id SERIAL PRIMARY KEY,
    product_id INTEGER,
    user_id INTEGER,
    feedback_type VARCHAR(100),
    rating INTEGER,
    comments TEXT
);

-- Supply chain tables
CREATE TABLE shipping_methods (
    method_id SERIAL PRIMARY KEY,
    method_name VARCHAR(100),
    delivery_time_days INTEGER,
    cost_per_kg DECIMAL(10,2),
    tracking_available BOOLEAN
);

CREATE TABLE delivery_routes (
    route_id SERIAL PRIMARY KEY,
    route_name VARCHAR(255),
    start_location VARCHAR(255),
    end_location VARCHAR(255),
    distance_km DECIMAL(10,2),
    estimated_time_hours INTEGER
);

CREATE TABLE customs_documents (
    document_id SERIAL PRIMARY KEY,
    shipment_id INTEGER,
    document_type VARCHAR(100),
    document_number VARCHAR(100),
    issue_date DATE,
    expiry_date DATE
);

CREATE TABLE compliance_checks (
    check_id SERIAL PRIMARY KEY,
    product_id INTEGER,
    regulation_type VARCHAR(100),
    compliance_status VARCHAR(50),
    check_date DATE,
    inspector VARCHAR(255)
);

CREATE TABLE sustainability_metrics (
    metric_id SERIAL PRIMARY KEY,
    product_id INTEGER,
    carbon_footprint DECIMAL(10,2),
    water_usage DECIMAL(10,2),
    recyclable_percentage DECIMAL(5,2),
    measurement_date DATE
);

-- Legal and compliance tables
CREATE TABLE contracts (
    contract_id SERIAL PRIMARY KEY,
    contract_name VARCHAR(255),
    party_a VARCHAR(255),
    party_b VARCHAR(255),
    start_date DATE,
    end_date DATE,
    value DECIMAL(15,2)
);

CREATE TABLE legal_documents (
    document_id SERIAL PRIMARY KEY,
    document_type VARCHAR(100),
    title VARCHAR(255),
    content TEXT,
    effective_date DATE,
    expiry_date DATE
);

CREATE TABLE compliance_audits (
    audit_id SERIAL PRIMARY KEY,
    audit_type VARCHAR(100),
    audit_date DATE,
    auditor VARCHAR(255),
    findings TEXT,
    recommendations TEXT
);

CREATE TABLE intellectual_property (
    ip_id SERIAL PRIMARY KEY,
    ip_type VARCHAR(100),
    title VARCHAR(255),
    registration_number VARCHAR(100),
    registration_date DATE,
    owner VARCHAR(255)
);

CREATE TABLE data_privacy_requests (
    request_id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    request_type VARCHAR(100),
    request_date DATE,
    status VARCHAR(50),
    response_date DATE
);

-- Research and development tables
CREATE TABLE research_projects (
    project_id SERIAL PRIMARY KEY,
    project_name VARCHAR(255),
    start_date DATE,
    end_date DATE,
    budget DECIMAL(12,2),
    lead_researcher VARCHAR(255)
);

CREATE TABLE patents (
    patent_id SERIAL PRIMARY KEY,
    patent_number VARCHAR(100),
    title VARCHAR(255),
    inventor VARCHAR(255),
    filing_date DATE,
    grant_date DATE
);

CREATE TABLE laboratory_tests (
    test_id SERIAL PRIMARY KEY,
    project_id INTEGER,
    test_name VARCHAR(255),
    test_date DATE,
    results TEXT,
    success BOOLEAN
);

CREATE TABLE research_publications (
    publication_id SERIAL PRIMARY KEY,
    title VARCHAR(255),
    authors TEXT,
    journal VARCHAR(255),
    publication_date DATE,
    impact_factor DECIMAL(5,2)
);

CREATE TABLE innovation_metrics (
    metric_id SERIAL PRIMARY KEY,
    project_id INTEGER,
    metric_name VARCHAR(255),
    value DECIMAL(10,2),
    unit VARCHAR(50),
    measurement_date DATE
);

-- Insert sample data for all distraction tables
INSERT INTO employees (first_name, last_name, department, hire_date, salary) VALUES
('Alice', 'Johnson', 'HR', '2023-01-15', 65000),
('Bob', 'Smith', 'IT', '2023-02-20', 75000),
('Carol', 'Davis', 'Finance', '2023-03-10', 70000),
('Dave', 'Brown', 'Sales', '2023-04-15', 80000),
('Eve', 'Green', 'Marketing', '2023-05-20', 90000),
('Frank', 'Jones', 'Engineering', '2023-06-10', 100000),
('George', 'White', 'Customer Service', '2023-07-15', 110000),
('Hannah', 'Black', 'Product Management', '2023-08-20', 120000),
('Isaac', 'Gray', 'Research and Development', '2023-09-10', 130000),
('James', 'Purple', 'Quality Assurance', '2023-10-15', 140000);

INSERT INTO departments (department_name, manager_id, budget) VALUES
('HR', 1, 500000),
('IT', 2, 800000),
('Finance', 3, 600000),
('Sales', 4, 700000),
('Marketing', 5, 800000),
('Engineering', 6, 900000),
('Customer Service', 7, 1000000),
('Product Management', 8, 1100000),
('Research and Development', 9, 1200000),
('Quality Assurance', 10, 1300000);

INSERT INTO employee_benefits (employee_id, benefit_type, amount, start_date) VALUES
(1, 'Health Insurance', 500, '2023-01-15'),
(2, 'Dental Insurance', 200, '2023-02-20'),
(3, 'Vision Insurance', 150, '2023-03-10'),
(4, 'Retirement Plan', 1000, '2023-04-15'),
(5, 'Stock Options', 500, '2023-05-20'),
(6, 'Professional Development', 200, '2023-06-10'),
(7, 'Life Insurance', 1000, '2023-07-15'),
(8, 'Paid Time Off', 1000, '2023-08-20'),
(9, 'Performance Bonus', 1000, '2023-09-10'),
(10, 'Holiday Bonus', 1000, '2023-10-15');

INSERT INTO payroll (employee_id, pay_period_start, pay_period_end, gross_pay, net_pay) VALUES
(1, '2024-01-01', '2024-01-15', 2500, 2000),
(2, '2024-01-01', '2024-01-15', 2885, 2300),
(3, '2024-01-01', '2024-01-15', 2692, 2150),
(4, '2024-01-01', '2024-01-15', 2800, 2240),
(5, '2024-01-01', '2024-01-15', 2950, 2360),
(6, '2024-01-01', '2024-01-15', 3100, 2480),
(7, '2024-01-01', '2024-01-15', 3250, 2600),
(8, '2024-01-01', '2024-01-15', 3400, 2720),
(9, '2024-01-01', '2024-01-15', 3550, 2840),
(10, '2024-01-01', '2024-01-15', 3700, 2960);

INSERT INTO time_off_requests (employee_id, start_date, end_date, reason, status) VALUES
(1, '2024-02-01', '2024-02-05', 'Vacation', 'Approved'),
(2, '2024-02-10', '2024-02-12', 'Sick Leave', 'Pending'),
(3, '2024-02-15', '2024-02-16', 'Personal', 'Approved'),
(4, '2024-02-20', '2024-02-22', 'Vacation', 'Pending'),
(5, '2024-02-25', '2024-02-26', 'Sick Leave', 'Approved'),
(6, '2024-03-01', '2024-03-03', 'Personal', 'Pending'),
(7, '2024-03-05', '2024-03-07', 'Vacation', 'Approved'),
(8, '2024-03-10', '2024-03-12', 'Sick Leave', 'Pending'),
(9, '2024-03-15', '2024-03-16', 'Personal', 'Approved'),
(10, '2024-03-20', '2024-03-22', 'Vacation', 'Pending'),
(11, '2024-03-25', '2024-03-26', 'Sick Leave', 'Approved'),
(12, '2024-04-01', '2024-04-03', 'Personal', 'Pending'),
(13, '2024-04-05', '2024-04-07', 'Vacation', 'Approved'),
(14, '2024-04-10', '2024-04-12', 'Sick Leave', 'Pending'),
(15, '2024-04-15', '2024-04-16', 'Personal', 'Approved'),
(16, '2024-04-20', '2024-04-22', 'Vacation', 'Pending');

INSERT INTO inventory (product_id, warehouse_id, quantity_on_hand, reorder_level, last_updated) VALUES
(1, 1, 50, 10, NOW()),
(2, 1, 25, 5, NOW()),
(3, 2, 100, 20, NOW()),
(4, 2, 75, 15, NOW()),
(5, 1, 200, 50, NOW()),
(6, 2, 150, 10, NOW()),
(7, 1, 100, 20, NOW()),
(8, 2, 75, 15, NOW()),
(9, 1, 200, 50, NOW()),
(10, 2, 150, 10, NOW());

INSERT INTO warehouses (warehouse_name, location, capacity, manager_id) VALUES
('Main Warehouse', '123 Industrial Blvd', 10000, 1),
('Secondary Warehouse', '456 Commerce St', 5000, 2),
('West Warehouse', '789 West St', 15000, 3),
('East Warehouse', '321 East St', 10000, 4),
('North Warehouse', '654 North St', 5000, 5),
('South Warehouse', '987 South St', 10000, 6),
('Central Warehouse', '555 Central St', 15000, 7);

INSERT INTO stock_movements (product_id, warehouse_id, movement_type, quantity, movement_date) VALUES
(1, 1, 'IN', 100, NOW()),
(2, 1, 'OUT', 25, NOW()),
(3, 2, 'IN', 50, NOW()),
(4, 1, 'IN', 100, NOW()),
(5, 2, 'OUT', 25, NOW()),
(6, 1, 'IN', 50, NOW()),
(7, 2, 'OUT', 25, NOW()),
(8, 1, 'IN', 100, NOW()),
(9, 2, 'OUT', 25, NOW());

INSERT INTO suppliers (supplier_name, contact_person, phone, email, address) VALUES
('Tech Supplies Inc', 'John Supplier', '555-1001', 'john@techsupplies.com', '789 Supply St'),
('Kitchen World', 'Jane Kitchen', '555-1002', 'jane@kitchenworld.com', '321 Kitchen Ave'),
('Sports Gear Co', 'Bob Sports', '555-1003', 'bob@sportsgear.com', '654 Sports Rd'),
('Furniture Co', 'Charlie Furniture', '555-1004', 'charlie@furnitureco.com', '987 Furniture Ave'),
('Lighting Inc', 'David Lighting', '555-1005', 'david@lightinginc.com', '555 Lighting St'),
('Kitchen Supplies', 'Eve Kitchen', '555-1006', 'eve@kitchensupplies.com', '321 Kitchen Ave'),
('Paper Co', 'Frank Paper', '555-1007', 'frank@paperco.com', '654 Paper Rd'),
('Audio Tech', 'George Audio', '555-1008', 'george@auditech.com', '987 Audio Ave');

INSERT INTO purchase_orders (supplier_id, order_date, expected_delivery, total_amount, status) VALUES
(1, '2024-01-10', '2024-01-20', 5000, 'Delivered'),
(2, '2024-01-15', '2024-01-25', 3000, 'In Transit'),
(3, '2024-01-20', '2024-01-30', 2000, 'Pending'),
(4, '2024-01-25', '2024-01-31', 4000, 'Delivered'),
(5, '2024-01-30', '2024-02-05', 1000, 'In Transit'),
(6, '2024-02-05', '2024-02-10', 2000, 'Pending'),
(7, '2024-02-10', '2024-02-15', 3000, 'Delivered'),
(8, '2024-02-15', '2024-02-20', 4000, 'In Transit'),
(9, '2024-02-20', '2024-02-25', 1000, 'Pending');

-- Continue with more sample data for remaining tables...
INSERT INTO campaigns (campaign_name, start_date, end_date, budget, target_audience) VALUES
('Summer Sale 2024', '2024-06-01', '2024-08-31', 100000, 'General'),
('Holiday Special', '2024-11-01', '2024-12-31', 150000, 'Premium Customers'),
('Christmas Sale 2024', '2024-12-01', '2024-12-31', 200000, 'General'),
('Valentine''s Day', '2024-02-01', '2024-02-14', 50000, 'Romantic'),
('Spring Cleaning', '2024-03-01', '2024-03-31', 75000, 'General'),
('Back-to-School', '2024-08-01', '2024-08-31', 100000, 'Students'),
('Labor Day Sale', '2024-09-01', '2024-09-07', 80000, 'General'),
('Halloween Sale', '2024-10-01', '2024-10-31', 60000, 'Spooky'),
('Black Friday', '2024-11-22', '2024-11-28', 150000, 'General'),
('Cyber Monday', '2024-11-26', '2024-11-28', 120000, 'General'),
('End of Year Sale', '2024-12-01', '2024-12-31', 200000, 'General'),
('Winter Clearance', '2024-12-01', '2024-12-31', 150000, 'General'),
('Spring Sale', '2024-03-01', '2024-03-31', 75000, 'General'),
('Summer Clearance', '2024-06-01', '2024-06-30', 50000, 'General');

INSERT INTO advertisements (campaign_id, ad_type, platform, cost, impressions) VALUES
(1, 'Banner', 'Google', 5000, 100000),
(2, 'Video', 'Facebook', 8000, 150000),
(3, 'Social Media', 'Instagram', 3000, 50000),
(4, 'Email', 'Mailchimp', 2000, 30000),
(5, 'Display', 'Google', 4000, 80000),
(6, 'Video', 'YouTube', 7000, 120000),
(7, 'Social Media', 'Facebook', 2500, 40000),
(8, 'Email', 'Mailchimp', 1500, 25000);

INSERT INTO customer_segments (segment_name, criteria, customer_count) VALUES
('Premium', 'Loyalty points > 200', 150),
('Regular', 'Loyalty points 50-200', 300),
('New', 'Loyalty points < 50', 100),
('VIP', 'Loyalty points > 500', 50),
('Young Adults', 'Age 18-30', 200),
('Seniors', 'Age 65+', 100);

INSERT INTO promotions (promotion_name, discount_percentage, start_date, end_date, applicable_products) VALUES
('Electronics Sale', 20.00, '2024-01-15', '2024-01-31', 'Electronics'),
('Clothing Clearance', 30.00, '2024-02-01', '2024-02-15', 'Clothing'),
('Home Decor Sale', 25.00, '2024-03-01', '2024-03-31', 'Home Decor'),
('Kitchen Essentials', 15.00, '2024-04-01', '2024-04-30', 'Kitchen'),
('Pet Supplies', 10.00, '2024-05-01', '2024-05-31', 'Pet'),
('Office Supplies', 10.00, '2024-06-01', '2024-06-30', 'Office'),
('Seasonal Sale', 15.00, '2024-07-01', '2024-07-31', 'General'),
('Holiday Sale', 20.00, '2024-11-01', '2024-11-30', 'General'),
('Clearance Sale', 25.00, '2024-12-01', '2024-12-31', 'General');

INSERT INTO loyalty_programs (program_name, points_per_dollar, redemption_rate, active) VALUES
('Gold Tier', 2.00, 0.01, TRUE),
('Silver Tier', 1.50, 0.01, TRUE),
('Bronze Tier', 1.00, 0.01, TRUE),
('Diamond Tier', 3.00, 0.01, TRUE),
('Platinum Tier', 4.00, 0.01, TRUE),
('Emerald Tier', 3.50, 0.01, TRUE),
('Ruby Tier', 3.00, 0.01, TRUE),
('Sapphire Tier', 2.50, 0.01, TRUE);

-- Add more sample data for remaining tables to reach 100+ tables
-- (Continuing with abbreviated sample data for space)

INSERT INTO accounts (account_name, account_type, balance, currency) VALUES
('Operating Account', 'Checking', 500000, 'USD'),
('Savings Account', 'Savings', 1000000, 'USD'),
('Investment Account', 'Investment', 2000000, 'USD'),
('Credit Card Account', 'Credit Card', 1000000, 'USD'),
('Debit Card Account', 'Debit Card', 1000000, 'USD'),
('Travel Account', 'Travel', 1000000, 'USD'),
('Entertainment Account', 'Entertainment', 1000000, 'USD'),
('Food Account', 'Food', 1000000, 'USD'),
('Utility Account', 'Utility', 1000000, 'USD');

INSERT INTO transactions_financial (account_id, transaction_type, amount, description, transaction_date) VALUES
(1, 'Deposit', 10000, 'Customer payment', NOW()),
(2, 'Withdrawal', 5000, 'Equipment purchase', NOW()),
(3, 'Transfer', 25000, 'Investment', NOW()),
(4, 'Deposit', 10000, 'Customer payment', NOW()),
(5, 'Withdrawal', 5000, 'Equipment purchase', NOW()),
(6, 'Transfer', 25000, 'Investment', NOW());

INSERT INTO budgets (department_id, fiscal_year, allocated_amount, spent_amount) VALUES
(1, 2024, 500000, 125000),
(2, 2024, 800000, 200000),
(3, 2024, 600000, 150000),
(4, 2024, 700000, 180000),
(5, 2024, 800000, 210000),
(6, 2024, 900000, 240000),
(7, 2024, 1000000, 270000),
(8, 2024, 1100000, 300000),
(9, 2024, 1200000, 330000),
(10, 2024, 1300000, 360000);

INSERT INTO invoices (customer_id, invoice_date, due_date, total_amount, status) VALUES
(1, '2024-01-15', '2024-02-15', 125.50, 'Paid'),
(2, '2024-01-16', '2024-02-16', 89.99, 'Pending'),
(3, '2024-01-17', '2024-02-17', 234.75, 'Overdue'),
(4, '2024-01-18', '2024-02-18', 67.25, 'Paid'),
(5, '2024-01-19', '2024-02-19', 156.80, 'Paid'),
(6, '2024-01-20', '2024-02-20', 125.50, 'Paid'),
(7, '2024-01-21', '2024-02-21', 89.99, 'Pending'),
(8, '2024-01-22', '2024-02-22', 234.75, 'Overdue'),
(9, '2024-01-23', '2024-02-23', 67.25, 'Paid'),
(10, '2024-01-24', '2024-02-24', 156.80, 'Paid');


INSERT INTO payments (invoice_id, payment_date, amount, payment_method) VALUES
(1, '2024-01-20', 125.50, 'Credit Card'),
(2, '2024-01-25', 89.99, 'Bank Transfer'),
(3, '2024-01-26', 234.75, 'Credit Card'),
(4, '2024-01-27', 67.25, 'Bank Transfer'),
(5, '2024-01-28', 156.80, 'Credit Card'),
(6, '2024-01-29', 125.50, 'Bank Transfer'),
(7, '2024-01-30', 89.99, 'Credit Card'),
(8, '2024-01-31', 234.75, 'Bank Transfer'),
(9, '2024-02-01', 67.25, 'Credit Card'),
(10, '2024-02-02', 156.80, 'Bank Transfer');

-- Continue with sample data for all remaining tables...
-- (For brevity, I'll add a few more key tables with sample data)

INSERT INTO servers (server_name, ip_address, operating_system, cpu_cores, memory_gb) VALUES
('Web Server 1', '192.168.1.10', 'Ubuntu 20.04', 8, 32),
('Database Server', '192.168.1.20', 'CentOS 8', 16, 64),
('App Server 1', '192.168.1.30', 'Windows Server 2019', 4, 16),
('App Server 2', '192.168.1.40', 'Windows Server 2022', 4, 16),
('App Server 3', '192.168.1.50', 'Windows Server 2022', 4, 16),
('App Server 4', '192.168.1.60', 'Windows Server 2022', 4, 16),
('App Server 5', '192.168.1.70', 'Windows Server 2022', 4, 16),
('App Server 6', '192.168.1.80', 'Windows Server 2022', 4, 16),
('App Server 7', '192.168.1.90', 'Windows Server 2022', 4, 16),
('App Server 8', '192.168.1.100', 'Windows Server 2022', 4, 16),
('Database Server 2', '192.168.1.200', 'Fedora 41', 16, 64),
('Database Server 3', '192.168.1.300', 'CentOS 8', 16, 64),
('Database Server 4', '192.168.1.400', 'Fedora 41', 16, 64),
('Database Server 5', '192.168.1.500', 'Fedora 41', 16, 64),
('Database Server 6', '192.168.1.600', 'Fedora 41', 16, 64),
('Database Server 7', '192.168.1.700', 'CentOS 8', 16, 64),
('Database Server 8', '192.168.1.800', 'CentOS 8', 16, 64);

INSERT INTO applications (app_name, version, server_id, status, last_deployed) VALUES
('Customer Portal', '2.1.0', 1, 'Active', NOW()),
('Inventory System', '1.5.2', 2, 'Active', NOW()),
('Reporting Tool', '3.0.1', 3, 'Active', NOW()),
('Payment Gateway', '1.0.0', 4, 'Active', NOW()),
('Email Service', '2.0.0', 5, 'Active', NOW()),
('File Storage', '1.0.0', 6, 'Active', NOW()),
('API Gateway', '1.0.0', 7, 'Active', NOW()),
('CDN Service', '1.0.0', 8, 'Active', NOW()),
('DNS Service', '1.0.0', 9, 'Active', NOW()),
('VPN Service', '1.0.0', 10, 'Active', NOW());

INSERT INTO user_accounts (username, email, role, last_login, active) VALUES
('admin', 'admin@company.com', 'Administrator', NOW(), TRUE),
('user1', 'user1@company.com', 'User', NOW(), TRUE),
('user2', 'user2@company.com', 'User', NOW(), FALSE),
('user3', 'user3@company.com', 'User', NOW(), TRUE),
('user4', 'user4@company.com', 'User', NOW(), FALSE),
('user5', 'user5@company.com', 'User', NOW(), TRUE),
('user6', 'user6@company.com', 'User', NOW(), FALSE),
('user7', 'user7@company.com', 'User', NOW(), TRUE),
('user8', 'user8@company.com', 'User', NOW(), FALSE),
('user9', 'user9@company.com', 'User', NOW(), TRUE),
('user10', 'user10@company.com', 'User', NOW(), FALSE);

INSERT INTO security_logs (user_id, action, ip_address, timestamp, success) VALUES
(1, 'Login', '192.168.1.100', NOW(), TRUE),
(2, 'Login', '192.168.1.101', NOW(), TRUE),
(3, 'Failed Login', '192.168.1.102', NOW(), FALSE),
(4, 'Login', '192.168.1.103', NOW(), TRUE),
(5, 'Failed Login', '192.168.1.104', NOW(), FALSE),
(6, 'Login', '192.168.1.105', NOW(), TRUE),
(7, 'Failed Login', '192.168.1.106', NOW(), FALSE),
(8, 'Login', '192.168.1.107', NOW(), TRUE),
(9, 'Failed Login', '192.168.1.108', NOW(), FALSE),
(10, 'Login', '192.168.1.109', NOW(), TRUE);

INSERT INTO backups (server_id, backup_type, size_gb, backup_date, status) VALUES
(1, 'Full', 25.5, NOW(), 'Completed'),
(2, 'Incremental', 5.2, NOW(), 'Completed'),
(3, 'Full', 15.8, NOW(), 'In Progress'),
(4, 'Incremental', 10.5, NOW(), 'Completed'),
(5, 'Full', 20.2, NOW(), 'In Progress'),
(6, 'Incremental', 15.3, NOW(), 'Completed'),
(7, 'Full', 25.7, NOW(), 'Completed'),
(8, 'Incremental', 20.4, NOW(), 'Completed'),
(9, 'Full', 30.1, NOW(), 'Completed'),
(10, 'Incremental', 25.6, NOW(), 'Completed');

-- Add sample data for remaining tables to complete the stress test
-- (This would continue for all 100+ tables, but for brevity, I'll add a few more key ones)

INSERT INTO job_positions (title, department_id, salary_range_min, salary_range_max, requirements) VALUES
('Software Engineer', 2, 70000, 120000, 'Bachelor degree in Computer Science'),
('HR Manager', 1, 60000, 90000, 'Master degree in HR or related field'),
('Financial Analyst', 3, 55000, 85000, 'Bachelor degree in Finance'),
('Marketing Manager', 4, 65000, 100000, 'Master degree in Marketing'),
('Sales Manager', 5, 75000, 120000, 'Bachelor degree in Business Administration'),
('Product Manager', 6, 85000, 130000, 'Master degree in Business Administration'),
('Customer Service Manager', 7, 95000, 140000, 'Bachelor degree in Business Administration'),
('IT Manager', 8, 105000, 150000, 'Master degree in Information Technology'),
('HR Manager', 9, 115000, 160000, 'Master degree in Human Resources'),
('Research Scientist', 10, 125000, 170000, 'PhD in Computer Science');

INSERT INTO interviews (candidate_name, position_id, interview_date, interviewer_id, rating, notes) VALUES
('John Candidate', 1, '2024-01-20', 2, 8, 'Strong technical skills'),
('Jane Candidate', 2, '2024-01-22', 1, 7, 'Good communication skills'),
('Bob Candidate', 3, '2024-01-25', 3, 9, 'Excellent analytical skills'),
('Charlie Candidate', 4, '2024-01-28', 4, 8, 'Strong technical skills'),
('David Candidate', 5, '2024-01-30', 5, 7, 'Good communication skills'),
('Eve Candidate', 6, '2024-02-02', 6, 9, 'Excellent analytical skills'),
('Frank Candidate', 7, '2024-02-05', 7, 8, 'Strong technical skills'),
('George Candidate', 8, '2024-02-08', 8, 7, 'Good communication skills'),
('Hannah Candidate', 9, '2024-02-11', 9, 9, 'Excellent analytical skills');

INSERT INTO training_programs (program_name, duration_hours, cost, instructor, description) VALUES
('Leadership Development', 40, 2000, 'Dr. Smith', 'Comprehensive leadership training'),
('Technical Skills', 24, 1500, 'Prof. Johnson', 'Advanced technical training'),
('Communication Skills', 16, 1000, 'Ms. Davis', 'Effective communication workshop'),
('Project Management', 32, 3000, 'Mr. Johnson', 'Advanced project management training'),
('Data Analysis', 24, 2000, 'Dr. Smith', 'Data analysis workshop'),
('Leadership Training', 40, 2500, 'Prof. Davis', 'Leadership development program'),
('Sales Training', 24, 1500, 'Ms. Johnson', 'Sales training workshop'),
('Customer Service Training', 16, 1000, 'Mr. Smith', 'Customer service training workshop'),
('IT Training', 32, 3000, 'Prof. Johnson', 'IT training workshop'),
('HR Training', 24, 2000, 'Dr. Davis', 'HR training workshop');

INSERT INTO performance_reviews (employee_id, review_period_start, review_period_end, overall_rating, goals_met, goals_total) VALUES
(1, '2023-01-01', '2023-12-31', 8, 4, 5),
(2, '2023-01-01', '2023-12-31', 9, 5, 5),
(3, '2023-01-01', '2023-12-31', 7, 3, 4),
(4, '2023-01-01', '2023-12-31', 8, 4, 5),
(5, '2023-01-01', '2023-12-31', 9, 5, 5),
(6, '2023-01-01', '2023-12-31', 7, 3, 4),
(7, '2023-01-01', '2023-12-31', 8, 4, 5),
(8, '2023-01-01', '2023-12-31', 9, 5, 5),
(9, '2023-01-01', '2023-12-31', 7, 3, 4),
(10, '2023-01-01', '2023-12-31', 8, 4, 5);

INSERT INTO certifications (employee_id, certification_name, issuing_organization, issue_date, expiry_date) VALUES
(1, 'PHR Certification', 'HRCI', '2023-06-15', '2026-06-15'),
(2, 'AWS Solutions Architect', 'Amazon', '2023-08-20', '2026-08-20'),
(3, 'CPA License', 'AICPA', '2023-09-10', '2025-09-10'),
(4, 'CFA Level I', 'CFA Institute', '2023-10-15', '2026-10-15'),
(5, 'PMP Certification', 'PMI', '2023-11-20', '2026-11-20'),
(6, 'ITIL Foundation', 'AXELOS', '2023-12-01', '2026-12-01'),
(7, 'SHRM-CP', 'SHRM', '2024-01-01', '2027-01-01'),
(8, 'CISA Certification', 'ISC2', '2024-02-01', '2027-02-01'),
(9, 'CISSP Certification', 'ISC2', '2024-03-01', '2027-03-01'),
(10, 'CISM Certification', 'ISC2', '2024-04-01', '2027-04-01');

-- Continue with more tables and sample data...
-- (For the full stress test, this would include all 100+ tables with 5-10 rows each)

-- Add sample data for remaining tables
INSERT INTO maintenance_schedules (equipment_id, maintenance_type, scheduled_date, technician_id, estimated_duration) VALUES
(1, 'Preventive', '2024-02-15', 1, 120),
(2, 'Corrective', '2024-02-20', 2, 180),
(3, 'Preventive', '2024-02-25', 1, 90),
(4, 'Emergency', '2024-03-01', 3, 240),
(5, 'Preventive', '2024-03-05', 2, 150),
(6, 'Corrective', '2024-03-10', 1, 200),
(7, 'Preventive', '2024-03-15', 3, 100),
(8, 'Emergency', '2024-03-20', 2, 300);

INSERT INTO equipment (equipment_name, equipment_type, location, purchase_date, warranty_expiry) VALUES
('Conveyor Belt A', 'Manufacturing', 'Production Floor 1', '2022-01-15', '2025-01-15'),
('Packaging Machine B', 'Packaging', 'Production Floor 2', '2022-03-20', '2025-03-20'),
('Quality Scanner C', 'Quality Control', 'QC Station 1', '2022-05-10', '2025-05-10'),
('Forklift D', 'Material Handling', 'Warehouse A', '2022-07-15', '2025-07-15'),
('Label Printer E', 'Labeling', 'Packaging Station 1', '2022-09-20', '2025-09-20'),
('Temperature Monitor F', 'Environmental', 'Cold Storage', '2022-11-10', '2025-11-10'),
('Security Camera G', 'Security', 'Main Entrance', '2023-01-15', '2026-01-15'),
('Network Switch H', 'IT Infrastructure', 'Server Room', '2023-03-20', '2026-03-20');

INSERT INTO work_orders (equipment_id, priority, description, assigned_to, status) VALUES
(1, 'High', 'Replace worn belt components', 1, 'In Progress'),
(2, 'Medium', 'Calibrate packaging sensors', 2, 'Pending'),
(3, 'Low', 'Update scanner software', 3, 'Completed'),
(4, 'High', 'Replace hydraulic fluid', 1, 'In Progress'),
(5, 'Medium', 'Clean print heads', 2, 'Pending'),
(6, 'Low', 'Replace temperature sensor', 3, 'Completed'),
(7, 'High', 'Reposition camera angle', 1, 'In Progress'),
(8, 'Medium', 'Update network firmware', 2, 'Pending');

INSERT INTO quality_checks (product_id, check_type, result, inspector_id, check_date) VALUES
(1, 'Visual Inspection', 'Pass', 1, NOW()),
(2, 'Dimensional Check', 'Pass', 2, NOW()),
(3, 'Functionality Test', 'Fail', 1, NOW()),
(4, 'Visual Inspection', 'Pass', 3, NOW()),
(5, 'Dimensional Check', 'Pass', 2, NOW()),
(6, 'Functionality Test', 'Pass', 1, NOW()),
(7, 'Visual Inspection', 'Fail', 3, NOW()),
(8, 'Dimensional Check', 'Pass', 2, NOW());

INSERT INTO safety_incidents (employee_id, incident_date, incident_type, severity, description) VALUES
(1, '2024-01-15', 'Slip and Fall', 'Minor', 'Employee slipped on wet floor'),
(2, '2024-01-20', 'Equipment Malfunction', 'Moderate', 'Machine stopped unexpectedly'),
(3, '2024-01-25', 'Chemical Exposure', 'Minor', 'Small chemical spill contained'),
(4, '2024-02-01', 'Lifting Injury', 'Moderate', 'Back strain from improper lifting'),
(5, '2024-02-05', 'Cut', 'Minor', 'Minor cut from sharp edge'),
(6, '2024-02-10', 'Electrical Shock', 'Major', 'Minor electrical shock from equipment'),
(7, '2024-02-15', 'Slip and Fall', 'Minor', 'Employee slipped on wet floor'),
(8, '2024-02-20', 'Equipment Malfunction', 'Moderate', 'Machine stopped unexpectedly');

INSERT INTO support_tickets (customer_id, issue_type, priority, status, created_date) VALUES
(1, 'Technical Support', 'High', 'Open', NOW()),
(2, 'Billing Question', 'Medium', 'In Progress', NOW()),
(3, 'Product Inquiry', 'Low', 'Resolved', NOW()),
(4, 'Technical Support', 'High', 'Open', NOW()),
(5, 'Billing Question', 'Medium', 'In Progress', NOW()),
(6, 'Product Inquiry', 'Low', 'Resolved', NOW()),
(7, 'Technical Support', 'High', 'Open', NOW()),
(8, 'Billing Question', 'Medium', 'In Progress', NOW());

INSERT INTO customer_feedback (customer_id, rating, comments, feedback_date, category) VALUES
(1, 5, 'Excellent service and fast delivery', NOW(), 'Service'),
(2, 4, 'Good product quality, could improve packaging', NOW(), 'Product'),
(3, 3, 'Average experience, room for improvement', NOW(), 'Service'),
(4, 5, 'Outstanding customer support', NOW(), 'Support'),
(5, 4, 'Good value for money', NOW(), 'Product'),
(6, 2, 'Product arrived damaged', NOW(), 'Product'),
(7, 5, 'Quick response to my inquiry', NOW(), 'Support'),
(8, 4, 'Satisfied with purchase', NOW(), 'Product');

INSERT INTO chat_sessions (customer_id, agent_id, start_time, end_time, resolution_status) VALUES
(1, 1, NOW() - INTERVAL '30 minutes', NOW(), 'Resolved'),
(2, 2, NOW() - INTERVAL '45 minutes', NOW(), 'Resolved'),
(3, 1, NOW() - INTERVAL '20 minutes', NOW(), 'Escalated'),
(4, 3, NOW() - INTERVAL '60 minutes', NOW(), 'Resolved'),
(5, 2, NOW() - INTERVAL '25 minutes', NOW(), 'Resolved'),
(6, 1, NOW() - INTERVAL '35 minutes', NOW(), 'Escalated'),
(7, 3, NOW() - INTERVAL '40 minutes', NOW(), 'Resolved'),
(8, 2, NOW() - INTERVAL '15 minutes', NOW(), 'Resolved');

INSERT INTO knowledge_base (title, content, category, last_updated, views) VALUES
('How to Reset Password', 'Step-by-step guide to reset your account password', 'Account Management', NOW(), 150),
('Product Return Policy', 'Complete information about our return and refund policy', 'Policies', NOW(), 200),
('Shipping Information', 'Details about shipping options and delivery times', 'Shipping', NOW(), 300),
('Payment Methods', 'Accepted payment methods and security information', 'Payment', NOW(), 180),
('Account Security', 'Tips for keeping your account secure', 'Security', NOW(), 120),
('Product Warranty', 'Information about product warranties and coverage', 'Warranty', NOW(), 90),
('Technical Specifications', 'Detailed technical specs for our products', 'Technical', NOW(), 250),
('Contact Information', 'How to reach our customer support team', 'Contact', NOW(), 400);

INSERT INTO faq (question, answer, category, helpful_votes) VALUES
('What is your return policy?', 'We offer 30-day returns for most items in original condition', 'Returns', 45),
('How long does shipping take?', 'Standard shipping takes 3-5 business days', 'Shipping', 60),
('Do you offer international shipping?', 'Yes, we ship to over 50 countries worldwide', 'Shipping', 35),
('What payment methods do you accept?', 'We accept all major credit cards, PayPal, and bank transfers', 'Payment', 50),
('How can I track my order?', 'You will receive a tracking number via email once your order ships', 'Orders', 40),
('Do you have a mobile app?', 'Yes, our mobile app is available for iOS and Android', 'Mobile', 25),
('What is your customer service hours?', 'We are available 24/7 via chat and email', 'Support', 55),
('Do you offer bulk discounts?', 'Yes, we offer volume discounts for orders over $1000', 'Pricing', 30);

INSERT INTO website_analytics (page_url, visitors, page_views, bounce_rate, date) VALUES
('/home', 1500, 3000, 35.5, '2024-01-15'),
('/products', 800, 1200, 45.2, '2024-01-15'),
('/about', 300, 400, 25.0, '2024-01-15'),
('/contact', 200, 250, 30.0, '2024-01-15'),
('/cart', 150, 200, 20.0, '2024-01-15'),
('/checkout', 100, 120, 15.0, '2024-01-15'),
('/blog', 400, 600, 40.0, '2024-01-15'),
('/support', 250, 350, 35.0, '2024-01-15');

INSERT INTO social_media_posts (platform, content, post_date, likes, shares) VALUES
('Facebook', 'Check out our new product line!', NOW() - INTERVAL '2 hours', 150, 25),
('Instagram', 'Behind the scenes at our warehouse', NOW() - INTERVAL '4 hours', 200, 30),
('Twitter', 'Customer success story of the week', NOW() - INTERVAL '6 hours', 100, 15),
('LinkedIn', 'Industry insights and trends', NOW() - INTERVAL '8 hours', 80, 20),
('Facebook', 'Holiday sale announcement', NOW() - INTERVAL '1 day', 300, 50),
('Instagram', 'Product demonstration video', NOW() - INTERVAL '1 day', 250, 40),
('Twitter', 'Customer feedback highlights', NOW() - INTERVAL '2 days', 120, 18),
('LinkedIn', 'Company culture spotlight', NOW() - INTERVAL '2 days', 90, 22);

INSERT INTO email_campaigns (subject_line, recipient_count, open_rate, click_rate, sent_date) VALUES
('Welcome to our newsletter!', 5000, 25.5, 5.2, NOW() - INTERVAL '1 day'),
('Special offer for you', 3000, 30.2, 8.1, NOW() - INTERVAL '2 days'),
('Product update announcement', 2000, 22.8, 4.5, NOW() - INTERVAL '3 days'),
('Customer success stories', 1500, 28.5, 6.8, NOW() - INTERVAL '4 days'),
('Holiday sale preview', 4000, 35.2, 10.5, NOW() - INTERVAL '5 days'),
('Industry insights', 1000, 20.1, 3.2, NOW() - INTERVAL '6 days'),
('New product launch', 2500, 32.8, 7.9, NOW() - INTERVAL '7 days'),
('Customer feedback request', 1800, 26.5, 5.8, NOW() - INTERVAL '8 days');

INSERT INTO conversion_funnels (funnel_name, step_name, step_order, conversion_rate) VALUES
('E-commerce Purchase', 'Homepage Visit', 1, 100.0),
('E-commerce Purchase', 'Product View', 2, 45.2),
('E-commerce Purchase', 'Add to Cart', 3, 15.8),
('E-commerce Purchase', 'Checkout Start', 4, 8.5),
('E-commerce Purchase', 'Purchase Complete', 5, 6.2),
('Newsletter Signup', 'Landing Page', 1, 100.0),
('Newsletter Signup', 'Email Entered', 2, 35.5),
('Newsletter Signup', 'Confirmation Clicked', 3, 28.2),
('Newsletter Signup', 'Signup Complete', 4, 25.8);

INSERT INTO a_b_tests (test_name, variant_a_visitors, variant_b_visitors, variant_a_conversions, variant_b_conversions) VALUES
('Homepage Hero Image', 1000, 1000, 45, 52),
('Checkout Button Color', 800, 800, 32, 38),
('Product Page Layout', 1200, 1200, 68, 75),
('Email Subject Line', 2000, 2000, 120, 135),
('Pricing Page Design', 600, 600, 25, 30),
('Mobile Navigation', 1500, 1500, 85, 92),
('Call-to-Action Text', 900, 900, 42, 48),
('Landing Page Headline', 1100, 1100, 55, 62);

INSERT INTO product_features (product_id, feature_name, description, development_status, estimated_effort) VALUES
(1, 'Wireless Connectivity', 'Bluetooth 5.0 support', 'Completed', 40),
(2, 'Auto Brew Timer', 'Programmable brewing schedule', 'In Progress', 60),
(3, 'Cushion Technology', 'Advanced foot support system', 'Completed', 80),
(4, 'Height Adjustment', 'Multiple height settings', 'Completed', 30),
(5, 'Insulation Technology', '24-hour temperature retention', 'Completed', 50),
(6, 'Noise Cancellation', 'Active noise cancellation', 'In Progress', 100),
(7, 'Smart Controls', 'Mobile app integration', 'Planned', 120),
(8, 'Battery Life', 'Extended battery performance', 'In Progress', 70);

INSERT INTO user_stories (product_id, story_title, description, priority, story_points) VALUES
(1, 'Wireless Pairing', 'As a user, I want to easily pair my headphones with my device', 'High', 5),
(2, 'Morning Brew', 'As a user, I want my coffee ready when I wake up', 'High', 8),
(3, 'Comfortable Run', 'As a user, I want comfortable shoes for long runs', 'Medium', 5),
(4, 'Ergonomic Setup', 'As a user, I want to adjust my laptop height for comfort', 'Medium', 3),
(5, 'Cold Water', 'As a user, I want my water to stay cold all day', 'High', 5),
(6, 'Quiet Environment', 'As a user, I want to block out background noise', 'High', 8),
(7, 'Smart Features', 'As a user, I want to control my device with my phone', 'Medium', 13),
(8, 'Long Battery', 'As a user, I want my device to last all day', 'High', 8);

INSERT INTO bug_reports (product_id, bug_title, description, severity, status, reported_by) VALUES
(1, 'Audio Dropout', 'Sound cuts out intermittently during use', 'High', 'Open', 1),
(2, 'Timer Not Working', 'Auto-brew timer fails to activate', 'Medium', 'In Progress', 2),
(3, 'Sole Separation', 'Shoe sole separates from upper after 3 months', 'High', 'Open', 3),
(4, 'Height Lock Failure', 'Height adjustment mechanism slips', 'Medium', 'Fixed', 4),
(5, 'Leak Issue', 'Water bottle leaks from cap area', 'High', 'Open', 5),
(6, 'Battery Drain', 'Battery drains faster than advertised', 'Medium', 'In Progress', 6),
(7, 'App Connection', 'Mobile app fails to connect to device', 'High', 'Open', 7),
(8, 'Charging Problem', 'Device does not charge properly', 'High', 'Fixed', 8);

INSERT INTO releases (product_id, version_number, release_date, release_notes, status) VALUES
(1, '2.1.0', '2024-01-15', 'Fixed audio dropout issues, improved battery life', 'Released'),
(2, '1.5.2', '2024-01-20', 'Fixed timer functionality, added new brew modes', 'Released'),
(3, '1.0.1', '2024-01-25', 'Improved sole durability, enhanced comfort', 'Released'),
(4, '2.0.0', '2024-02-01', 'Redesigned height mechanism, added memory settings', 'Released'),
(5, '1.2.0', '2024-02-05', 'Enhanced insulation, new color options', 'Released'),
(6, '3.0.1', '2024-02-10', 'Improved noise cancellation, better battery management', 'Released'),
(7, '1.0.0', '2024-02-15', 'Initial release with smart features', 'Released'),
(8, '2.1.0', '2024-02-20', 'Extended battery life, improved charging', 'Released');

INSERT INTO user_feedback (product_id, user_id, feedback_type, rating, comments) VALUES
(1, 1, 'Product Review', 4, 'Great sound quality, minor connectivity issues'),
(2, 2, 'Product Review', 5, 'Perfect morning coffee every time'),
(3, 3, 'Product Review', 3, 'Comfortable but durability concerns'),
(4, 4, 'Product Review', 4, 'Good ergonomics, easy to adjust'),
(5, 5, 'Product Review', 5, 'Keeps water cold all day long'),
(6, 6, 'Product Review', 4, 'Excellent noise cancellation'),
(7, 7, 'Product Review', 3, 'Smart features need improvement'),
(8, 8, 'Product Review', 4, 'Good battery life, reliable performance');

INSERT INTO shipping_methods (method_name, delivery_time_days, cost_per_kg, tracking_available) VALUES
('Standard Ground', 5, 5.99, TRUE),
('Express 2-Day', 2, 12.99, TRUE),
('Overnight', 1, 24.99, TRUE),
('International Standard', 10, 15.99, TRUE),
('International Express', 5, 29.99, TRUE),
('Free Shipping', 7, 0.00, TRUE),
('Same Day Delivery', 0, 39.99, TRUE),
('Economy', 10, 3.99, FALSE);

INSERT INTO delivery_routes (route_name, start_location, end_location, distance_km, estimated_time_hours) VALUES
('Route A - Downtown', 'Main Warehouse', 'Downtown District', 15.5, 2),
('Route B - Suburbs', 'Main Warehouse', 'Suburban Area', 25.8, 3),
('Route C - Airport', 'Main Warehouse', 'Airport District', 18.2, 2.5),
('Route D - Industrial', 'Main Warehouse', 'Industrial Zone', 22.1, 3),
('Route E - Residential', 'Main Warehouse', 'Residential Area', 30.5, 4),
('Route F - Business', 'Main Warehouse', 'Business District', 12.3, 1.5),
('Route G - University', 'Main Warehouse', 'University Area', 20.7, 2.5),
('Route H - Mall', 'Main Warehouse', 'Shopping Mall', 16.9, 2);

INSERT INTO customs_documents (shipment_id, document_type, document_number, issue_date, expiry_date) VALUES
(1, 'Commercial Invoice', 'CI-2024-001', '2024-01-15', '2024-04-15'),
(2, 'Packing List', 'PL-2024-002', '2024-01-16', '2024-04-16'),
(3, 'Certificate of Origin', 'CO-2024-003', '2024-01-17', '2024-04-17'),
(4, 'Bill of Lading', 'BL-2024-004', '2024-01-18', '2024-04-18'),
(5, 'Insurance Certificate', 'IC-2024-005', '2024-01-19', '2024-04-19'),
(6, 'Export License', 'EL-2024-006', '2024-01-20', '2024-04-20'),
(7, 'Import Permit', 'IP-2024-007', '2024-01-21', '2024-04-21'),
(8, 'Customs Declaration', 'CD-2024-008', '2024-01-22', '2024-04-22');

INSERT INTO compliance_checks (product_id, regulation_type, compliance_status, check_date, inspector) VALUES
(1, 'FCC Certification', 'Compliant', '2024-01-15', 'John Inspector'),
(2, 'UL Safety Standard', 'Compliant', '2024-01-16', 'Jane Inspector'),
(3, 'CE Marking', 'Compliant', '2024-01-17', 'Bob Inspector'),
(4, 'RoHS Compliance', 'Compliant', '2024-01-18', 'Alice Inspector'),
(5, 'FDA Approval', 'Compliant', '2024-01-19', 'Charlie Inspector'),
(6, 'ISO 9001', 'Compliant', '2024-01-20', 'David Inspector'),
(7, 'Energy Star', 'Compliant', '2024-01-21', 'Eve Inspector'),
(8, 'WEEE Directive', 'Compliant', '2024-01-22', 'Frank Inspector');

INSERT INTO sustainability_metrics (product_id, carbon_footprint, water_usage, recyclable_percentage, measurement_date) VALUES
(1, 2.5, 0.8, 85.0, '2024-01-15'),
(2, 1.8, 1.2, 90.0, '2024-01-16'),
(3, 3.2, 0.5, 75.0, '2024-01-17'),
(4, 1.5, 0.3, 95.0, '2024-01-18'),
(5, 0.8, 0.2, 100.0, '2024-01-19'),
(6, 2.8, 0.6, 80.0, '2024-01-20'),
(7, 3.5, 1.0, 70.0, '2024-01-21'),
(8, 2.1, 0.4, 88.0, '2024-01-22');

INSERT INTO contracts (contract_name, party_a, party_b, start_date, end_date, value) VALUES
('Supply Agreement - Electronics', 'TechCorp Inc', 'Electronics Supplier Ltd', '2024-01-01', '2024-12-31', 500000),
('Service Contract - IT Support', 'TechCorp Inc', 'IT Solutions Corp', '2024-01-15', '2024-12-31', 250000),
('Distribution Agreement', 'TechCorp Inc', 'Global Distributors', '2024-02-01', '2025-01-31', 750000),
('Maintenance Contract', 'TechCorp Inc', 'Equipment Services LLC', '2024-02-15', '2024-12-31', 150000),
('Marketing Partnership', 'TechCorp Inc', 'Digital Marketing Agency', '2024-03-01', '2024-12-31', 200000),
('Software License', 'TechCorp Inc', 'Software Solutions Inc', '2024-03-15', '2025-03-14', 100000),
('Consulting Agreement', 'TechCorp Inc', 'Business Consultants Ltd', '2024-04-01', '2024-12-31', 300000),
('Insurance Policy', 'TechCorp Inc', 'Insurance Company', '2024-04-15', '2025-04-14', 50000);

INSERT INTO legal_documents (document_type, title, content, effective_date, expiry_date) VALUES
('Terms of Service', 'User Agreement', 'Terms and conditions for using our services', '2024-01-01', '2025-01-01'),
('Privacy Policy', 'Data Protection Policy', 'How we collect and protect user data', '2024-01-01', '2025-01-01'),
('Employment Contract', 'Standard Employment Terms', 'Standard terms for employee contracts', '2024-01-01', '2025-01-01'),
('Non-Disclosure Agreement', 'Confidentiality Agreement', 'Standard NDA for business partners', '2024-01-01', '2025-01-01'),
('Service Level Agreement', 'SLA for IT Services', 'Service level commitments for IT support', '2024-01-01', '2025-01-01'),
('Vendor Agreement', 'Supplier Terms', 'Standard terms for vendor relationships', '2024-01-01', '2025-01-01'),
('Intellectual Property Policy', 'IP Protection Guidelines', 'Guidelines for protecting intellectual property', '2024-01-01', '2025-01-01'),
('Compliance Manual', 'Regulatory Compliance', 'Manual for regulatory compliance procedures', '2024-01-01', '2025-01-01');

INSERT INTO compliance_audits (audit_type, audit_date, auditor, findings, recommendations) VALUES
('Financial Audit', '2024-01-15', 'Audit Firm A', 'Minor discrepancies in expense reporting', 'Implement automated expense tracking'),
('Security Audit', '2024-01-20', 'Security Consultants', 'Strong security measures in place', 'Continue regular security updates'),
('Environmental Audit', '2024-01-25', 'Environmental Assessors', 'Good environmental practices', 'Consider renewable energy options'),
('Quality Audit', '2024-02-01', 'Quality Assurance Team', 'High quality standards maintained', 'Document quality procedures better'),
('Safety Audit', '2024-02-05', 'Safety Inspectors', 'Good safety record', 'Enhance safety training programs'),
('Data Privacy Audit', '2024-02-10', 'Privacy Consultants', 'Compliant with data protection laws', 'Update privacy notices'),
('IT Audit', '2024-02-15', 'IT Auditors', 'Robust IT infrastructure', 'Improve backup procedures'),
('HR Audit', '2024-02-20', 'HR Consultants', 'Good HR practices', 'Enhance employee development programs');

INSERT INTO intellectual_property (ip_type, title, registration_number, registration_date, owner) VALUES
('Trademark', 'TechCorp Logo', 'TM-2024-001', '2024-01-15', 'TechCorp Inc'),
('Patent', 'Wireless Charging Technology', 'US-2024-002', '2024-01-20', 'TechCorp Inc'),
('Copyright', 'Software Application', 'CR-2024-003', '2024-01-25', 'TechCorp Inc'),
('Trademark', 'Product Brand Name', 'TM-2024-004', '2024-02-01', 'TechCorp Inc'),
('Patent', 'Manufacturing Process', 'US-2024-005', '2024-02-05', 'TechCorp Inc'),
('Copyright', 'User Manual', 'CR-2024-006', '2024-02-10', 'TechCorp Inc'),
('Trademark', 'Service Mark', 'TM-2024-007', '2024-02-15', 'TechCorp Inc'),
('Patent', 'Design Innovation', 'US-2024-008', '2024-02-20', 'TechCorp Inc');

INSERT INTO data_privacy_requests (customer_id, request_type, request_date, status, response_date) VALUES
(1, 'Data Access', '2024-01-15', 'Completed', '2024-01-20'),
(2, 'Data Deletion', '2024-01-16', 'In Progress', NULL),
(3, 'Data Portability', '2024-01-17', 'Completed', '2024-01-22'),
(4, 'Data Correction', '2024-01-18', 'Completed', '2024-01-23'),
(5, 'Data Access', '2024-01-19', 'In Progress', NULL),
(6, 'Data Deletion', '2024-01-20', 'Completed', '2024-01-25'),
(7, 'Data Portability', '2024-01-21', 'In Progress', NULL),
(8, 'Data Correction', '2024-01-22', 'Completed', '2024-01-27');

INSERT INTO research_projects (project_name, start_date, end_date, budget, lead_researcher) VALUES
('AI-Powered Analytics', '2024-01-01', '2024-12-31', 500000, 'Dr. Sarah Johnson'),
('Sustainable Materials', '2024-02-01', '2024-11-30', 300000, 'Dr. Michael Chen'),
('User Experience Optimization', '2024-03-01', '2024-10-31', 200000, 'Dr. Emily Rodriguez'),
('Energy Efficiency', '2024-04-01', '2024-09-30', 400000, 'Dr. David Kim'),
('Machine Learning Applications', '2024-05-01', '2024-08-31', 350000, 'Dr. Lisa Wang'),
('Product Innovation', '2024-06-01', '2024-07-31', 150000, 'Dr. Robert Taylor'),
('Market Research', '2024-07-01', '2024-12-31', 250000, 'Dr. Jennifer Brown'),
('Technology Integration', '2024-08-01', '2024-11-30', 180000, 'Dr. Christopher Lee');

INSERT INTO patents (patent_number, title, inventor, filing_date, grant_date) VALUES
('US-2024-001', 'Advanced Wireless Charging System', 'Dr. Sarah Johnson', '2024-01-15', '2024-06-15'),
('US-2024-002', 'Eco-Friendly Manufacturing Process', 'Dr. Michael Chen', '2024-01-20', '2024-06-20'),
('US-2024-003', 'User Interface Optimization Method', 'Dr. Emily Rodriguez', '2024-01-25', '2024-06-25'),
('US-2024-004', 'Energy-Efficient Device Design', 'Dr. David Kim', '2024-02-01', '2024-07-01'),
('US-2024-005', 'Machine Learning Algorithm', 'Dr. Lisa Wang', '2024-02-05', '2024-07-05'),
('US-2024-006', 'Innovative Product Feature', 'Dr. Robert Taylor', '2024-02-10', '2024-07-10'),
('US-2024-007', 'Market Analysis System', 'Dr. Jennifer Brown', '2024-02-15', '2024-07-15'),
('US-2024-008', 'Technology Integration Framework', 'Dr. Christopher Lee', '2024-02-20', '2024-07-20');

INSERT INTO laboratory_tests (project_id, test_name, test_date, results, success) VALUES
(1, 'AI Model Accuracy Test', '2024-01-15', '95% accuracy achieved', TRUE),
(2, 'Material Durability Test', '2024-01-20', 'Exceeded durability requirements', TRUE),
(3, 'User Interface Usability Test', '2024-01-25', 'Improved user satisfaction by 30%', TRUE),
(4, 'Energy Consumption Test', '2024-02-01', 'Reduced energy usage by 25%', TRUE),
(5, 'Machine Learning Performance Test', '2024-02-05', 'Processing speed improved by 40%', TRUE),
(6, 'Product Feature Validation', '2024-02-10', 'Feature meets all requirements', TRUE),
(7, 'Market Analysis Accuracy', '2024-02-15', 'Analysis accuracy of 92%', TRUE),
(8, 'Technology Integration Test', '2024-02-20', 'Successful integration achieved', TRUE);

INSERT INTO research_publications (title, authors, journal, publication_date, impact_factor) VALUES
('Advances in AI-Powered Analytics', 'Dr. Sarah Johnson, Dr. Michael Chen', 'Journal of AI Research', '2024-01-15', 8.5),
('Sustainable Materials in Manufacturing', 'Dr. Michael Chen, Dr. Emily Rodriguez', 'Environmental Science Journal', '2024-01-20', 7.2),
('User Experience Optimization Techniques', 'Dr. Emily Rodriguez, Dr. David Kim', 'Human-Computer Interaction', '2024-01-25', 6.8),
('Energy Efficiency in Device Design', 'Dr. David Kim, Dr. Lisa Wang', 'Energy Technology Review', '2024-02-01', 7.5),
('Machine Learning Applications in Business', 'Dr. Lisa Wang, Dr. Robert Taylor', 'Machine Learning Quarterly', '2024-02-05', 9.1),
('Innovation in Product Development', 'Dr. Robert Taylor, Dr. Jennifer Brown', 'Innovation Management', '2024-02-10', 6.3),
('Market Research Methodologies', 'Dr. Jennifer Brown, Dr. Christopher Lee', 'Market Research Journal', '2024-02-15', 5.9),
('Technology Integration Strategies', 'Dr. Christopher Lee, Dr. Sarah Johnson', 'Technology Management', '2024-02-20', 7.8);

INSERT INTO innovation_metrics (project_id, metric_name, value, unit, measurement_date) VALUES
(1, 'Innovation Index', 85.5, 'Score', '2024-01-15'),
(2, 'Sustainability Rating', 92.0, 'Percentage', '2024-01-20'),
(3, 'User Satisfaction', 88.3, 'Score', '2024-01-25'),
(4, 'Energy Efficiency', 76.8, 'Percentage', '2024-02-01'),
(5, 'Performance Improvement', 94.2, 'Percentage', '2024-02-05'),
(6, 'Feature Adoption Rate', 67.5, 'Percentage', '2024-02-10'),
(7, 'Market Accuracy', 91.7, 'Percentage', '2024-02-15'),
(8, 'Integration Success Rate', 89.1, 'Percentage', '2024-02-20');

-- Verify schema setup
SELECT 'PostgreSQL stress test schema setup complete' AS status;
SELECT COUNT(*) AS total_tables FROM information_schema.tables WHERE table_schema = 'public';
SELECT 'Core sales tables ready for agent analysis' AS message;
