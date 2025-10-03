-- Raw product ingestion table for storing JSONB data with schema evolution support
CREATE TABLE raw_product_data (
    id SERIAL PRIMARY KEY,
    merchant_id VARCHAR(100) NOT NULL,
    product_data JSONB NOT NULL,
    schema_version VARCHAR(20),
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_source VARCHAR(100) DEFAULT 'MERCHANT_API'
);

-- Normalized product table for consistent querying despite schema changes
CREATE TABLE products_normalized (
    product_id VARCHAR(100) PRIMARY KEY,
    merchant_id VARCHAR(100) NOT NULL,
    product_name TEXT NOT NULL,
    price_amount DECIMAL(10,2),
    price_currency VARCHAR(10) DEFAULT 'USD',
    category VARCHAR(100),
    description TEXT,
    inventory_count INTEGER,
    tags TEXT[],  -- Array of tags
    raw_data_id INTEGER REFERENCES raw_product_data(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Schema evolution tracking table (production pattern)
CREATE TABLE schema_versions (
    version VARCHAR(20) PRIMARY KEY,
    description TEXT,
    sample_structure JSONB,
    field_mappings JSONB,  -- Store field extraction rules
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT true
);

-- Product specifications table for handling nested/complex data
CREATE TABLE product_specifications (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(100) REFERENCES products_normalized(product_id),
    spec_type VARCHAR(50),  -- 'dimensions', 'weight', 'materials', etc.
    spec_value JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create GIN indexes for JSONB performance (critical for production)
CREATE INDEX idx_raw_product_data_jsonb ON raw_product_data USING GIN (product_data);
CREATE INDEX idx_raw_product_data_merchant ON raw_product_data(merchant_id);
CREATE INDEX idx_products_normalized_merchant ON products_normalized(merchant_id);
CREATE INDEX idx_products_normalized_category ON products_normalized(category);

-- Insert schema version tracking data
INSERT INTO schema_versions (version, description, sample_structure, field_mappings) VALUES
('v1', 'Initial simple product format', 
 '{"id": "string", "name": "string", "price": "number", "category": "string"}',
 '{"product_id": "$.id", "name": "$.name", "price": "$.price", "category": "$.category"}'),

('v2', 'Added description, tags, inventory, nested price', 
 '{"id": "string", "name": "string", "price": {"amount": "number", "currency": "string"}, "category": "string", "description": "string", "tags": ["array"], "inventory_count": "number"}',
 '{"product_id": "$.id", "name": "$.name", "price": "$.price.amount", "currency": "$.price.currency", "category": "$.category", "description": "$.description", "tags": "$.tags", "inventory_count": "$.inventory_count"}'),

('v3', 'Added nested specs and promotions', 
 '{"id": "string", "name": "string", "price": {"amount": "number", "currency": "string"}, "specs": {"dimensions": "string", "weight": "string", "materials": ["array"]}, "promotions": [{"type": "string", "discount": "number", "valid_until": "string"}]}',
 '{"product_id": "$.id", "name": "$.name", "price": "$.price.amount", "specs": "$.specs", "promotions": "$.promotions"}');

-- Sample test data in different schema versions
-- V1 Format (simple)
INSERT INTO raw_product_data (merchant_id, product_data, schema_version) VALUES
('merchant_001', '{"id": "PROD_001", "name": "Basic T-Shirt", "price": 19.99, "category": "Clothing"}', 'v1'),
('merchant_002', '{"id": "PROD_002", "name": "Coffee Mug", "price": 12.50, "category": "Home"}', 'v1');

-- V2 Format (with nested price, tags, description)
INSERT INTO raw_product_data (merchant_id, product_data, schema_version) VALUES
('merchant_003', '{"id": "PROD_003", "name": "Premium Headphones", "price": {"amount": 299.99, "currency": "USD"}, "category": "Electronics", "description": "High-quality wireless headphones", "tags": ["wireless", "premium", "audio"], "inventory_count": 50}', 'v2'),
('merchant_004', '{"id": "PROD_004", "name": "Organic Soap", "price": {"amount": 8.75, "currency": "EUR"}, "category": "Beauty", "description": "Natural ingredients soap", "tags": ["organic", "natural"], "inventory_count": 200}', 'v2');

-- V3 Format (with specs and promotions)
INSERT INTO raw_product_data (merchant_id, product_data, schema_version) VALUES
('merchant_005', '{"id": "PROD_005", "name": "Gaming Laptop", "price": {"amount": 1299.99, "currency": "USD"}, "category": "Electronics", "specs": {"dimensions": "15.6 x 10.2 x 0.9 inches", "weight": "4.5 lbs", "materials": ["aluminum", "plastic"]}, "promotions": [{"type": "discount", "discount": 10, "valid_until": "2024-12-31"}]}', 'v3');

-- Variant formats (different field names - real-world challenge)
INSERT INTO raw_product_data (merchant_id, product_data, schema_version) VALUES
('merchant_006', '{"product_id": "PROD_006", "title": "Vintage Watch", "cost": 450.00, "type": "Accessories", "details": "Classic timepiece"}', 'variant'),
('merchant_007', '{"sku": "PROD_007", "product_name": "Running Shoes", "amount": 89.99, "category_name": "Sports", "tags": "running,athletic,comfort"}', 'variant');
