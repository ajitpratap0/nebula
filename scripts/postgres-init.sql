-- PostgreSQL initialization script for Nebula development environment
-- This script sets up tables and data for testing CDC functionality

-- Create schema for testing
CREATE SCHEMA IF NOT EXISTS testing;

-- Create a sample users table for CDC testing
CREATE TABLE IF NOT EXISTS testing.users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    age INTEGER,
    city VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create a sample orders table for CDC testing
CREATE TABLE IF NOT EXISTS testing.orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES testing.users(id),
    product_name VARCHAR(255) NOT NULL,
    quantity INTEGER DEFAULT 1,
    price DECIMAL(10,2),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50) DEFAULT 'pending'
);

-- Create a trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_users_updated_at 
    BEFORE UPDATE ON testing.users 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Insert sample data
INSERT INTO testing.users (name, email, age, city) VALUES
    ('John Doe', 'john@example.com', 30, 'New York'),
    ('Jane Smith', 'jane@example.com', 25, 'Los Angeles'),
    ('Bob Johnson', 'bob@example.com', 35, 'Chicago'),
    ('Alice Brown', 'alice@example.com', 28, 'Houston'),
    ('Charlie Wilson', 'charlie@example.com', 32, 'Phoenix')
ON CONFLICT (email) DO NOTHING;

INSERT INTO testing.orders (user_id, product_name, quantity, price, status) VALUES
    (1, 'Laptop', 1, 999.99, 'completed'),
    (2, 'Mouse', 2, 25.50, 'pending'),
    (1, 'Keyboard', 1, 75.00, 'shipped'),
    (3, 'Monitor', 1, 299.99, 'completed'),
    (4, 'Headphones', 1, 150.00, 'pending')
ON CONFLICT DO NOTHING;

-- Create a publication for logical replication (CDC)
SELECT pg_create_logical_replication_slot('nebula_slot', 'pgoutput') 
WHERE NOT EXISTS (
    SELECT 1 FROM pg_replication_slots WHERE slot_name = 'nebula_slot'
);

-- Create publication for all tables in testing schema
DROP PUBLICATION IF EXISTS nebula_publication;
CREATE PUBLICATION nebula_publication FOR ALL TABLES IN SCHEMA testing;

-- Grant necessary permissions
GRANT USAGE ON SCHEMA testing TO nebula;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA testing TO nebula;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA testing TO nebula;

-- Grant replication permissions
ALTER USER nebula WITH REPLICATION;

-- Create a view for monitoring replication lag
CREATE OR REPLACE VIEW testing.replication_status AS
SELECT 
    slot_name,
    plugin,
    slot_type,
    database,
    active,
    active_pid,
    confirmed_flush_lsn,
    restart_lsn
FROM pg_replication_slots
WHERE slot_name = 'nebula_slot';

GRANT SELECT ON testing.replication_status TO nebula;

-- Add some logging
\echo 'PostgreSQL initialized for Nebula development'
\echo 'Created testing schema with sample data'
\echo 'Configured logical replication slot: nebula_slot'
\echo 'Created publication: nebula_publication'