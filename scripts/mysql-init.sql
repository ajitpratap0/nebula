-- MySQL initialization script for Nebula development environment
-- This script sets up tables and data for testing CDC functionality

-- Create database if not exists (already created by environment variables)
CREATE DATABASE IF NOT EXISTS nebula_dev;
USE nebula_dev;

-- Create a sample users table for CDC testing
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    age INT,
    city VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_email (email),
    INDEX idx_city (city)
) ENGINE=InnoDB;

-- Create a sample orders table for CDC testing
CREATE TABLE IF NOT EXISTS orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT,
    product_name VARCHAR(255) NOT NULL,
    quantity INT DEFAULT 1,
    price DECIMAL(10,2),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50) DEFAULT 'pending',
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    INDEX idx_user_id (user_id),
    INDEX idx_status (status),
    INDEX idx_order_date (order_date)
) ENGINE=InnoDB;

-- Create a sample products table
CREATE TABLE IF NOT EXISTS products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    price DECIMAL(10,2),
    stock_quantity INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_category (category),
    INDEX idx_name (name)
) ENGINE=InnoDB;

-- Insert sample users
INSERT IGNORE INTO users (name, email, age, city) VALUES
    ('John Doe', 'john@example.com', 30, 'New York'),
    ('Jane Smith', 'jane@example.com', 25, 'Los Angeles'),
    ('Bob Johnson', 'bob@example.com', 35, 'Chicago'),
    ('Alice Brown', 'alice@example.com', 28, 'Houston'),
    ('Charlie Wilson', 'charlie@example.com', 32, 'Phoenix'),
    ('David Lee', 'david@example.com', 29, 'Philadelphia'),
    ('Emma Davis', 'emma@example.com', 31, 'San Antonio'),
    ('Frank Miller', 'frank@example.com', 27, 'San Diego'),
    ('Grace Chen', 'grace@example.com', 26, 'Dallas'),
    ('Henry Garcia', 'henry@example.com', 33, 'San Jose');

-- Insert sample products
INSERT IGNORE INTO products (name, category, price, stock_quantity) VALUES
    ('Laptop Pro', 'Electronics', 999.99, 50),
    ('Wireless Mouse', 'Electronics', 25.50, 200),
    ('Mechanical Keyboard', 'Electronics', 75.00, 100),
    ('4K Monitor', 'Electronics', 299.99, 75),
    ('Bluetooth Headphones', 'Electronics', 150.00, 120),
    ('USB-C Hub', 'Electronics', 45.99, 80),
    ('Webcam HD', 'Electronics', 89.99, 60),
    ('External SSD', 'Storage', 199.99, 40),
    ('Power Bank', 'Electronics', 35.99, 150),
    ('Phone Stand', 'Accessories', 15.99, 300);

-- Insert sample orders
INSERT IGNORE INTO orders (user_id, product_name, quantity, price, status) VALUES
    (1, 'Laptop Pro', 1, 999.99, 'completed'),
    (2, 'Wireless Mouse', 2, 25.50, 'pending'),
    (1, 'Mechanical Keyboard', 1, 75.00, 'shipped'),
    (3, '4K Monitor', 1, 299.99, 'completed'),
    (4, 'Bluetooth Headphones', 1, 150.00, 'pending'),
    (5, 'USB-C Hub', 2, 45.99, 'shipped'),
    (2, 'Webcam HD', 1, 89.99, 'completed'),
    (6, 'External SSD', 1, 199.99, 'pending'),
    (7, 'Power Bank', 3, 35.99, 'completed'),
    (8, 'Phone Stand', 5, 15.99, 'shipped');

-- Create a user for replication monitoring
CREATE USER IF NOT EXISTS 'repl_monitor'@'%' IDENTIFIED BY 'monitor123';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl_monitor'@'%';

-- Grant permissions to the nebula user for CDC
GRANT SELECT, INSERT, UPDATE, DELETE ON nebula_dev.* TO 'nebula'@'%';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'nebula'@'%';

-- Create a view for monitoring binary log status
CREATE OR REPLACE VIEW replication_status AS
SELECT 
    File as current_log_file,
    Position as current_position,
    Binlog_Do_DB as replicated_databases,
    Binlog_Ignore_DB as ignored_databases
FROM information_schema.ENGINES 
WHERE Engine = 'InnoDB'
UNION ALL
SELECT 
    VARIABLE_NAME as metric_name,
    VARIABLE_VALUE as metric_value,
    '' as db1,
    '' as db2
FROM information_schema.GLOBAL_STATUS 
WHERE VARIABLE_NAME IN ('Binlog_cache_use', 'Binlog_cache_disk_use');

-- Show current binary log status
SHOW MASTER STATUS;

-- Create some stored procedures for testing
DELIMITER $$

CREATE PROCEDURE IF NOT EXISTS generate_test_data(IN num_users INT, IN num_orders INT)
BEGIN
    DECLARE i INT DEFAULT 0;
    DECLARE user_id INT;
    
    -- Generate users
    WHILE i < num_users DO
        INSERT INTO users (name, email, age, city) VALUES 
        (CONCAT('User', i), CONCAT('user', i, '@test.com'), 
         20 + (RAND() * 40), 
         ELT(1 + FLOOR(RAND() * 5), 'New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'));
        SET i = i + 1;
    END WHILE;
    
    -- Generate orders
    SET i = 0;
    WHILE i < num_orders DO
        SELECT id INTO user_id FROM users ORDER BY RAND() LIMIT 1;
        INSERT INTO orders (user_id, product_name, quantity, price, status) VALUES
        (user_id, 
         ELT(1 + FLOOR(RAND() * 10), 'Product A', 'Product B', 'Product C', 'Product D', 'Product E', 'Product F', 'Product G', 'Product H', 'Product I', 'Product J'),
         1 + FLOOR(RAND() * 5),
         10.00 + (RAND() * 990),
         ELT(1 + FLOOR(RAND() * 3), 'pending', 'shipped', 'completed'));
        SET i = i + 1;
    END WHILE;
END$$

DELIMITER ;

-- Set up binary logging variables for optimal CDC
SET GLOBAL binlog_format = 'ROW';
SET GLOBAL binlog_row_image = 'FULL';

SELECT 'MySQL initialized for Nebula development' as status;
SELECT 'Created sample tables with test data' as info;
SELECT 'Configured binary logging for CDC' as setup;
SELECT 'Use CALL generate_test_data(100, 500) to create more test data' as tip;