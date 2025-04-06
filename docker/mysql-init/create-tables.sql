-- Create database schema for source data

DROP TABLE IF EXISTS product_categories;
-- Create product categories table
CREATE TABLE  product_categories (
    id VARCHAR(10) PRIMARY KEY,
    category VARCHAR(50) NOT NULL,
    subcategory VARCHAR(50) NOT NULL,
    maintenance VARCHAR(5)
);

DROP TABLE IF EXISTS customer_locations;
-- Create customer locations table
CREATE TABLE customer_locations (
    customer_id VARCHAR(20) PRIMARY KEY,
    country VARCHAR(50) NOT NULL
);

DROP TABLE IF EXISTS customers;
-- Create customer information table
CREATE TABLE customers (
    customer_id VARCHAR(20) PRIMARY KEY,
    birth_date VARCHAR(50),
    gender VARCHAR(10)
);