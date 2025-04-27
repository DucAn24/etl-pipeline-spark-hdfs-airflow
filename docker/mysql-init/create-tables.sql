-- Create database schema for source data

DROP TABLE IF EXISTS product_categories;
-- Create product categories table
CREATE TABLE  product_categories (
    id VARCHAR(10) PRIMARY KEY,
    cat VARCHAR(50) NOT NULL,
    subcat VARCHAR(50) NOT NULL,
    maintenance VARCHAR(5)
);

DROP TABLE IF EXISTS customer_locations;
-- Create customer locations table
CREATE TABLE customer_locations (
    cid VARCHAR(20) PRIMARY KEY,
    cntry VARCHAR(50) NOT NULL
);

DROP TABLE IF EXISTS customers;
-- Create customer information table
CREATE TABLE customers (
    cid VARCHAR(20) PRIMARY KEY,
    bdate VARCHAR(50),
    gen VARCHAR(10)
);