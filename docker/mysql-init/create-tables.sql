-- Create database schema for source data

DROP TABLE IF EXISTS product_categories;
-- Create product categories table
CREATE TABLE  product_categories (
    ID VARCHAR(10) PRIMARY KEY,
    CAT VARCHAR(50) NOT NULL,
    SUBCAT VARCHAR(50) NOT NULL,
    MAINTENANCE VARCHAR(5)
);

DROP TABLE IF EXISTS customer_locations;
-- Create customer locations table
CREATE TABLE customer_locations (
    CID VARCHAR(20) PRIMARY KEY,
    CNTRY VARCHAR(50) NOT NULL
);

DROP TABLE IF EXISTS customers;
-- Create customer information table
CREATE TABLE customers (
    CID VARCHAR(20) PRIMARY KEY,
    BDATE VARCHAR(50),
    GEN VARCHAR(10)
);