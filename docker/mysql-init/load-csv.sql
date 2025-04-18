-- Load data from CSV files

-- Load product categories data
LOAD DATA INFILE '/var/lib/mysql-files/PX_CAT_G1V2.csv'
INTO TABLE product_categories
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(id, category, subcategory, maintenance);

-- Load customer locations data
LOAD DATA INFILE '/var/lib/mysql-files/LOC_A101.csv'
INTO TABLE customer_locations
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(customer_id, country);

-- Load customer information data
LOAD DATA INFILE '/var/lib/mysql-files/CUS_AZ12.csv'
INTO TABLE customers
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@CID, @BDATE, @GEN)
SET 
  customer_id = @CID,
  birth_date = @BDATE,
  gender = @GEN;