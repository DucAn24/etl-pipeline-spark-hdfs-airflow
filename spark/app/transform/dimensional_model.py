import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, col, coalesce, lit, when, expr
from pyspark.sql.window import Window
import traceback
import os
import sys

def create_spark_session():
    """Create a Spark session"""
    print("Creating Spark session for Dimensional Model...")
    return SparkSession.builder \
        .appName("Build Dimensional Model") \
        .master("spark://spark:7077") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.hadoop.user.name", "root") \
        .getOrCreate()

def build_dim_customers(spark):
    """Build customer dimension from CRM and ERP sources"""
    try:
        print("Building customer dimension...")
        
        # Load source tables from HDFS with explicit error handling
        crm_path = "hdfs://namenode:9000/transform/source_crm/cust_info.csv"
        erp_cust_path = "hdfs://namenode:9000/transform/source_erp/customers.csv"
        erp_loc_path = "hdfs://namenode:9000/transform/source_erp/customer_locations.csv"

        print(f"Loading CRM customer data from: {crm_path}")
        try:
            crm_cust_info = spark.read.csv(crm_path, header=True, inferSchema=True)
            print(f"Successfully loaded CRM customer data: {crm_cust_info.count()} rows")
        except Exception as e:
            print(f"ERROR loading CRM customer data: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise
            
        print(f"Loading ERP customer data from: {erp_cust_path}")
        try:
            erp_cust_az12 = spark.read.csv(erp_cust_path, header=True, inferSchema=True)
            print(f"Successfully loaded ERP customer data: {erp_cust_az12.count()} rows")
        except Exception as e:
            print(f"ERROR loading ERP customer data: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise
            
        print(f"Loading ERP location data from: {erp_loc_path}")
        try:
            erp_loc_a101 = spark.read.csv(erp_loc_path, header=True, inferSchema=True)
            print(f"Successfully loaded ERP location data: {erp_loc_a101.count()} rows")
        except Exception as e:
            print(f"ERROR loading ERP location data: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise
        
        # Print sample data for debugging
        print("Sample CRM customer data:")
        crm_cust_info.show(3, truncate=False)
        print("Sample ERP customer data:")
        erp_cust_az12.show(3, truncate=False)
        print("Sample ERP location data:")
        erp_loc_a101.show(3, truncate=False)
        
        # Create a window for generating surrogate keys
        print("Creating surrogate keys...")
        window_spec = Window.orderBy("cst_id")  # Using cst_id directly as in the SQL
        
        # Join the datasets and build dimension following the SQL view exactly
        print("Joining datasets to build customer dimension...")
        try:
            dim_customers = crm_cust_info \
                .join(erp_cust_az12, 
                    crm_cust_info.cst_id == erp_cust_az12.cid,  
                    "left") \
                .join(erp_loc_a101,
                    crm_cust_info.cst_id == erp_loc_a101.cid, 
                    "left") \
                .select(
                    row_number().over(window_spec).alias("customer_key"),
                    crm_cust_info.cst_id.alias("customer_id"),           # Matching SQL column names
                    crm_cust_info.cst_key.alias("customer_number"),
                    crm_cust_info.cst_firstname.alias("first_name"),
                    crm_cust_info.cst_lastname.alias("last_name"),
                    erp_loc_a101.cntry.alias("country"),
                    crm_cust_info.cst_marital_status.alias("marital_status"),
                    when(crm_cust_info.cst_gndr != 'n/a', crm_cust_info.cst_gndr)  # Using cst_gndr directly
                    .otherwise(coalesce(erp_cust_az12.gen, lit('n/a'))).alias("gender"),
                    erp_cust_az12.bdate.alias("birthdate"),
                    crm_cust_info.cst_create_date.alias("create_date")
                )
                
        except Exception as e:
            print(f"ERROR joining datasets for customer dimension: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise
        
        # Write dimension table to HDFS
        output_path = "hdfs://namenode:9000/transform/dim"
        print(f"Writing customer dimension to: {output_path}")
        try:
            write_as_single_csv(spark, dim_customers, output_path, "dim_customer.csv")
            print(f"Successfully wrote customer dimension to {output_path}/dim_customer.csv")
        except Exception as e:
            print(f"ERROR writing customer dimension: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise
        
        return dim_customers
        
    except Exception as e:
        print(f"ERROR in build_dim_customers: {str(e)}")
        print(f"Stack trace: {traceback.format_exc()}")
        raise

def build_dim_products(spark):
    """Build product dimension from CRM and ERP sources"""
    try:
        print("Building product dimension...")
        
        # Load source tables from HDFS with explicit error handling
        crm_path = "hdfs://namenode:9000/transform/source_crm/prd_info.csv"
        erp_path = "hdfs://namenode:9000/transform/source_erp/product_categories.csv"
        
        print(f"Loading CRM product data from: {crm_path}")
        try:
            crm_prd_info = spark.read.csv(crm_path, header=True, inferSchema=True)
            print(f"Successfully loaded CRM product data: {crm_prd_info.count()} rows")
        except Exception as e:
            print(f"ERROR loading CRM product data: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise
            
        print(f"Loading ERP product category data from: {erp_path}")
        try:
            erp_px_cat_g1v2 = spark.read.csv(erp_path, header=True, inferSchema=True)
            print(f"Successfully loaded ERP product category data: {erp_px_cat_g1v2.count()} rows")
        except Exception as e:
            print(f"ERROR loading ERP product category data: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise
        
        # Print sample data for debugging
        print("Sample CRM product data:")
        crm_prd_info.show(3, truncate=False)
        print("Sample ERP product category data:")
        erp_px_cat_g1v2.show(3, truncate=False)
        
        # Create a window for generating surrogate keys - exactly like SQL
        print("Creating surrogate keys...")
        window_spec = Window.orderBy("prd_start_dt", "prd_key")
        
        # Filter out historical data - exactly like SQL WHERE clause
        print("Filtering active products...")
        active_products = crm_prd_info.filter(crm_prd_info.prd_end_dt.isNull())
        print(f"Active products count: {active_products.count()}")
        
        # Join and build dimension - exactly matching SQL view columns
        print("Joining datasets to build product dimension...")
        try:
            dim_products = active_products \
                .join(erp_px_cat_g1v2, 
                    active_products.cat_id == erp_px_cat_g1v2.id, 
                    "left") \
                .select(
                    row_number().over(window_spec).alias("product_key"),
                    active_products.prd_id.alias("product_id"),
                    active_products.prd_key.alias("product_number"),
                    active_products.prd_nm.alias("product_name"),
                    active_products.cat_id.alias("category_id"),
                    erp_px_cat_g1v2.cat.alias("category"),
                    erp_px_cat_g1v2.subcat.alias("subcategory"),
                    erp_px_cat_g1v2.maintenance,
                    active_products.prd_cost.alias("cost"),
                    active_products.prd_line.alias("product_line"),
                    active_products.prd_start_dt.alias("start_date")
                )
            
            print(f"Product dimension built successfully")
        except Exception as e:
            print(f"ERROR joining datasets for product dimension: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise
        
        # Write dimension table to HDFS
        output_path = "hdfs://namenode:9000/transform/dim"
        print(f"Writing product dimension to: {output_path}")
        try:
            write_as_single_csv(spark, dim_products, output_path, "dim_product.csv")
            print(f"Successfully wrote product dimension to {output_path}/dim_product.csv")
        except Exception as e:
            print(f"ERROR writing product dimension: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise
        
        return dim_products
        
    except Exception as e:
        print(f"ERROR in build_dim_products: {str(e)}")
        print(f"Stack trace: {traceback.format_exc()}")
        raise

def build_fact_sales(spark, dim_customers, dim_products):
    """Build fact sales table with dimension foreign keys"""
    try:
        print("Building sales fact table...")
        
        # Load source data from HDFS with explicit error handling
        sales_path = "hdfs://namenode:9000/transform/source_crm/sales_details.csv"
        
        print(f"Loading sales data from: {sales_path}")
        try:
            sales_details = spark.read.csv(sales_path, header=True, inferSchema=True)
            print(f"Successfully loaded sales data: {sales_details.count()} rows")
            print("Sample sales data:")
            sales_details.show(3, truncate=False)
        except Exception as e:
            print(f"ERROR loading sales data: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise
        
        # Join with dimension tables
        print("Joining with dimension tables...")
        try:
            fact_sales = sales_details \
                .join(dim_products, 
                    sales_details.sls_prd_key == dim_products.product_number, 
                    "left") \
                .join(dim_customers,
                    sales_details.sls_cust_id == dim_customers.customer_id,
                    "left") \
                .select(
                    sales_details.sls_ord_num.alias("order_number"),
                    dim_products.product_key,
                    dim_customers.customer_key,
                    sales_details.sls_order_dt.alias("order_date"),
                    sales_details.sls_ship_dt.alias("shipping_date"),
                    sales_details.sls_due_dt.alias("due_date"),
                    sales_details.sls_sales.alias("sales_amount"),
                    sales_details.sls_quantity.alias("quantity"),
                    sales_details.sls_price.alias("price")
                )
            
            print(f"Sales fact table built successfully")
        except Exception as e:
            print(f"ERROR joining for fact sales table: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise
        
        # Write fact table to HDFS
        output_path = "hdfs://namenode:9000/transform/fact"
        print(f"Writing sales fact table to: {output_path}")
        try:
            # fact_sales.write.mode("overwrite") \
            #     .option("header", "true") \
            #     .csv(output_path)
            # print(f"Successfully wrote sales fact table to {output_path}")
            write_as_single_csv(spark, fact_sales, output_path, "fact_sales.csv")
            print(f"Successfully wrote sales fact table to {output_path}/fact_sales.csv")
        except Exception as e:
            print(f"ERROR writing sales fact table: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise
        
        return fact_sales
        
    except Exception as e:
        print(f"ERROR in build_fact_sales: {str(e)}")
        print(f"Stack trace: {traceback.format_exc()}")
        raise

def write_as_single_csv(spark, df, output_path, filename):
    """Write DataFrame as a single CSV file with a specific name"""
    temp_path = f"{output_path}/_temp"
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_path)
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    part_files = [f for f in fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(temp_path))
                  if f.getPath().getName().startswith("part-")]
    if part_files:
        src_path = part_files[0].getPath()
        dest_path = spark._jvm.org.apache.hadoop.fs.Path(f"{output_path}/{filename}")
        if fs.exists(dest_path):
            fs.delete(dest_path, True)
        fs.rename(src_path, dest_path)
        fs.delete(spark._jvm.org.apache.hadoop.fs.Path(temp_path), True)
        print(f"Successfully wrote data to {output_path}/{filename}")
    else:
        raise Exception(f"No part files found in {temp_path}")

def main():
    """Build dimensional model from source data"""
    spark = None
    try:
        print("Starting dimensional model building process...")
        spark = create_spark_session()
        
        # Print Spark and Hadoop info
        print(f"Spark version: {spark.version}")
        print(f"Spark UI: {spark.sparkContext.uiWebUrl}")
        print(f"HDFS default FS: {spark._jsc.hadoopConfiguration().get('fs.defaultFS')}")
        print(f"Active workers: {spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size() - 1}")
        
        # Build dimension tables
        dim_customers = build_dim_customers(spark)
        print(f"Built customer dimension")
        
        dim_products = build_dim_products(spark)
        print(f"Built product dimension")
        
        # Build fact table after dimensions
        fact_sales = build_fact_sales(spark, dim_customers, dim_products)
        print(f"Built sales fact table")
        
        print("Dimensional model build completed successfully")
    except Exception as e:
        print(f"ERROR building dimensional model: {str(e)}")
        print(f"Stack trace: {traceback.format_exc()}")
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
            print("Spark session stopped")

if __name__ == "__main__":
    main()