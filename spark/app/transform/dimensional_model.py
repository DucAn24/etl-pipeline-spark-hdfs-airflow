import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, col, coalesce, lit, when, expr
from pyspark.sql.window import Window

def create_spark_session():
    """Create a Spark session"""
    return SparkSession.builder \
        .appName("Build Dimensional Model") \
        .getOrCreate()

def build_dim_customers(spark):
    """Build customer dimension from CRM and ERP sources"""
    # Load source tables
    crm_cust_info = spark.read.csv("datasets/source_crm/customer_info.csv", header=True, inferSchema=True)
    erp_cust_az12 = spark.read.csv("datasets/source_erp/cust_az12.csv", header=True, inferSchema=True)
    erp_loc_a101 = spark.read.csv("datasets/source_erp/loc_a101.csv", header=True, inferSchema=True)
    
    # Rename columns to match actual data 
    crm_cust_info = crm_cust_info.withColumnRenamed("cst_id", "customer_id") \
                                 .withColumnRenamed("cst_firstname", "first_name") \
                                 .withColumnRenamed("cst_lastname", "last_name") \
                                 .withColumnRenamed("cst_gndr", "gender") \
                                 .withColumnRenamed("cst_marital_status", "marital_status") \
                                 .withColumnRenamed("cst_key", "customer_number")
    
    # Create a window for generating surrogate keys
    window_spec = Window.orderBy("customer_id")
    
    # Join the datasets and build dimension
    dim_customers = crm_cust_info \
        .join(erp_cust_az12, 
              crm_cust_info.customer_number == erp_cust_az12.cid, 
              "left") \
        .join(erp_loc_a101,
              crm_cust_info.customer_number == erp_loc_a101.cid,
              "left") \
        .select(
            row_number().over(window_spec).alias("customer_key"),
            crm_cust_info.customer_id,
            crm_cust_info.customer_number,
            crm_cust_info.first_name,
            crm_cust_info.last_name,
            erp_loc_a101.cntry.alias("country"),
            crm_cust_info.marital_status,
            when(crm_cust_info.gender != 'n/a', crm_cust_info.gender)
            .otherwise(coalesce(erp_cust_az12.gen, lit('n/a'))).alias("gender"),
            erp_cust_az12.bdate.alias("birthdate"),
            crm_cust_info.cst_create_date.alias("create_date")
        )
    
    # Write dimension table
    dim_customers.write.mode("overwrite") \
        .option("header", "true") \
        .csv("output/dimensions/dim_customers")
    
    return dim_customers

def build_dim_products(spark):
    """Build product dimension from CRM and ERP sources"""
    # Load source tables
    crm_prd_info = spark.read.csv("datasets/source_crm/product_info.csv", header=True, inferSchema=True)
    erp_px_cat_g1v2 = spark.read.csv("datasets/source_erp/px_cat_g1v2.csv", header=True, inferSchema=True)
    
    # Rename columns to match actual data
    crm_prd_info = crm_prd_info.withColumnRenamed("prd_id", "product_id") \
                               .withColumnRenamed("prd_key", "product_key") \
                               .withColumnRenamed("prd_nm", "product_name") \
                               .withColumnRenamed("prd_cost", "cost") \
                               .withColumnRenamed("prd_line", "product_line") \
                               .withColumnRenamed("prd_start_dt", "start_date")
    
    # Filter out historical data
    active_products = crm_prd_info.filter(crm_prd_info.prd_end_dt.isNull())
    
    # Create a window for generating surrogate keys
    window_spec = Window.orderBy("start_date", "product_key")
    
    # Join and build dimension
    dim_products = active_products \
        .join(erp_px_cat_g1v2, 
              active_products.cat_id == erp_px_cat_g1v2.id, 
              "left") \
        .select(
            row_number().over(window_spec).alias("product_key"),
            active_products.product_id,
            active_products.product_key.alias("product_number"),
            active_products.product_name,
            active_products.cat_id.alias("category_id"),
            erp_px_cat_g1v2.cat.alias("category"),
            erp_px_cat_g1v2.subcat.alias("subcategory"),
            erp_px_cat_g1v2.maintenance,
            active_products.cost,
            active_products.product_line,
            active_products.start_date
        )
    
    # Write dimension table
    dim_products.write.mode("overwrite") \
        .option("header", "true") \
        .csv("output/dimensions/dim_products")
    
    return dim_products

def build_fact_sales(spark, dim_customers, dim_products):
    """Build fact sales table with dimension foreign keys"""
    # Load source data
    sales_details = spark.read.csv("datasets/source_crm/sales_details.csv", header=True, inferSchema=True)
    
    # Convert date strings to date type
    sales_details = sales_details \
        .withColumn("order_date", expr("to_date(cast(sls_order_dt as string), 'yyyyMMdd')")) \
        .withColumn("ship_date", expr("to_date(cast(sls_ship_dt as string), 'yyyyMMdd')")) \
        .withColumn("due_date", expr("to_date(cast(sls_due_dt as string), 'yyyyMMdd')"))
    
    # Join with dimension tables
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
            col("order_date"),
            col("ship_date").alias("shipping_date"),
            col("due_date"),
            sales_details.sls_sales.alias("sales_amount"),
            sales_details.sls_quantity.alias("quantity"),
            sales_details.sls_price.alias("price")
        )
    
    # Write fact table
    fact_sales.write.mode("overwrite") \
        .option("header", "true") \
        .csv("output/facts/fact_sales")
    
    return fact_sales

def main():
    """Build dimensional model from source data"""
    spark = create_spark_session()
    
    try:
        # Build dimension tables
        dim_customers = build_dim_customers(spark)
        print(f"Built customer dimension with {dim_customers.count()} rows")
        
        dim_products = build_dim_products(spark)
        print(f"Built product dimension with {dim_products.count()} rows")
        
        # Build fact table after dimensions
        fact_sales = build_fact_sales(spark, dim_customers, dim_products)
        print(f"Built sales fact table with {fact_sales.count()} rows")
        
    except Exception as e:
        print(f"Error building dimensional model: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()