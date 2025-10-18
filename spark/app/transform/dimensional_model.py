import pandas as pd
from pyspark.sql.functions import row_number, col, coalesce, lit, when, expr
from pyspark.sql.window import Window
import traceback
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.spark_utils import create_spark_session, run_transform_job

def create_dim_model_session():
    return create_spark_session("Build Dimensional Model")

def build_dim_customers(spark):
    try:
        print("Building customer dimension...")
        
        # Load source tables from HDFS
        crm_path = "hdfs://namenode:9000/transform/source_crm/cust_info"
        erp_cust_path = "hdfs://namenode:9000/transform/source_erp/customers"
        erp_loc_path = "hdfs://namenode:9000/transform/source_erp/customer_locations"

        # Load data sources
        try:
            crm_cust_info = spark.read.csv(crm_path, header=True, inferSchema=True)
            erp_cust_az12 = spark.read.csv(erp_cust_path, header=True, inferSchema=True)
            erp_loc_a101 = spark.read.csv(erp_loc_path, header=True, inferSchema=True)
            
            print(f"Loaded data sources: CRM customers: {crm_cust_info.count()} rows, " +
                  f"ERP customers: {erp_cust_az12.count()} rows, " +
                  f"ERP locations: {erp_loc_a101.count()} rows")
        except Exception as e:
            print(f"ERROR loading data sources: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise
        
        # Create a window for generating surrogate keys
        window_spec = Window.orderBy("cst_id")
        
        # Join the datasets and build dimension
        try:
            dim_customers = crm_cust_info \
                .join(erp_cust_az12, 
                    crm_cust_info.cst_key == erp_cust_az12.cid,  
                    "left") \
                .join(erp_loc_a101,
                    erp_cust_az12.cid == erp_loc_a101.cid, 
                    "left") \
                .select(
                    row_number().over(window_spec).alias("customer_key"),
                    crm_cust_info.cst_id.alias("customer_id"),
                    crm_cust_info.cst_key.alias("customer_number"),
                    crm_cust_info.cst_firstname.alias("first_name"),
                    crm_cust_info.cst_lastname.alias("last_name"),
                    erp_loc_a101.cntry.alias("country"),
                    crm_cust_info.cst_marital_status.alias("marital_status"),
                    when(crm_cust_info.cst_gndr != 'n/a', crm_cust_info.cst_gndr)
                    .otherwise(coalesce(erp_cust_az12.gen, lit('n/a'))).alias("gender"),
                    erp_cust_az12.bdate.alias("birthdate"),
                    crm_cust_info.cst_create_date.alias("create_date")
                )
                
        except Exception as e:
            print(f"ERROR joining datasets for customer dimension: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise
        
        output_path = "hdfs://namenode:9000/transform/dim/dim_customer"
        dim_customers.write.mode("overwrite") \
            .option("header", "true") \
            .csv(output_path)
        
        return dim_customers
        
    except Exception as e:
        print(f"ERROR in build_dim_customers: {str(e)}")
        print(f"Stack trace: {traceback.format_exc()}")
        raise

def build_dim_products(spark):
    try:
        print("Building product dimension...")
        
        # Load source tables from HDFS
        crm_path = "hdfs://namenode:9000/transform/source_crm/prd_info"
        erp_path = "hdfs://namenode:9000/transform/source_erp/product_categories"
        
        # Load data sources
        try:
            crm_prd_info = spark.read.csv(crm_path, header=True, inferSchema=True)
            erp_px_cat_g1v2 = spark.read.csv(erp_path, header=True, inferSchema=True)
            
            print(f"Loaded data sources: CRM products: {crm_prd_info.count()} rows, " +
                  f"ERP product categories: {erp_px_cat_g1v2.count()} rows")
        except Exception as e:
            print(f"ERROR loading data sources: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise
        
        # Create a window for generating surrogate keys
        window_spec = Window.orderBy("prd_start_dt", "prd_key")
        
        # Filter out historical data
        active_products = crm_prd_info.filter(crm_prd_info.prd_end_dt.isNull())
        
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
        except Exception as e:
            print(f"ERROR joining datasets for product dimension: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise
        
        output_path = "hdfs://namenode:9000/transform/dim/dim_product"
        dim_products.write.mode("overwrite") \
            .option("header", "true") \
            .csv(output_path)
        
        return dim_products
        
    except Exception as e:
        print(f"ERROR in build_dim_products: {str(e)}")
        print(f"Stack trace: {traceback.format_exc()}")
        raise

def build_fact_sales(spark, dim_customers, dim_products):
    try:
        print("Building sales fact table...")
        
        sales_path = "hdfs://namenode:9000/transform/source_crm/sales_details"
        
        try:
            sales_details = spark.read.csv(sales_path, header=True, inferSchema=True)
            print(f"Loaded sales data: {sales_details.count()} rows")
        except Exception as e:
            print(f"ERROR loading sales data: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise
        
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
        except Exception as e:
            print(f"ERROR joining for fact sales table: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise
        
        output_path = "hdfs://namenode:9000/transform/fact/fact_sales"
        fact_sales.write.mode("overwrite") \
            .option("header", "true") \
            .csv(output_path)
        
        return fact_sales
    except Exception as e:
        print(f"ERROR in build_fact_sales: {str(e)}")
        print(f"Stack trace: {traceback.format_exc()}")
        raise

def build_dimensional_model(spark):
    try:
        dim_customers = build_dim_customers(spark)
        dim_products = build_dim_products(spark)
        
        fact_sales = build_fact_sales(spark, dim_customers, dim_products)
        
        print("Dimensional model built successfully")
        return {
            "dim_customers": dim_customers,
            "dim_products": dim_products,
            "fact_sales": fact_sales
        }
    except Exception as e:
        print(f"ERROR building dimensional model: {str(e)}")
        print(f"Stack trace: {traceback.format_exc()}")
        raise

if __name__ == "__main__":
    run_transform_job(create_dim_model_session, build_dimensional_model)