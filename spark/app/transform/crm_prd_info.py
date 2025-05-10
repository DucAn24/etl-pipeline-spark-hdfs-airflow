from pyspark.sql.functions import col, substring, replace, trim, upper, lead, when, isnull, lit, expr
from pyspark.sql.window import Window
import os
import traceback
import sys
sys.path.append('/usr/local/spark/app')

from utils.spark_utils import create_spark_session, write_as_single_csv, run_transform_job

def create_crm_product_session():
    """Create a Spark session for Product Info transformation"""
    return create_spark_session("Transform Product Info")

def transform_product_info(spark):
    """Transform product information data"""
    try:
        # Define paths with explicit HDFS scheme
        input_path = "hdfs://namenode:9000/raw/source_crm/prd_info.csv"
        output_path = "hdfs://namenode:9000/transform/source_crm"
        
        print(f"Starting product info transformation. Reading from: {input_path}")
        
        # Read product info data from HDFS
        try:
            df = spark.read.option("header", "true") \
                .option("inferSchema", "true") \
                .csv(input_path)
            print(f"Successfully read data: {df.count()} rows")
            df.printSchema()
        except Exception as e:
            print(f"ERROR reading product info data: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise
        
        # Create a window spec for finding the next start date per product key
        window_spec = Window.partitionBy("prd_key").orderBy("prd_start_dt")
        
        # Apply transformations
        transformed_df = df \
            .withColumn("cat_id", replace(substring(col("prd_key"), 1, 5), lit("-"), lit("_"))) \
            .withColumn("prd_key", substring(col("prd_key"), 7, 100)) \
            .withColumn("prd_cost", when(isnull(col("prd_cost")), 0).otherwise(col("prd_cost"))) \
            .withColumn("prd_line", 
                    when(upper(trim(col("prd_line"))) == "M", "Mountain")
                    .when(upper(trim(col("prd_line"))) == "R", "Road")
                    .when(upper(trim(col("prd_line"))) == "S", "Other Sales")
                    .when(upper(trim(col("prd_line"))) == "T", "Touring")
                    .otherwise("n/a")) \
            .withColumn("prd_start_dt", col("prd_start_dt").cast("date")) \
            .withColumn("next_start_dt", lead("prd_start_dt", 1).over(window_spec)) \
            .withColumn("prd_end_dt", expr("date_sub(next_start_dt, 1)")) \
            .select("prd_id", "cat_id", "prd_key", "prd_nm", "prd_cost", "prd_line", 
                    "prd_start_dt", "prd_end_dt")
        
        # Write transformed data
        print(f"Writing data to: {output_path}")
        input_filename = os.path.basename(input_path)
        write_as_single_csv(spark, transformed_df, output_path, input_filename)
            
        print(f"Transformed product records")
        return transformed_df
    except Exception as e:
        print(f"ERROR in transform_product_info: {str(e)}")
        print(f"Stack trace: {traceback.format_exc()}")
        raise

if __name__ == "__main__":
    run_transform_job(create_crm_product_session, transform_product_info)