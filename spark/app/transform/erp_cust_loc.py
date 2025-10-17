from pyspark.sql.functions import col, trim, when, regexp_replace, lit
import traceback
import os
import sys
# Add the app directory to path for local utils import
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.spark_utils import create_spark_session, run_transform_job

def create_erp_location_session():
    """Create a Spark session for ERP Customer Location transformation"""
    return create_spark_session("Transform ERP Customer Location")

def transform_customer_locations(spark):
    """
    Transform customer location data following standardization rules:
    - Remove hyphens from customer_id
    - Normalize country codes:
      - 'DE' -> 'Germany'
      - 'US'/'USA' -> 'United States'
      - Empty or NULL -> 'n/a'
      - Others -> Keep as is (trimmed)
    """
    try:
        # Define paths
        input_path = "hdfs://namenode:9000/raw/source_erp/customer_locations.csv"
        output_path = "hdfs://namenode:9000/transform/source_erp/customer_locations"
        
        print(f"Starting customer locations transformation. Reading from: {input_path}")
        
        # Read source data
        try:
            df = spark.read.csv(input_path, header=True)
            print(f"Successfully read data, count: {df.count()}")
            df.printSchema()
        except Exception as e:
            print(f"Error reading source data: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise
        
        # Apply transformations
        transformed_df = df.withColumn(
            "cid",
            regexp_replace(col("cid"), "-", "")
        ).withColumn(
            "cntry",
            when(trim(col("cntry")) == "DE", lit("Germany"))
            .when(trim(col("cntry")).isin("US", "USA"), lit("United States"))
            .when((trim(col("cntry")) == "") | col("cntry").isNull(), lit("n/a"))
            .otherwise(trim(col("cntry")))
        )
        
        # Write transformed data
        print(f"Writing to output path: {output_path}")
        transformed_df.write.mode("overwrite") \
            .option("header", "true") \
            .csv(output_path)
        
        print(f"Transformed customer locations data processed")
        return transformed_df
    
    except Exception as e:
        print(f"ERROR in customer locations transformation: {str(e)}")
        print(f"Stack trace: {traceback.format_exc()}")
        raise

if __name__ == "__main__":
    run_transform_job(create_erp_location_session, transform_customer_locations)