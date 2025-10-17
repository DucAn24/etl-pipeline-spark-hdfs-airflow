from pyspark.sql.functions import col
import traceback
import os
import sys
# Add the app directory to path for local utils import
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.spark_utils import create_spark_session, run_transform_job

def create_erp_category_session():
    """Create a Spark session for ERP Product Category transformation"""
    return create_spark_session("Transform ERP Product Category")

def transform_product_categories(spark):
    """
    Transform product categories data:
    - Simple pass-through transformation that reads from source and writes to destination
    - Maintains schema with columns: id, category, subcategory, maintenance
    """
    try:
        # Define paths
        input_path = "hdfs://namenode:9000/raw/source_erp/product_categories.csv"
        output_path = "hdfs://namenode:9000/transform/source_erp/product_categories"
        
        print(f"Starting product categories transformation. Reading from: {input_path}")
        
        # Read source data
        try:
            df = spark.read.csv(input_path, header=True)
            print(f"Successfully read data, count: {df.count()}")
            df.printSchema()
        except Exception as e:
            print(f"ERROR reading product categories data: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise
        
        # Select columns (simple pass-through, no complex transformations needed)
        transformed_df = df.select(
            col("id"),
            col("cat"),
            col("subcat"),
            col("maintenance")
        )
        
        # Write transformed data
        print(f"Writing data to: {output_path}")
        transformed_df.write.mode("overwrite") \
            .option("header", "true") \
            .csv(output_path)
        
        print(f"Transformed product categories data processed")
        return transformed_df
        
    except Exception as e:
        print(f"ERROR in product categories transformation: {str(e)}")
        print(f"Stack trace: {traceback.format_exc()}")
        raise

if __name__ == "__main__":
    run_transform_job(create_erp_category_session, transform_product_categories)