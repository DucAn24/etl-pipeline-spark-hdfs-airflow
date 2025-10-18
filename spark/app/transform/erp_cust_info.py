from pyspark.sql.functions import col, regexp_replace, when, upper, trim, current_date, lit
import os
import traceback
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.spark_utils import create_spark_session, run_transform_job

def create_erp_customer_session():
    return create_spark_session("Transform ERP Customer Info")

def transform_customers(spark):
    """
    Transform customers data following standardization rules:
    - Remove 'NAS' prefix from customer_id if present
    - Set future birth dates to NULL
    - Normalize gender values (M/MALE -> Male, F/FEMALE -> Female, others -> n/a)
    """
    try:

        input_path = "hdfs://namenode:9000/raw/source_erp/customers.csv"
        output_path = "hdfs://namenode:9000/transform/source_erp/customers"
        
        print(f"Starting customers transformation. Reading from: {input_path}")
        
        try:
            df = spark.read.csv(input_path, header=True)
            print(f"Successfully read data, count: {df.count()}")
            df.printSchema()
        except Exception as e:
            print(f"Error reading source data: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise
        
        transformed_df = df.withColumn(
            "cid",
            when(col("cid").isNotNull(), regexp_replace(col("cid"), "^NAS", "")).otherwise(None)
        ).withColumn(
            "bdate",
            when(col("bdate").cast("date") > current_date(), None)
            .otherwise(col("bdate"))
        ).withColumn(
            "gen",
            when(col("gen").isNotNull(), upper(trim(col("gen"))))
            .when(upper(trim(col("gen"))).isin("F", "FEMALE"), lit("Female"))
            .when(upper(trim(col("gen"))).isin("M", "MALE"), lit("Male"))
            .otherwise(lit("n/a"))
        )
        
        print(f"Writing to output path: {output_path}")
        transformed_df.write.mode("overwrite") \
            .option("header", "true") \
            .csv(output_path)

        print(f"Transformed customers data processed")
        return transformed_df
    
    except Exception as e:
        print(f"ERROR in customer transformation: {str(e)}")
        print(f"Stack trace: {traceback.format_exc()}")
        raise

if __name__ == "__main__":
    run_transform_job(create_erp_customer_session, transform_customers)