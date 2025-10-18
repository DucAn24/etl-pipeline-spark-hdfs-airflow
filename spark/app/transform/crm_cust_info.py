from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, upper, when, expr, col, row_number
from pyspark.sql.window import Window
import traceback

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.spark_utils import create_spark_session, run_transform_job

def create_crm_customer_session():
    return create_spark_session("Transform Customer Info")

def transform_customer_info(spark):
    try:
        input_path = "hdfs://namenode:9000/raw/source_crm/cust_info.csv"
        output_path = "hdfs://namenode:9000/transform/source_crm/cust_info"

        print(f"Starting customer info transformation. Reading from: {input_path}")
        try:
            df = spark.read.option("header", "true") \
                .option("inferSchema", "true") \
                .csv(input_path)
            print(f"Successfully read data: {df.count()} rows")
            df.printSchema()
        except Exception as e:
            print(f"ERROR reading customer info data: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise

        window_spec = Window.partitionBy("cst_id").orderBy(col("cst_create_date").desc())
        
        transformed_df = df \
            .withColumn("flag_last", row_number().over(window_spec)) \
            .filter(col("flag_last") == 1) \
            .filter(col("cst_id").isNotNull()) \
            .withColumn("cst_firstname", trim(col("cst_firstname"))) \
            .withColumn("cst_lastname", trim(col("cst_lastname"))) \
            .withColumn("cst_marital_status", 
                    when(upper(trim(col("cst_marital_status"))) == "S", "Single")
                    .when(upper(trim(col("cst_marital_status"))) == "M", "Married")
                    .otherwise("n/a")) \
            .withColumn("cst_gndr", 
                    when(upper(trim(col("cst_gndr"))) == "F", "Female")
                    .when(upper(trim(col("cst_gndr"))) == "M", "Male")
                    .otherwise("n/a")) \
            .select("cst_id", "cst_key", "cst_firstname", "cst_lastname", 
                    "cst_marital_status", "cst_gndr", "cst_create_date"        )

        print(f"Writing data to: {output_path}")
        transformed_df.write.mode("overwrite") \
            .option("header", "true") \
            .csv(output_path)

        print(f"Transformed customer records")
        return transformed_df
    except Exception as e:
        print(f"ERROR in transform_customer_info: {str(e)}")
        print(f"Stack trace: {traceback.format_exc()}")
        raise

if __name__ == "__main__":
    run_transform_job(create_crm_customer_session, transform_customer_info)