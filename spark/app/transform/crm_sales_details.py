from pyspark.sql.functions import col, when, length, expr, abs, coalesce, lit, concat
from pyspark.sql.types import DateType, DoubleType
import traceback
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.spark_utils import create_spark_session, run_transform_job

def create_crm_sales_session():
    return create_spark_session("Transform Sales Details")

def transform_sales_details(spark):
    try:
        input_path = "hdfs://namenode:9000/raw/source_crm/sales_details.csv"
        output_path = "hdfs://namenode:9000/transform/source_crm/sales_details"

        print(f"Starting sales details transformation. Reading from: {input_path}")
        try:
            df = spark.read.option("header", "true") \
                .option("inferSchema", "true") \
                .csv(input_path)
            print(f"Successfully read data: {df.count()} rows")
            df.printSchema()
        except Exception as e:
            print(f"ERROR reading sales details data: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise

        transformed_df = df \
            .withColumn("sls_order_dt_str",
                    when((col("sls_order_dt") == 0) | (length(col("sls_order_dt").cast("string")) != 8), None)
                    .otherwise(
                        concat(
                            col("sls_order_dt").cast("string").substr(1, 4),
                            lit("-"),
                            col("sls_order_dt").cast("string").substr(5, 2),
                            lit("-"),
                            col("sls_order_dt").cast("string").substr(7, 2)
                        )
                    )) \
            .withColumn("sls_order_dt", col("sls_order_dt_str").cast(DateType())) \
            .withColumn("sls_ship_dt_str",
                    when((col("sls_ship_dt") == 0) | (length(col("sls_ship_dt").cast("string")) != 8), None)
                    .otherwise(
                        concat(
                            col("sls_ship_dt").cast("string").substr(1, 4),
                            lit("-"),
                            col("sls_ship_dt").cast("string").substr(5, 2),
                            lit("-"),
                            col("sls_ship_dt").cast("string").substr(7, 2)
                        )
                    )) \
            .withColumn("sls_ship_dt", col("sls_ship_dt_str").cast(DateType())) \
            .withColumn("sls_due_dt_str",
                    when((col("sls_due_dt") == 0) | (length(col("sls_due_dt").cast("string")) != 8), None)
                    .otherwise(
                        concat(
                            col("sls_due_dt").cast("string").substr(1, 4),
                            lit("-"),
                            col("sls_due_dt").cast("string").substr(5, 2),
                            lit("-"),
                            col("sls_due_dt").cast("string").substr(7, 2)
                        )
                    )) \
            .withColumn("sls_due_dt", col("sls_due_dt_str").cast(DateType()))

        transformed_df = transformed_df \
            .withColumn("calculated_sales", col("sls_quantity") * abs(col("sls_price"))) \
            .withColumn("sls_sales", 
                    when(col("sls_sales").isNull() | 
                        (col("sls_sales") <= 0) | 
                        (col("sls_sales") != col("calculated_sales")), 
                        col("calculated_sales"))
                    .otherwise(col("sls_sales"))) \
            .withColumn("sls_price",
                    when(col("sls_price").isNull() | (col("sls_price") <= 0),
                        when(col("sls_quantity") != 0, col("sls_sales") / col("sls_quantity")).otherwise(lit(None)))
                    .otherwise(col("sls_price"))) \
            .select("sls_ord_num", "sls_prd_key", "sls_cust_id", "sls_order_dt", 
                    "sls_ship_dt", "sls_due_dt", "sls_sales", "sls_quantity", "sls_price"        )

        print(f"Writing data to: {output_path}")
        transformed_df.write.mode("overwrite") \
            .option("header", "true") \
            .csv(output_path)

        print(f"Transformed sales detail records")
        return transformed_df
    except Exception as e:
        print(f"ERROR in transform_sales_details: {str(e)}")
        print(f"Stack trace: {traceback.format_exc()}")
        raise

if __name__ == "__main__":
    run_transform_job(create_crm_sales_session, transform_sales_details)