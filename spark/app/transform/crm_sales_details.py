from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, length, expr, abs, coalesce, lit, concat
from pyspark.sql.types import DateType, DoubleType

def create_spark_session():
    """Create a Spark session"""
    return SparkSession.builder \
        .appName("Transform Sales Details") \
        .master("spark://spark:7077") \
        .config("spark.executor.memory", "1g") \
        .getOrCreate()

def transform_sales_details(spark):
    """Transform sales details data"""
    
    # Read sales details data from HDFS
    df = spark.read.option("header", "true") \
        .option("inferSchema", "true") \
        .csv("/user/hadoop/raw/source_crm/sales_details.csv")
    
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
        .withColumn("sls_due_dt", col("sls_due_dt_str").cast(DateType())) \
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
                "sls_ship_dt", "sls_due_dt", "sls_sales", "sls_quantity", "sls_price")
    
    # Write transformed data to HDFS in parquet format
    transformed_df.write.mode("overwrite") \
        .option("header", "true") \
        .csv("/user/hadoop/transform/source_crm/sales_details/")
        
    
    print(f"Transformed {transformed_df.count()} sales detail records")
    return transformed_df

if __name__ == "__main__":
    spark = create_spark_session()
    
    # Transform sales details data
    transform_sales_details(spark)
    
    spark.stop()