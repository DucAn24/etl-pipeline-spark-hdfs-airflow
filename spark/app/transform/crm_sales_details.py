from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, length, expr, abs, coalesce, lit, concat
from pyspark.sql.types import DateType, DoubleType
import traceback
import os

def create_spark_session():
    """Create a Spark session"""
    print("Creating Spark session for Sales Details transformation...")
    return SparkSession.builder \
        .appName("Transform Sales Details") \
        .master("spark://spark:7077") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.hadoop.user.name", "root") \
        .getOrCreate()

def transform_sales_details(spark):
    """Transform sales details data"""
    try:
        # Define paths with explicit HDFS scheme
        input_path = "hdfs://namenode:9000/raw/source_crm/sales_details.csv"
        output_path = "hdfs://namenode:9000/transform/source_crm"
        
        print(f"Starting sales details transformation. Reading from: {input_path}")
        
        # Read sales details data from HDFS with explicit error handling
        try:
            df = spark.read.option("header", "true") \
                .option("inferSchema", "true") \
                .csv(input_path)
            print(f"Successfully read data: {df.count()} rows")
            print("Schema:")
            df.printSchema()
            
            # Show sample data
            print("Sample input data:")
            df.show(5, truncate=False)
        except Exception as e:
            print(f"ERROR reading sales details data: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise
            
        print("Applying transformations...")
        # Apply the transformations with explicit error handling
        try:
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
                
            # Debug intermediate state
            print("Date transformations completed, now calculating sales values...")
                
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
                        "sls_ship_dt", "sls_due_dt", "sls_sales", "sls_quantity", "sls_price")
            
        except Exception as e:
            print(f"ERROR during transformation: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise
        
        # Write transformed data with explicit error handling
        print(f"Writing data to: {output_path}")
        try:
            # transformed_df.write.mode("overwrite") \
            #     .option("header", "true") \
            #     .csv(output_path)
            # print(f"Successfully wrote data to {output_path}")

            input_filename = os.path.basename(input_path) 
            write_as_single_csv(spark, transformed_df, output_path, input_filename)
        except Exception as e:
            print(f"ERROR writing data: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise
        
        print(f"Transformed sales detail records")
        return transformed_df
    except Exception as e:
        print(f"ERROR in transform_sales_details: {str(e)}")
        print(f"Stack trace: {traceback.format_exc()}")
        raise

def write_as_single_csv(spark, df, output_path, filename):
    """Write DataFrame as a single CSV file with a specific name"""
    # First write using Spark's CSV writer
    temp_path = f"{output_path}/_temp"
    
    df.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(temp_path)
    
    # Use Hadoop FileSystem API to rename the part file
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    
    # Find the part file
    part_files = [f for f in fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(temp_path)) 
                  if f.getPath().getName().startswith("part-")]
    
    if part_files:
        # Source path - the first part file
        src_path = part_files[0].getPath()
        
        # Destination path with desired filename
        dest_path = spark._jvm.org.apache.hadoop.fs.Path(f"{output_path}/{filename}")
        
        # Delete the destination if it exists
        if fs.exists(dest_path):
            fs.delete(dest_path, True)
        
        # Rename the part file to the desired filename
        fs.rename(src_path, dest_path)
        
        # Delete temporary directory
        fs.delete(spark._jvm.org.apache.hadoop.fs.Path(temp_path), True)
        
        print(f"Successfully wrote data to {output_path}/{filename}")
    else:
        raise Exception(f"No part files found in {temp_path}")
    
if __name__ == "__main__":
    spark = None
    try:
        spark = create_spark_session()
        
        # Print Spark and Hadoop info
        print(f"Spark version: {spark.version}")
        print(f"Spark UI: {spark.sparkContext.uiWebUrl}")
        print(f"HDFS default FS: {spark._jsc.hadoopConfiguration().get('fs.defaultFS')}")
        print(f"Active workers: {spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size() - 1}")
        
        # Transform sales details data
        transform_sales_details(spark)
        print("Sales details transformation completed successfully")
    except Exception as e:
        print(f"ERROR in main function: {str(e)}")
        print(f"Stack trace: {traceback.format_exc()}")
        if spark:
            spark.stop()
        raise
    finally:
        if spark:
            spark.stop()
            print("Spark session stopped")