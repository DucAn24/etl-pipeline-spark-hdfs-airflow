from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, upper, when, expr, col, row_number
from pyspark.sql.window import Window
import traceback
import os

def create_spark_session():
    """Create a Spark session"""
    print("Creating Spark session for Customer Info transformation...")
    return SparkSession.builder \
        .appName("Transform Customer Info") \
        .master("spark://spark:7077") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.hadoop.user.name", "root") \
        .getOrCreate()

def transform_customer_info(spark):
    """Transform customer information data"""
    try:
        # Define paths with explicit HDFS scheme
        input_path = "hdfs://namenode:9000/raw/source_crm/cust_info.csv"
        output_path = "hdfs://namenode:9000/transform/source_crm"
        
        print(f"Starting customer info transformation. Reading from: {input_path}")
        
        # Read customer info data from HDFS with explicit error handling
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
            print(f"ERROR reading customer info data: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise
        
        print("Creating window specification...")
        # Create a window spec for finding the latest record per customer
        window_spec = Window.partitionBy("cst_id").orderBy(col("cst_create_date").desc())
        
        print("Applying transformations...")
        # Apply the transformations according to the SQL logic  
        try:
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
                        "cst_marital_status", "cst_gndr", "cst_create_date")
            
        except Exception as e:
            print(f"ERROR during transformation: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise
        
        # Write transformed data to HDFS with explicit error handling
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
        
        print(f"Transformed customer records")
        return transformed_df
    except Exception as e:
        print(f"ERROR in transform_customer_info: {str(e)}")
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
        
        # Transform customer data
        transform_customer_info(spark)
        print("Customer info transformation completed successfully")
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