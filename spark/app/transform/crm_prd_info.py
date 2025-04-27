from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, replace, trim, upper, lead, when, isnull, lit, expr
from pyspark.sql.window import Window
import traceback
import os

def create_spark_session():
    """Create a Spark session"""
    print("Creating Spark session for Product Info transformation...")
    return SparkSession.builder \
        .appName("Transform Product Info") \
        .master("spark://spark:7077") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.hadoop.user.name", "root") \
        .getOrCreate()

def transform_product_info(spark):
    """Transform product information data"""
    try:
        # Define paths with explicit HDFS scheme
        input_path = "hdfs://namenode:9000/raw/source_crm/prd_info.csv"
        output_path = "hdfs://namenode:9000/transform/source_crm"
        
        print(f"Starting product info transformation. Reading from: {input_path}")
        
        # Read product info data from HDFS with explicit error handling
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
            print(f"ERROR reading product info data: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise
        
        print("Creating window specification...")
        # Create a window spec for finding the next start date per product key
        window_spec = Window.partitionBy("prd_key").orderBy("prd_start_dt")
        
        print("Applying transformations...")
        # Apply the transformations according to the SQL logic CO-RF-FR-R92B-58
        try:
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
            input_filename = os.path.basename(input_path)  # customers.csv
            write_as_single_csv(spark, transformed_df, output_path, input_filename)
            
        except Exception as e:
            print(f"ERROR writing data: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise
            
        print(f"Transformed product records")
        return transformed_df
    except Exception as e:
        print(f"ERROR in transform_product_info: {str(e)}")
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
        
        # Transform product data
        transform_product_info(spark)
        print("Product info transformation completed successfully")
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