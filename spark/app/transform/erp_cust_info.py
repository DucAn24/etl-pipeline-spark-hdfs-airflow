from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, when, upper, trim, current_date, lit
import traceback
import os

def transform_customers(spark):
    """
    Transform customers data following standardization rules:
    - Remove 'NAS' prefix from customer_id if present
    - Set future birth dates to NULL
    - Normalize gender values (M/MALE -> Male, F/FEMALE -> Female, others -> n/a)
    """
    try:
        # Debug info
        print("Starting customers transformation")
        input_path = "hdfs://namenode:9000/raw/source_erp/customers.csv"
        output_path = "hdfs://namenode:9000/transform/source_erp"
        
        print(f"Reading from HDFS path: {input_path}")
        
        # Check if the input path exists
        try:
            if not spark._jsc.sc().hadoopConfiguration().get("fs.defaultFS").startswith("hdfs"):
                print("WARNING: Not using HDFS as default file system")
                
            input_exists = spark._jsc.hadoopConfiguration().get("fs.defaultFS") + input_path
            print(f"Checking if path exists: {input_exists}")
        except Exception as e:
            print(f"Error checking path: {str(e)}")
        
        # Read source data with verbose error handling
        try:
            df = spark.read.csv(input_path, header=True)
            print(f"Successfully read data, count: {df.count()}, schema: {df.schema}")
        except Exception as e:
            print(f"Error reading source data: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise
        
        # Apply transformations matching the SQL logic
        print("Applying transformations...")
        transformed_df = df.withColumn(
            "cid",
            regexp_replace(col("cid"), "^NAS", "")
            # when(col("customer_id").startswith("NAS"), 
            #      substring(col("customer_id"), 4, length(col("customer_id"))))
            # .otherwise(col("customer_id"))
        ).withColumn(
            "bdate",
            when(col("bdate") > current_date(), None)
            .otherwise(col("bdate"))
        ).withColumn(
            "gen",
            when(upper(trim(col("gen"))).isin("F", "FEMALE"), lit("Female"))
            .when(upper(trim(col("gen"))).isin("M", "MALE"), lit("Male"))
            .otherwise(lit("n/a"))
        )
        
        # Debug transformed data
        # print(f"Transformation complete. Row count: {transformed_df.count()}") # Removed intermediate action
        # print("Sample data after transformation:") # Removed intermediate action
        # transformed_df.show(5, truncate=False) # Removed intermediate action
        
        # Write transformed data
        print(f"Writing to output path: {output_path}")
        try:
            # transformed_df.write.mode("overwrite") \
            #     .option("header", "true") \
            #     .csv(output_path)
            # print(f"Successfully wrote data to {output_path}")

            input_filename = os.path.basename(input_path) 
            write_as_single_csv(spark, transformed_df, output_path, input_filename)
        except Exception as e:
            print(f"Error writing transformed data: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise

        print(f"Transformed customers data processed") # Removed count action
        return transformed_df
    
    except Exception as e:
        print(f"ERROR in customer transformation: {str(e)}")
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

def create_spark_session():
    """Create a Spark session"""
    print("Creating Spark session for ERP Customer Info transformation...")
    return SparkSession.builder \
        .appName("Transform ERP Customer Info") \
        .master("spark://spark:7077") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.hadoop.user.name", "root") \
        .getOrCreate()

def main():
    """Main ETL script execution"""
    # Initialize Spark session
    spark = None
    try:
        spark = create_spark_session()
        print(f"Spark session created. Version: {spark.version}")
        print(f"Spark UI: {spark.sparkContext.uiWebUrl}")
        
        # Verify Hadoop configuration
        hdfs_url = spark._jsc.hadoopConfiguration().get("fs.defaultFS")
        print(f"HDFS URL: {hdfs_url}")
        
        # Execute transformation
        transform_customers(spark)
        print("Customer information transformation completed successfully")
    except Exception as e:
        print(f"Error in customer transformation: {str(e)}")
        print(f"Stack trace: {traceback.format_exc()}")
        if spark:
            spark.stop()
        raise
    finally:
        if spark:
            spark.stop()
            print("Spark session stopped")

if __name__ == "__main__":
    main()