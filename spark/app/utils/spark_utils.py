import os
import traceback
from pyspark.sql import SparkSession

def create_spark_session(app_name):
    """Create a Spark session with standard configuration
    
    Args:
        app_name (str): Name of the Spark application
        
    Returns:
        SparkSession: Configured Spark session
    """
    print(f"Creating Spark session for {app_name}...")
    return SparkSession.builder \
        .appName(app_name) \
        .master("spark://spark:7077") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.hadoop.user.name", "root") \
        .getOrCreate()

def run_transform_job(create_session_fn, transform_fn):
    """Run a Spark transform job with standard error handling
    
    Args:
        create_session_fn: Function to create Spark session
        transform_fn: Function to perform the transformation
    """
    spark = None
    try:
        spark = create_session_fn()
        
        # Print Spark and Hadoop info
        print(f"Spark version: {spark.version}")
        print(f"Spark UI: {spark.sparkContext.uiWebUrl}")
        print(f"HDFS default FS: {spark._jsc.hadoopConfiguration().get('fs.defaultFS')}")
        
        # Execute transformation
        result = transform_fn(spark)
        print("Transformation completed successfully")
        return result
    except Exception as e:
        print(f"ERROR in transformation job: {str(e)}")
        print(f"Stack trace: {traceback.format_exc()}")
        if spark:
            spark.stop()
        raise
    finally:
        if spark:
            spark.stop()
            print("Spark session stopped")