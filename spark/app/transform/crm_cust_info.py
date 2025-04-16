from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, upper, when, expr, col, row_number
from pyspark.sql.window import Window

def create_spark_session():
    """Create a Spark session"""
    return SparkSession.builder \
        .appName("Transform Customer Info") \
        .master("spark://spark:7077") \
        .config("spark.executor.memory", "1g") \
        .getOrCreate()

def transform_customer_info(spark):
    """Transform customer information data"""
    
    # Read customer info data from HDFS or local path depending on your setup

    df = spark.read.option("header", "true") \
        .option("inferSchema", "true") \
        .csv("/user/hadoop/raw/source_crm/cust_info.csv")
    
    
    # Create a window spec for finding the latest record per customer
    window_spec = Window.partitionBy("cst_id").orderBy(col("cst_create_date").desc())
    
    # Apply the transformations according to the SQL logic
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
    
    # Write transformed data to HDFS in parquet format
    transformed_df.write.mode("overwrite") \
        .option("header", "true") \
        .csv("/user/hadoop/transform/source_crm/customer_info/")
        
       
    print(f"Transformed {transformed_df.count()} customer records")
    return transformed_df

if __name__ == "__main__":
    spark = create_spark_session()
    
    # Transform customer data
    transform_customer_info(spark)
    
    spark.stop()