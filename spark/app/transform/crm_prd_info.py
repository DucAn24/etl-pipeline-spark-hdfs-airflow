from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, replace, trim, upper, lead, when, isnull, lit, expr
from pyspark.sql.window import Window

def create_spark_session():
    """Create a Spark session"""
    return SparkSession.builder \
        .appName("Transform Product Info") \
        .master("spark://spark:7077") \
        .config("spark.executor.memory", "1g") \
        .getOrCreate()

def transform_product_info(spark):
    """Transform product information data"""
    
    # Read product info data from HDFS
    df = spark.read.option("header", "true") \
        .option("inferSchema", "true") \
        .csv("/user/hadoop/raw/source_crm/prd_info.csv")
    
    # Create a window spec for finding the next start date per product key
    window_spec = Window.partitionBy("prd_key").orderBy("prd_start_dt")
    
    # Apply the transformations according to the SQL logic
    transformed_df = df \
        .withColumn("cat_id", replace(substring(col("prd_key"), 1, 5), "-", "_")) \
        .withColumn("prd_key_new", substring(col("prd_key"), 7, 100)) \
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
        .select("prd_id", "cat_id", "prd_key_new", "prd_nm", "prd_cost", "prd_line", 
                "prd_start_dt", "prd_end_dt")
    
    # Write transformed data to parquet format
    transformed_df.write.mode("overwrite") \
        .option("header", "true") \
        .csv("/user/hadoop/transform/source_crm/product_info/")
        
    
    print(f"Transformed {transformed_df.count()} product records")
    return transformed_df

if __name__ == "__main__":
    spark = create_spark_session()
    
    # Transform product data
    transform_product_info(spark)
    
    spark.stop()