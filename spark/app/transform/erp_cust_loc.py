from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, regexp_replace, lit

def transform_customer_locations(spark):
    """
    Transform customer location data following standardization rules:
    - Remove hyphens from customer_id
    - Normalize country codes:
      - 'DE' -> 'Germany'
      - 'US'/'USA' -> 'United States'
      - Empty or NULL -> 'n/a'
      - Others -> Keep as is (trimmed)
    """
    # Read source data
    df = spark.read.csv("/user/hadoop/raw/source_erp/customer_locations.csv", header=True)
    
    # Apply transformations matching the SQL logic
    transformed_df = df.withColumn(
        "customer_id",
        regexp_replace(col("customer_id"), "-", "")
    ).withColumn(
        "country",
        when(trim(col("country")) == "DE", lit("Germany"))
        .when(trim(col("country")).isin("US", "USA"), lit("United States"))
        .when((trim(col("country")) == "") | col("country").isNull(), lit("n/a"))
        .otherwise(trim(col("country")))
    )
    
    # Write transformed data
    transformed_df.write.mode("overwrite") \
        .option("header", "true") \
        .csv("/user/hadoop/transform/source_erp/customer_locations/")
    
    
    print(f"Transformed customer locations data: {transformed_df.count()} rows processed")
    return transformed_df

def main():
    """Main ETL script execution"""
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Customer Locations Transformation") \
        .getOrCreate()
    
    try:
        # Execute transformation
        transform_customer_locations(spark)
        print("Customer locations transformation completed successfully")
    except Exception as e:
        print(f"Error in customer locations transformation: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()