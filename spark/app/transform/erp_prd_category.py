from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def transform_product_categories(spark):
    """
    Transform product categories data:
    - Simple pass-through transformation that reads from source and writes to destination
    - Maintains schema with columns: id, category, subcategory, maintenance
    """
    # Read source data
    df = spark.read.csv("/user/hadoop/raw/source_erp/product_categories.csv", header=True)
    
    # Select columns (simple pass-through, no complex transformations needed)
    transformed_df = df.select(
        col("id"),
        col("category").alias("cat"),
        col("subcategory").alias("subcat"),
        col("maintenance")
    )
    
    # Write transformed data
    transformed_df.write.mode("overwrite") \
        .option("header", "true") \
        .csv("/user/hadoop/transform/source_erp/product_categories/")
    
    print(f"Transformed product categories data: {transformed_df.count()} rows processed")
    return transformed_df

def main():
    """Main ETL script execution"""
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Product Categories Transformation") \
        .getOrCreate()
    
    try:
        # Execute transformation
        transform_product_categories(spark)
        print("Product categories transformation completed successfully")
    except Exception as e:
        print(f"Error in product categories transformation: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()