from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, length, when, upper, trim, current_date, lit

def transform_customers(spark):
    """
    Transform customers data following standardization rules:
    - Remove 'NAS' prefix from customer_id if present
    - Set future birth dates to NULL
    - Normalize gender values (M/MALE -> Male, F/FEMALE -> Female, others -> n/a)
    """
    # Read source data
    df = spark.read.csv("/user/hadoop/raw/source_erp/customers.csv", header=True)
    
    # Apply transformations matching the SQL logic
    transformed_df = df.withColumn(
        "customer_id",
        when(col("customer_id").startswith("NAS"), 
             substring(col("customer_id"), 4, length(col("customer_id"))))
        .otherwise(col("customer_id"))
    ).withColumn(
        "birth_date",
        when(col("birth_date") > current_date(), None)
        .otherwise(col("birth_date"))
    ).withColumn(
        "gender",
        when(upper(trim(col("gender"))).isin("F", "FEMALE"), lit("Female"))
        .when(upper(trim(col("gender"))).isin("M", "MALE"), lit("Male"))
        .otherwise(lit("n/a"))
    )
    
    # Write transformed data
    transformed_df.write.mode("overwrite") \
        .option("header", "true") \
        .csv("/user/hadoop/transform/source_erp/customers/")
        

    print(f"Transformed customers data: {transformed_df.count()} rows processed")
    return transformed_df

def main():
    """Main ETL script execution"""
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Customer Information Transformation") \
        .getOrCreate()
    
    try:
        # Execute transformation
        transform_customers(spark)
        print("Customer information transformation completed successfully")
    except Exception as e:
        print(f"Error in customer transformation: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()