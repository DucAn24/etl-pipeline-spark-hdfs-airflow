from pyspark.sql import SparkSession
import traceback
import sys
from pyspark.sql.functions import col

def create_spark_session():
    return SparkSession.builder \
        .appName("Load to Data Warehouse") \
        .master("spark://spark:7077") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.hadoop.user.name", "root") \
        .config("spark.jars", "/opt/spark/resources/jars/postgresql-42.7.5.jar") \
        .getOrCreate()

def load_dim_customer(spark, jdbc_url, jdbc_properties):
    try:
        print("Loading customer dimension to PostgreSQL...")
        
        hdfs_path = "hdfs://namenode:9000/transform/dim/dim_customer"
        
        print(f"Reading customer dimension from: {hdfs_path}")
        try:
            dim_customer = spark.read.csv(hdfs_path, header=True, inferSchema=True)
            print(f"Successfully read customer dimension: {dim_customer.count()} rows")
            print("Sample data:")
            dim_customer.show(3, truncate=False)
        except Exception as e:
            print(f"ERROR reading customer dimension: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise
        
        table_name = "dim_customer"
        print(f"Writing customer dimension to PostgreSQL table: {table_name}")
        try:
            dim_customer.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", table_name) \
                .option("user", jdbc_properties["user"]) \
                .option("password", jdbc_properties["password"]) \
                .option("driver", jdbc_properties["driver"]) \
                .mode("overwrite") \
                .save()
                
            print(f"Successfully loaded customer dimension to PostgreSQL")
            return True
        except Exception as e:
            print(f"ERROR loading customer dimension to PostgreSQL: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise
    except Exception as e:
        print(f"ERROR in load_dim_customer: {str(e)}")
        print(f"Stack trace: {traceback.format_exc()}")
        return False

def load_dim_product(spark, jdbc_url, jdbc_properties):
    try:
        print("Loading product dimension to PostgreSQL...")
        
        hdfs_path = "hdfs://namenode:9000/transform/dim/dim_product"
        
        print(f"Reading product dimension from: {hdfs_path}")
        try:
            dim_product = spark.read.csv(hdfs_path, header=True, inferSchema=True)
            print(f"Successfully read product dimension: {dim_product.count()} rows")
            print("Sample data:")
            dim_product.show(3, truncate=False)
        except Exception as e:
            print(f"ERROR reading product dimension: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise
        
        table_name = "dim_product"
        print(f"Writing product dimension to PostgreSQL table: {table_name}")
        try:
            dim_product.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", table_name) \
                .option("user", jdbc_properties["user"]) \
                .option("password", jdbc_properties["password"]) \
                .option("driver", jdbc_properties["driver"]) \
                .mode("overwrite") \
                .save()
                
            print(f"Successfully loaded product dimension to PostgreSQL")
            return True
        except Exception as e:
            print(f"ERROR loading product dimension to PostgreSQL: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise
    except Exception as e:
        print(f"ERROR in load_dim_product: {str(e)}")
        print(f"Stack trace: {traceback.format_exc()}")
        return False

def load_fact_sales(spark, jdbc_url, jdbc_properties):
    try:
        print("Loading sales fact table to PostgreSQL...")
        
        hdfs_path = "hdfs://namenode:9000/transform/fact/fact_sales"
        
        print(f"Reading sales fact table from: {hdfs_path}")
        try:
            fact_sales = spark.read.csv(hdfs_path, header=True, inferSchema=True)
            print(f"Successfully read sales fact table: {fact_sales.count()} rows")
            print("Sample data:")
            fact_sales.show(3, truncate=False)
        except Exception as e:
            print(f"ERROR reading sales fact table: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise
        
        table_name = "fact_sales"
        print(f"Writing sales fact table to PostgreSQL table: {table_name}")
        try:
            fact_sales = fact_sales.withColumn("product_key", col("product_key").cast("integer"))
            fact_sales = fact_sales.withColumn("customer_key", col("customer_key").cast("integer"))
            
            fact_sales.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", table_name) \
                .option("user", jdbc_properties["user"]) \
                .option("password", jdbc_properties["password"]) \
                .option("driver", jdbc_properties["driver"]) \
                .mode("overwrite") \
                .save()
                
            print(f"Successfully loaded sales fact table to PostgreSQL")
            return True
        except Exception as e:
            print(f"ERROR loading sales fact table to PostgreSQL: {str(e)}")
            print(f"Stack trace: {traceback.format_exc()}")
            raise
    except Exception as e:
        print(f"ERROR in load_fact_sales: {str(e)}")
        print(f"Stack trace: {traceback.format_exc()}")
        return False

def create_db_schema(spark, jdbc_url, jdbc_properties):
    try:
        print("Creating database schema in PostgreSQL...")

        schema_name = "dwh"
        
        df = spark.createDataFrame([("1",)], ["dummy"])
        
        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "schema_init") \
            .option("user", jdbc_properties["user"]) \
            .option("password", jdbc_properties["password"]) \
            .option("driver", jdbc_properties["driver"]) \
            .option("createTableColumnTypes", "dummy VARCHAR(255)") \
            .option("createTableOptions", f"SCHEMA {schema_name}") \
            .mode("overwrite") \
            .save()
        
        print(f"Successfully created schema {schema_name}")
        return True
    except Exception as e:
        print(f"ERROR creating database schema: {str(e)}")
        print(f"Stack trace: {traceback.format_exc()}")
        return False


def main():
    spark = None
    
    try:
        
        print("Starting data warehouse loading process...")
        spark = create_spark_session()
        
        print(f"Spark version: {spark.version}")
        print(f"Spark UI: {spark.sparkContext.uiWebUrl}")
        print(f"HDFS default FS: {spark._jsc.hadoopConfiguration().get('fs.defaultFS')}")
        print(f"Active workers: {spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size() - 1}")
        
        jdbc_url = "jdbc:postgresql://postgres_dw:5432/dwh"
        jdbc_properties = {
            "user": "airflow",
            "password": "airflow",
            "driver": "org.postgresql.Driver"
        }
        
        create_db_schema(spark, jdbc_url, jdbc_properties)
        
        success_dim_customer = load_dim_customer(spark, jdbc_url, jdbc_properties)
        success_dim_product = load_dim_product(spark, jdbc_url, jdbc_properties)
        
        if success_dim_customer and success_dim_product:
            success_fact_sales = load_fact_sales(spark, jdbc_url, jdbc_properties)
            if success_fact_sales:
                print("Successfully loaded all tables to PostgreSQL data warehouse")
                sys.exit(0)
            else:
                print("Failed to load sales fact table")
                sys.exit(1)
        else:
            print("Failed to load dimension tables")
            sys.exit(1)
    
    except Exception as e:
        print(f"ERROR in data warehouse loading: {str(e)}")
        print(f"Stack trace: {traceback.format_exc()}")
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
            print("Spark session stopped")

if __name__ == "__main__":
    main()