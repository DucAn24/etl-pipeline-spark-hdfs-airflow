from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("Test Workers") \
    .master("spark://spark:7077") \
    .config("spark.executor.instances", "2") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.memory", "1024m") \
    .getOrCreate()

# Verify connection
print(f"Active Workers: {spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size()}")

# Create test data with enough partitions to distribute
test_data = range(1000)
rdd = spark.sparkContext.parallelize(test_data, 2)  # 2 partitions for 2 workers

# Run a simple computation
result = rdd.map(lambda x: x * 2).reduce(lambda a, b: a + b)
print(f"Computation result: {result}")

# Show partition distribution
print(f"Number of partitions: {rdd.getNumPartitions()}")
print("Partition locations:")
print(rdd.glom().map(len).collect())

# Stop the Spark session
spark.stop()