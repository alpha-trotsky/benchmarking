from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MockQueryTest") \
    .getOrCreate()

# Load CSV
df = spark.read.option("header", True).csv("/dev/shm/mock_people.csv")

# Simple query: filter age > 30
result = df.filter(df.age.cast("int") > 30)

# Save output to disk
result.write.mode("overwrite").csv("/dev/shm/mock_output")

spark.stop()
