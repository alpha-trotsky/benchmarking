from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, split, to_timestamp
import socket
import time
import os
import uuid

# Retry loop: wait for mock-data to be available
def wait_for_socket(host, port, timeout=30):
    for i in range(timeout):
        try:
            with socket.create_connection((host, port), timeout=2):
                print(f"Yippee connected to mock-data at {host}:{port}")
                return
        except OSError:
            print(f"Waiting for mock-data... retry {i+1}/{timeout}")
            time.sleep(1)
    raise ConnectionError(f"Could not connect to {host}:{port}")

wait_for_socket("mock-data", 9998)

# Create SparkSession
spark = SparkSession.builder \
    .appName("SparkStreaming") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Read from socket
lines = spark.readStream \
    .format("socket") \
    .option("host", "mock-data") \
    .option("port", 9998) \
    .load()

# Parse timestamp and value
df = lines.select(
    split(col("value"), ",").getItem(0).alias("event_time_str"),
    split(col("value"), ",").getItem(1).cast("int").alias("value")
).withColumn("event_time", to_timestamp("event_time_str")) \
 .withColumn("processing_time", current_timestamp()) \
 .withColumn("latency_seconds", col("processing_time").cast("long") - col("event_time").cast("long"))

# Define output directory
unique_id = str(uuid.uuid4())[:8]
output_dir = f"/app/metrics/output_{unique_id}"
checkpoint_dir = f"/app/metrics/checkpoint_{unique_id}"

os.makedirs(output_dir, exist_ok=True)
os.makedirs(checkpoint_dir, exist_ok=True)

query = df.select("event_time", "processing_time", "latency_seconds") \
    .writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", output_dir) \
    .option("checkpointLocation", checkpoint_dir) \
    .start()

# Await termination
query.awaitTermination()
