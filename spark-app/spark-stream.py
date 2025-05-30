from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, current_timestamp
import socket
import time

# Retry loop: wait for mock-data to be available
def wait_for_socket(host, port, timeout=30):
    for i in range(timeout):
        try:
            with socket.create_connection((host, port), timeout=2):
                print(f"[✓] Connected to mock-data at {host}:{port}")
                return
        except OSError:
            print(f"[⏳] Waiting for mock-data... retry {i+1}/{timeout}")
            time.sleep(1)
    raise ConnectionError(f"[✗] Could not connect to {host}:{port}")

wait_for_socket("mock-data", 9999)

# Create SparkSession
spark = SparkSession.builder \
    .appName("SparkStreaming") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Read from socket
lines = spark.readStream \
    .format("socket") \
    .option("host", "mock-data") \
    .option("port", 9999) \
    .load()

# Parse and add proper event timestamp
df = lines.selectExpr("CAST(value AS INT) AS number") \
          .withColumn("event_time", current_timestamp())  # play around with event time

# event_time for windowing
agg = df.groupBy(window(col("event_time"), "30 seconds")).count()

# output to console
query = agg.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
