from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, split, to_timestamp, expr
import socket
import time
import os
import uuid
import sys
from datetime import datetime

# Add metrics package to path
sys.path.append('/app/metrics')
from metrics.metrics_collector import MetricsCollector

# Initialize metrics collector
metrics_collector = MetricsCollector('/app/metrics')

# Generate run ID
run_id = str(uuid.uuid4())[:8]

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

# Create SparkSession with metrics enabled
spark = SparkSession.builder \
    .appName("SparkStreaming") \
    .master("spark://spark-master:7077") \
    .config("spark.metrics.conf", "/app/metrics/spark-metrics.properties") \
    .config("spark.sql.streaming.metricsEnabled", "true") \
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
 .withColumn("latency_ms", expr("unix_timestamp(processing_time) - unix_timestamp(event_time)")) \
 .withColumn("run_id", expr(f"'{run_id}'"))

# Define output directory
output_dir = f"/app/metrics/spark_metrics"
checkpoint_dir = f"/app/metrics/spark_checkpoints"

os.makedirs(output_dir, exist_ok=True)
os.makedirs(checkpoint_dir, exist_ok=True)

# Process the stream and write metrics
query = df.select(
    "run_id",
    "event_time",
    "value",
    "processing_time",
    "latency_ms"
).writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", output_dir) \
    .option("checkpointLocation", checkpoint_dir) \
    .start()

# Function to collect Spark metrics
def collect_spark_metrics():
    while True:
        try:
            # Get metrics from Spark's metrics system
            metrics = spark.sparkContext._jsc.sc().getExecutorMemoryStatus()
            
            # Extract relevant metrics
            performance_metrics = {
                'latency_avg_ms': query.recentProgress[-1].durationMs if query.recentProgress else 0,
                'latency_p95_ms': query.recentProgress[-1].durationMs if query.recentProgress else 0,  # Simplified
                'latency_max_ms': query.recentProgress[-1].durationMs if query.recentProgress else 0,  # Simplified
                'stage_latency_map_ms': 0,  # Would need custom metrics
                'stage_latency_group_ms': 0,  # Would need custom metrics
                'stage_latency_window_ms': 0,  # Would need custom metrics
                'watermark_lag_ms': 0  # Would need custom metrics
            }
            
            throughput_metrics = {
                'records_processed': query.recentProgress[-1].numInputRows if query.recentProgress else 0,
                'throughput': query.recentProgress[-1].inputRowsPerSecond if query.recentProgress else 0,
                'output_record_count': query.recentProgress[-1].numOutputRows if query.recentProgress else 0
            }
            
            resource_metrics = {
                'shuffle_volume': query.recentProgress[-1].shuffleReadMetrics.totalBytesRead if query.recentProgress else 0,
                'partition_skew': 0,  # Would need custom metrics
                'failures': query.recentProgress[-1].numFailedTasks if query.recentProgress else 0,
                'retries': query.recentProgress[-1].numRetries if query.recentProgress else 0
            }
            
            # Configuration metrics
            input_config = {
                'input_rate': 1000,  # Example value, should be configured
                'send_interval': 1.0,  # Example value, should be configured
                'batch_size': 1000,  # Example value, should be configured
                'record_size': 100,  # Example value, should be configured
                'key_cardinality': 1000,  # Example value, should be configured
                'data_distribution': 'random'
            }
            
            query_config = {
                'query_type': 'streaming',
                'window_type': 'tumbling',
                'window_size': 10000,  # 10 seconds
                'num_stages': 3
            }
            
            partitioning_config = {
                'repartitioned': True,
                'repartition_strategy': 'hash',
                'num_partitions': spark.sparkContext.defaultParallelism,
                'adaptive_partitioning': False,
                'shuffle_mode': 'batch'
            }
            
            infrastructure_config = {
                'num_nodes': len(spark.sparkContext._jsc.sc().statusTracker().getExecutorMetrics()),
                'network_latency': 1.0  # Example value, should be configured
            }
            
            # Save metrics
            metrics_collector.collect_and_save_metrics(
                run_id=run_id,
                engine='spark',
                input_config=input_config,
                query_config=query_config,
                partitioning_config=partitioning_config,
                infrastructure_config=infrastructure_config,
                performance_metrics=performance_metrics,
                throughput_metrics=throughput_metrics,
                resource_metrics=resource_metrics,
                notes='Spark streaming job metrics'
            )
            
            time.sleep(10)  # Collect metrics every 10 seconds
            
        except Exception as e:
            print(f"Error collecting metrics: {e}")
            time.sleep(10)

# Start metrics collection in a separate thread
import threading
metrics_thread = threading.Thread(target=collect_spark_metrics, daemon=True)
metrics_thread.start()

# Await termination
query.awaitTermination()
