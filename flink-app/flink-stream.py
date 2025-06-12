from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.common.time import Time
import sys
import os
import uuid
from datetime import datetime

# Add metrics package to path
sys.path.append('/app/metrics')
from metrics_collector import MetricsCollector

# Initialize metrics collector
metrics_collector = MetricsCollector('/app/metrics')

# Generate run ID
run_id = str(uuid.uuid4())[:8]

# Initialize Flink environment
env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)

# Configure checkpointing for exactly-once processing
env.enable_checkpointing(10000)  # Checkpoint every 10 seconds
env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)

# Create source table
t_env.execute_sql("""
    CREATE TABLE numbers (
        event_time TIMESTAMP(3),
        value INT,
        run_id STRING,
        input_timestamp TIMESTAMP(3)
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'mock-data-stream',
        'properties.bootstrap.servers' = 'kafka:9092',
        'properties.group.id' = 'flink-consumer',
        'format' = 'csv',
        'scan.startup.mode' = 'latest-offset'
    )
""")

# Create metrics sink table
t_env.execute_sql("""
    CREATE TABLE flink_metrics (
        run_id STRING,
        event_time TIMESTAMP(3),
        value INT,
        processing_time TIMESTAMP(3),
        latency_ms BIGINT,
        watermark_lag_ms BIGINT,
        stage_latency_map_ms BIGINT,
        stage_latency_group_ms BIGINT,
        stage_latency_window_ms BIGINT
    ) WITH (
        'connector' = 'filesystem',
        'path' = 'file:///app/metrics/flink_metrics.csv',
        'format' = 'csv'
    )
""")

# Process data and collect metrics
t_env.execute_sql("""
    INSERT INTO flink_metrics
    SELECT
        run_id,
        event_time,
        value,
        CURRENT_TIMESTAMP as processing_time,
        UNIX_TIMESTAMP(CURRENT_TIMESTAMP) - UNIX_TIMESTAMP(event_time) as latency_ms,
        UNIX_TIMESTAMP(CURRENT_TIMESTAMP) - UNIX_TIMESTAMP(WATERMARK(event_time)) as watermark_lag_ms,
        -- Stage latencies will be collected from Flink's metrics system
        0 as stage_latency_map_ms,
        0 as stage_latency_group_ms,
        0 as stage_latency_window_ms
    FROM numbers
""")

# Collect and save metrics
def collect_flink_metrics():
    # Get metrics from Flink's metrics system
    metrics = env.get_metrics()
    
    # Extract relevant metrics
    performance_metrics = {
        'latency_avg_ms': metrics.get('latency_avg', 0),
        'latency_p95_ms': metrics.get('latency_p95', 0),
        'latency_max_ms': metrics.get('latency_max', 0),
        'stage_latency_map_ms': metrics.get('stage_map_latency', 0),
        'stage_latency_group_ms': metrics.get('stage_group_latency', 0),
        'stage_latency_window_ms': metrics.get('stage_window_latency', 0),
        'watermark_lag_ms': metrics.get('watermark_lag', 0)
    }
    
    throughput_metrics = {
        'records_processed': metrics.get('records_processed', 0),
        'throughput': metrics.get('throughput', 0),
        'output_record_count': metrics.get('output_records', 0)
    }
    
    resource_metrics = {
        'shuffle_volume': metrics.get('shuffle_volume', 0),
        'partition_skew': metrics.get('partition_skew', 0),
        'failures': metrics.get('failures', 0),
        'retries': metrics.get('retries', 0)
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
        'num_partitions': 4,
        'adaptive_partitioning': False,
        'shuffle_mode': 'batch'
    }
    
    infrastructure_config = {
        'num_nodes': 1,  # Example value, should be configured
        'network_latency': 1.0  # Example value, should be configured
    }
    
    # Save metrics
    metrics_collector.collect_and_save_metrics(
        run_id=run_id,
        engine='flink',
        input_config=input_config,
        query_config=query_config,
        partitioning_config=partitioning_config,
        infrastructure_config=infrastructure_config,
        performance_metrics=performance_metrics,
        throughput_metrics=throughput_metrics,
        resource_metrics=resource_metrics,
        notes='Flink streaming job metrics'
    )

# Start metrics collection in a separate thread
import threading
metrics_thread = threading.Thread(target=collect_flink_metrics, daemon=True)
metrics_thread.start()

# Execute the job
t_env.execute("Flink Streaming Job")
