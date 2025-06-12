from dataclasses import dataclass
from typing import Dict, Optional
from datetime import datetime

@dataclass
class BenchmarkMetrics:
    # Run Identification
    run_id: str
    engine: str  # 'flink' or 'spark'
    timestamp: datetime
    notes: str

    # Input Configuration
    input_rate: float  # records per second
    send_interval: float  # milliseconds
    batch_size: int
    record_size: int  # bytes
    key_cardinality: int
    data_distribution: str  # 'random' or 'set'

    # Query Configuration
    query_type: str
    window_type: Optional[str]  # 'tumbling', 'sliding', 'session', or None
    window_size: Optional[int]  # milliseconds
    num_stages: int

    # Partitioning Configuration
    repartitioned: bool
    repartition_strategy: Optional[str]
    num_partitions: int
    adaptive_partitioning: bool
    shuffle_mode: str

    # Infrastructure
    num_nodes: int
    network_latency: float  # milliseconds

    # Performance Metrics
    latency_avg_ms: float
    latency_p95_ms: float
    latency_max_ms: float
    stage_latency_map_ms: float
    stage_latency_group_ms: float
    stage_latency_window_ms: float
    watermark_lag_ms: float

    # Throughput Metrics
    records_processed: int
    throughput: float  # records per second
    output_record_count: int

    # Resource Utilization
    shuffle_volume: int  # bytes
    partition_skew: float  # 0-1 ratio
    repartition_effect: Optional[float]  # percentage improvement
    failures: int
    retries: int
    memory_utilization_avg: float  # percentage
    cpu_utilization_avg: float  # percentage

    def to_dict(self) -> Dict:
        """Convert metrics to dictionary for CSV writing"""
        return {
            'run_id': self.run_id,
            'engine': self.engine,
            'timestamp': self.timestamp.isoformat(),
            'notes': self.notes,
            'input_rate': self.input_rate,
            'send_interval': self.send_interval,
            'batch_size': self.batch_size,
            'record_size': self.record_size,
            'key_cardinality': self.key_cardinality,
            'data_distribution': self.data_distribution,
            'query_type': self.query_type,
            'window_type': self.window_type,
            'window_size': self.window_size,
            'num_stages': self.num_stages,
            'repartitioned': self.repartitioned,
            'repartition_strategy': self.repartition_strategy,
            'num_partitions': self.num_partitions,
            'adaptive_partitioning': self.adaptive_partitioning,
            'shuffle_mode': self.shuffle_mode,
            'num_nodes': self.num_nodes,
            'network_latency': self.network_latency,
            'latency_avg_ms': self.latency_avg_ms,
            'latency_p95_ms': self.latency_p95_ms,
            'latency_max_ms': self.latency_max_ms,
            'stage_latency_map_ms': self.stage_latency_map_ms,
            'stage_latency_group_ms': self.stage_latency_group_ms,
            'stage_latency_window_ms': self.stage_latency_window_ms,
            'watermark_lag_ms': self.watermark_lag_ms,
            'records_processed': self.records_processed,
            'throughput': self.throughput,
            'output_record_count': self.output_record_count,
            'shuffle_volume': self.shuffle_volume,
            'partition_skew': self.partition_skew,
            'repartition_effect': self.repartition_effect,
            'failures': self.failures,
            'retries': self.retries,
            'memory_utilization_avg': self.memory_utilization_avg,
            'cpu_utilization_avg': self.cpu_utilization_avg
        } 