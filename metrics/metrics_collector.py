import csv
import os
from datetime import datetime
from typing import Dict, List
import psutil
import statistics
from .metrics_schema import BenchmarkMetrics

class MetricsCollector:
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        self.metrics_file = os.path.join(output_dir, 'benchmark_metrics.csv')
        self._ensure_output_dir()
        self._initialize_csv()

    def _ensure_output_dir(self):
        """Ensure the output directory exists"""
        os.makedirs(self.output_dir, exist_ok=True)

    def _initialize_csv(self):
        """Initialize CSV file with headers if it doesn't exist"""
        if not os.path.exists(self.metrics_file):
            with open(self.metrics_file, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=BenchmarkMetrics.__annotations__.keys())
                writer.writeheader()

    def collect_system_metrics(self) -> Dict:
        """Collect current system metrics"""
        process = psutil.Process()
        return {
            'memory_utilization_avg': process.memory_percent(),
            'cpu_utilization_avg': process.cpu_percent()
        }

    def calculate_latency_metrics(self, latencies: List[float]) -> Dict:
        """Calculate latency statistics from a list of latency measurements"""
        if not latencies:
            return {
                'latency_avg_ms': 0,
                'latency_p95_ms': 0,
                'latency_max_ms': 0
            }
        
        return {
            'latency_avg_ms': statistics.mean(latencies),
            'latency_p95_ms': statistics.quantiles(latencies, n=20)[18],  # 95th percentile
            'latency_max_ms': max(latencies)
        }

    def calculate_partition_skew(self, partition_sizes: List[int]) -> float:
        """Calculate partition skew (0-1 ratio)"""
        if not partition_sizes:
            return 0.0
        avg_size = sum(partition_sizes) / len(partition_sizes)
        max_deviation = max(abs(size - avg_size) for size in partition_sizes)
        return max_deviation / avg_size if avg_size > 0 else 0.0

    def save_metrics(self, metrics: BenchmarkMetrics):
        """Save metrics to CSV file"""
        with open(self.metrics_file, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=BenchmarkMetrics.__annotations__.keys())
            writer.writerow(metrics.to_dict())

    def collect_and_save_metrics(self, 
                               run_id: str,
                               engine: str,
                               input_config: Dict,
                               query_config: Dict,
                               partitioning_config: Dict,
                               infrastructure_config: Dict,
                               performance_metrics: Dict,
                               throughput_metrics: Dict,
                               resource_metrics: Dict,
                               notes: str = ""):
        """Collect and save all metrics in one go"""
        
        # Collect system metrics
        system_metrics = self.collect_system_metrics()
        
        # Create metrics object
        metrics = BenchmarkMetrics(
            run_id=run_id,
            engine=engine,
            timestamp=datetime.now(),
            notes=notes,
            **input_config,
            **query_config,
            **partitioning_config,
            **infrastructure_config,
            **performance_metrics,
            **throughput_metrics,
            **resource_metrics,
            **system_metrics
        )
        
        # Save metrics
        self.save_metrics(metrics) 