import os
import csv

def summarize_metrics():
    spark_metrics_path = "./spark-app/metrics/spark_metrics.csv"
    flink_metrics_path = "./flink-app/metrics/flink_metrics.csv"

    spark_latencies = []
    flink_latencies = []

    # Read Spark metrics
    if os.path.exists(spark_metrics_path):
        with open(spark_metrics_path, mode='r') as infile:
            reader = csv.reader(infile)
            # Assuming the CSV has a header and latency is in the 3rd column (index 2)
            next(reader) # Skip header
            for rows in reader:
                try:
                    latency = int(rows[2])
                    spark_latencies.append(latency)
                except (IndexError, ValueError):
                    pass # Skip lines that don't have the expected format

    # Read Flink metrics
    if os.path.exists(flink_metrics_path):
        with open(flink_metrics_path, mode='r') as infile:
            reader = csv.reader(infile)
            # Assuming the CSV has a header and latency is in the 4th column (index 3)
            next(reader) # Skip header
            for rows in reader:
                try:
                    latency = int(rows[3])
                    flink_latencies.append(latency)
                except (IndexError, ValueError):
                    pass # Skip lines that don't have the expected format

    # Summarize Spark metrics
    spark_summary = {}
    if spark_latencies:
        spark_summary["min_latency"] = min(spark_latencies)
        spark_summary["max_latency"] = max(spark_latencies)
        spark_summary["avg_latency"] = sum(spark_latencies) / len(spark_latencies)
        spark_summary["total_records"] = len(spark_latencies)

    print("Summary of Spark Metrics:")
    print(spark_summary)

    # Summarize Flink metrics
    flink_summary = {}
    if flink_latencies:
        flink_summary["min_latency"] = min(flink_latencies)
        flink_summary["max_latency"] = max(flink_latencies)
        flink_summary["avg_latency"] = sum(flink_latencies) / len(flink_latencies)
        flink_summary["total_records"] = len(flink_latencies)

    print("\nSummary of Flink Metrics:")
    print(flink_summary)


if __name__ == "__main__":
    summarize_metrics()
