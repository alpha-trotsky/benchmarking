#!/bin/bash

# Create metrics directories in both apps
mkdir -p flink-app/metrics
mkdir -p spark-app/metrics

# Copy metrics files to Flink app
cp metrics/__init__.py flink-app/metrics/
cp metrics/metrics_schema.py flink-app/metrics/
cp metrics/metrics_collector.py flink-app/metrics/
cp metrics/requirements.txt flink-app/metrics/

# Copy metrics files to Spark app
cp metrics/__init__.py spark-app/metrics/
cp metrics/metrics_schema.py spark-app/metrics/
cp metrics/metrics_collector.py spark-app/metrics/
cp metrics/requirements.txt spark-app/metrics/
cp metrics/spark-metrics.properties spark-app/metrics/ 