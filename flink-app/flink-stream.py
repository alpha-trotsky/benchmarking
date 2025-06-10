from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes

env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)


t_env.execute_sql("""
    CREATE TABLE numbers (
        event_time TIMESTAMP(3),
        value INT
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'mock-data-stream',
        'properties.bootstrap.servers' = 'kafka:9092',
        'properties.group.id' = 'flink-consumer',
        'format' = 'csv',
        'scan.startup.mode' = 'latest-offset'
    )
""")

t_env.execute_sql("""
    CREATE TABLE flink_csv_output (
        event_time TIMESTAMP(3),
        value INT,
        processing_time TIMESTAMP(3),
        latency_seconds BIGINT
    ) WITH (
        'connector' = 'filesystem',
        'path' = 'file:///app/metrics/flink_metrics.csv',
        'format' = 'csv'
    )
""")

t_env.execute_sql("""
    INSERT INTO flink_csv_output
    SELECT
        event_time,
        value,
        CURRENT_TIMESTAMP as processing_time,
        UNIX_TIMESTAMP(CURRENT_TIMESTAMP) - UNIX_TIMESTAMP(event_time) as latency_seconds
    FROM numbers
""")
