from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.descriptors import Schema, ConnectorDescriptor, OldCsv, FileSystem

env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)


t_env.execute_sql("""
    CREATE TABLE numbers (
        value INT
    ) WITH (
        'connector' = 'filesystem',
        'path' = 'file:///tmp/mock_data.txt',
        'format' = 'csv'
    )
""")

t_env.execute_sql("""
    CREATE TABLE counts (
        cnt BIGINT
    ) WITH (
        'connector' = 'print'
    )
""")

t_env.execute_sql("""
    INSERT INTO counts
    SELECT COUNT(*) FROM numbers
""")
