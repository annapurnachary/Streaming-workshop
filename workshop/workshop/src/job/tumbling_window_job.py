from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def create_green_trips_source_kafka(t_env):
    table_name = "green_trips"
    # IMPORTANT: Homework uses lpep_prefix and 5-minute windows
    # We use VARCHAR for the datetime to match the 'string timestamp' requirement
    source_ddl = f"""
        CREATE TABLE {table_name} (
            pulocationid INT,
            total_amount DOUBLE,
            lpep_pickup_datetime VARCHAR,
            -- Convert string to timestamp
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'green-trips',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'properties.group.id' = 'flink-homework-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name

def create_processed_sink_postgres(t_env):
    table_name = 'trip_counts_by_window'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            window_start TIMESTAMP(3),
            pulocationid INT,
            num_trips BIGINT,
            PRIMARY KEY (window_start, pulocationid) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name

def log_aggregation():
    env = StreamExecutionEnvironment.get_execution_environment()
    # REQUIRED for homework: 1 partition = parallelism 1
    env.set_parallelism(1)
    # Checkpoint every 10 seconds to force data flush to Postgres
    env.enable_checkpointing(10000) 


    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Updated to match your container's /opt/flink/lib/
    kafka_jar = "file:///opt/flink/lib/flink-sql-connector-kafka-4.0.1-2.0.jar"
    jdbc_core_jar = "file:///opt/flink/lib/flink-connector-jdbc-core-4.0.0-2.0.jar"
    jdbc_postgres_jar = "file:///opt/flink/lib/flink-connector-jdbc-postgres-4.0.0-2.0.jar"
    postgres_driver_jar = "file:///opt/flink/lib/postgresql-42.7.10.jar"
    # Add JARs (ensure these filenames match your /opt/flink/lib/ content)
    #t_env.get_config().set("pipeline.jars", "file:///opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar;file:///opt/flink/lib/flink-connector-jdbc-3.1.2-1.18.jar")
    t_env.get_config().set(
    "pipeline.jars", 
    f"{kafka_jar};{jdbc_core_jar};{jdbc_postgres_jar};{postgres_driver_jar}"
)
    try:
        source_table = create_green_trips_source_kafka(t_env)
        sink_table = create_processed_sink_postgres(t_env)

        # 5-minute Tumbling Window as per Question 4
        t_env.execute_sql(f"""
        INSERT INTO {sink_table}
        SELECT
            window_start,
            pulocationid,
            COUNT(*) AS num_trips
        FROM TABLE(
            TUMBLE(TABLE {source_table}, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTES)
        )
        WHERE pulocationid IS NOT NULL
        GROUP BY window_start, pulocationid;
        """).wait()

    except Exception as e:
        print("Flink Job Failed:", str(e))

if __name__ == '__main__':
    log_aggregation()
