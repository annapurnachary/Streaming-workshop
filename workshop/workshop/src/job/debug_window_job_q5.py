from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def run_q5_debug_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1) 
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, settings)

    # Source DDL (Ensuring total_amount is included)
    t_env.execute_sql(f"""
        CREATE TABLE green_trips (
         pulocationid INT,
         lpep_pickup_datetime STRING,
         event_timestamp AS CAST(lpep_pickup_datetime AS TIMESTAMP(3)),
         WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
     ) WITH (
    'connector' = 'kafka',
    'topic' = 'green-trips',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'session-homework-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
    )
    """)

    # Sink DDL (Printing results to TaskManager logs)
    t_env.execute_sql("""
        CREATE TABLE print_sink (
            window_start TIMESTAMP(3),
            PULocationID INT,
            num_trips BIGINT,
            total_revenue DOUBLE
        ) WITH (
            'connector' = 'print'
        )
    """)

    # Process: 5-minute Tumbling Window calculating Sum of Revenue
    t_env.execute_sql("""
        INSERT INTO print_sink
        SELECT 
            TUMBLE_START(event_timestamp, INTERVAL '5' MINUTE) AS window_start,
            PULocationID,
            COUNT(*) AS num_trips,
            SUM(total_amount) AS total_revenue
        FROM green_trips
        WHERE PULocationID IS NOT NULL
        GROUP BY 
            TUMBLE(event_timestamp, INTERVAL '5' MINUTE),
            PULocationID
    """).wait()

if __name__ == "__main__":
    run_q5_debug_job()
