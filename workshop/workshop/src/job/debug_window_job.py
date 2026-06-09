from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def run_debug_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1) 
    
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, settings)

    # 1. Source DDL (Same as before)
    t_env.execute_sql(f"""
        CREATE TABLE green_trips (
            PULocationID INT,
            lpep_pickup_datetime STRING,
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'green-trips',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'properties.group.id' = 'debug-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """)

    # 2. Sink DDL (Using 'print' instead of 'jdbc')
    t_env.execute_sql("""
        CREATE TABLE print_sink (
            window_start TIMESTAMP(3),
            PULocationID INT,
            num_trips BIGINT
        ) WITH (
            'connector' = 'print'
        )
    """)

    # 3. Process with NULL filter
    t_env.execute_sql("""
        INSERT INTO print_sink
        SELECT 
            TUMBLE_START(event_timestamp, INTERVAL '5' MINUTE) AS window_start,
            PULocationID,
            COUNT(*) AS num_trips
        FROM green_trips
        WHERE PULocationID IS NOT NULL
        GROUP BY 
            TUMBLE(event_timestamp, INTERVAL '5' MINUTE),
            PULocationID
    """).wait() # This will keep the terminal open until processed

if __name__ == "__main__":
    run_debug_job()
