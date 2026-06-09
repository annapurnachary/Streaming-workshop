from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def run_hourly_revenue():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1) 
    
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, settings)

    # 1. Source DDL: Include total_amount and 5-second Watermark
    t_env.execute_sql(f"""
        CREATE TABLE green_trips (
            PULocationID INT,
            total_amount DOUBLE,
            lpep_pickup_datetime STRING,
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'green-trips',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'properties.group.id' = 'q6-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """)

    # 2. Sink DDL (Print Sink)
    t_env.execute_sql("""
        CREATE TABLE print_sink (
            window_start TIMESTAMP(3),
            PULocationID INT,
            total_revenue DOUBLE
        ) WITH (
            'connector' = 'print'
        )
    """)

    # 3. Process: 1-hour Tumbling Window for Revenue
    t_env.execute_sql("""
        INSERT INTO print_sink
        SELECT 
            TUMBLE_START(event_timestamp, INTERVAL '1' HOUR) AS window_start,
            PULocationID,
            SUM(total_amount) AS total_revenue
        FROM green_trips
        WHERE PULocationID IS NOT NULL
        GROUP BY 
            TUMBLE(event_timestamp, INTERVAL '1' HOUR),
            PULocationID
    """).wait()

if __name__ == "__main__":
    run_hourly_revenue()
