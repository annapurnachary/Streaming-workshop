from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def run_session_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1) 
    env.enable_checkpointing(1000)
    
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # 1. Source DDL: 5-second Watermark tolerance
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

    # 2. Sink DDL (Print Sink)
    t_env.execute_sql("""
    CREATE TABLE session_results (
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        pulocationid INT,
        num_trips BIGINT,
        PRIMARY KEY (pulocationid, window_start) NOT ENFORCED -- ADD THIS LINE
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgres:5432/postgres',
        'table-name' = 'pulocation_sessions', -- MUST match Postgres table name
        'username' = 'postgres',
        'password' = 'postgres',
        'driver' = 'org.postgresql.Driver'
    )
""")

    # 3. Process: Session Window with 5-minute gap
    t_env.execute_sql("""
    INSERT INTO session_results
    SELECT 
        window_start, 
        window_end, 
        pulocationid, 
        COUNT(*) AS num_trips
    FROM TABLE(
        SESSION(
            DATA => TABLE green_trips, 
            TIMECOL => DESCRIPTOR(event_timestamp), 
            KEY => DESCRIPTOR(pulocationid), 
            GAP => INTERVAL '5' MINUTES
        )
    )
    GROUP BY window_start, window_end, pulocationid
""").wait()

if __name__ == "__main__":
    run_session_job()
