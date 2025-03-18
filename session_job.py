from pyflink.datastream import StreamExecutionEnvironment 
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def create_longest_sessions_sink(t_env):
    table_name = 'longest_sessions'
    sink_ddl = f"""
    CREATE TABLE {table_name} (
        pu_location_id INT,
        do_location_id INT,
        session_start TIMESTAMP(3),
        session_end TIMESTAMP(3),
        trip_count BIGINT,
        PRIMARY KEY (pu_location_id, do_location_id) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgres:5432/postgres',
        'table-name' = '{table_name}',
        'username' = 'postgres',
        'password' = 'postgres',
        'driver' = 'org.postgresql.Driver'
    )
    """
    t_env.execute_sql(sink_ddl)
    return table_name

def create_green_trips_source_kafka(t_env):
    table_name = "green_trips"
    source_ddl = f"""
    CREATE TABLE {table_name} (
        lpep_pickup_datetime STRING,
        lpep_dropoff_datetime STRING,
        PULocationID INT,
        DOLocationID INT,
        passenger_count INT,
        trip_distance DOUBLE,
        tip_amount DOUBLE,
        event_time AS TO_TIMESTAMP(lpep_dropoff_datetime),
        WATERMARK FOR event_time AS event_time - INTERVAL '5' SECONDS
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'green-trips',
        'properties.bootstrap.servers' = 'redpanda-1:29092',
        'scan.startup.mode' = 'earliest-offset',
        'properties.auto.offset.reset' = 'earliest',
        'format' = 'json',
        'json.ignore-parse-errors' = 'true'
    )
    """
    t_env.execute_sql(source_ddl)
    return table_name

def session_job():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    # Enable checkpointing (with default timeout/failure thresholds)
    env.enable_checkpointing(30 * 1000)  # 30-second interval
    
    # The following calls aren't available in your version of Flink/PyFlink:
    # env.get_checkpoint_config().setCheckpointTimeout(60000)
    # env.get_checkpoint_config().setTolerableCheckpointFailureNumber(10)
    
    env.set_parallelism(1)  # for testing
    
    # Set up the table environment in streaming mode
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    
    try:
        # Create Kafka source table
        create_green_trips_source_kafka(t_env)
        
        # Create sink table
        longest_sessions_table = create_longest_sessions_sink(t_env)
        
        # Create a temporary view with session window aggregation
        session_view = """
        SELECT 
            PULocationID as pu_location_id,
            DOLocationID as do_location_id,
            SESSION_START(event_time, INTERVAL '5' MINUTES) as session_start,
            SESSION_END(event_time, INTERVAL '5' MINUTES) as session_end,
            COUNT(*) AS trip_count
        FROM green_trips
        GROUP BY 
            PULocationID, 
            DOLocationID,
            SESSION(event_time, INTERVAL '5' MINUTES)
        """
        t_env.execute_sql("CREATE TEMPORARY VIEW session_results AS " + session_view)
        
        # Insert top session by trip_count into the sink
        top_session_query = """
        INSERT INTO longest_sessions
        SELECT pu_location_id, do_location_id, session_start, session_end, trip_count
        FROM (
            SELECT 
                pu_location_id,
                do_location_id,
                session_start,
                session_end,
                trip_count,
                ROW_NUMBER() OVER (ORDER BY trip_count DESC) AS rn
            FROM session_results
        )
        WHERE rn = 1
        """
        t_env.execute_sql(top_session_query).wait()
        
        print("Session job completed successfully!")
        
    except Exception as e:
        print("Session job failed:", str(e))

if __name__ == '__main__':
    session_job()

