from airflow import DAG
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageSensor
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import json
import logging

def accept_any_message(message):
    """Apply function for AwaitMessageSensor - accepts any non-null message"""
    return message is not None

def process_anime_event(message, **context):
    """Process incoming anime database events"""
    logger = logging.getLogger(__name__)
    
    try:
        # Parse the message
        if isinstance(message, bytes):
            message_str = message.decode('utf-8')
        else:
            message_str = str(message)
            
        # Log the received event
        logger.info(f"Received anime-db event: {message_str}")
        
        # Parse JSON event data
        event_data = json.loads(message_str) if message_str.startswith('{') else {"raw_message": message_str}
        
        # Extract table name from topic or event data
        table_name = event_data.get('table', 'unknown')
        operation = event_data.get('op', 'unknown')  # INSERT, UPDATE, DELETE
        
        logger.info(f"Processing {operation} event for table: {table_name}")
        
        return {
            "processed": True, 
            "table": table_name,
            "operation": operation,
            "event_data": event_data,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error processing anime event: {str(e)}")
        raise

def insert_to_timescale(**context):
    """Insert processed event data into TimescaleDB"""
    logger = logging.getLogger(__name__)
    
    # Get the processed event data from previous task
    processed_data = context['ti'].xcom_pull(task_ids='consume_anime_events')
    
    if not processed_data:
        logger.warning("No processed data found to insert")
        return "No data to insert"
    
    try:
        # Connect to TimescaleDB using the 'timescale' connection
        postgres_hook = PostgresHook(postgres_conn_id='timescale')
        
        table_name = processed_data.get('table', 'unknown')
        operation = processed_data.get('operation', 'unknown')
        event_data = processed_data.get('event_data', {})
        event_timestamp = processed_data.get('timestamp')
        
        logger.info(f"Inserting {operation} event for table {table_name} into TimescaleDB")
        
        # Create events table if not exists (adjust schema as needed)
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS anime_events (
            id SERIAL PRIMARY KEY,
            event_timestamp TIMESTAMPTZ NOT NULL,
            source_table VARCHAR(100) NOT NULL,
            operation VARCHAR(20) NOT NULL,
            event_data JSONB,
            processed_at TIMESTAMPTZ DEFAULT NOW()
        );
        
        -- Create hypertable for time-series data if not already created
        SELECT create_hypertable('anime_events', 'event_timestamp', if_not_exists => TRUE);
        """
        
        postgres_hook.run(create_table_sql)
        
        # Insert the event data
        insert_sql = """
        INSERT INTO anime_events (event_timestamp, source_table, operation, event_data)
        VALUES (%s, %s, %s, %s)
        """
        
        postgres_hook.run(
            insert_sql,
            parameters=(
                event_timestamp,
                table_name,
                operation,
                json.dumps(event_data)
            )
        )
        
        logger.info(f"Successfully inserted event for table {table_name} into TimescaleDB")
        return f"Inserted {operation} event for {table_name}"
        
    except Exception as e:
        logger.error(f"Error inserting to TimescaleDB: {str(e)}")
        raise

# Default arguments for the DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id="anime_kafka_consumer",
    default_args=default_args,
    description="DAG to consume Kafka events from anime-db.public.<table> topics and store in TimescaleDB",
    schedule=None,  # Event-driven, no schedule
    catchup=False,
    tags=["kafka", "anime", "events", "timescale"]
) as dag:
    
    # Sensor to wait for messages on anime-db topics
    wait_for_anime_events = AwaitMessageSensor(
        task_id="wait_for_anime_events",
        kafka_config_id="kafka_default",  # Connection ID in Airflow
        topics=["anime-db.public.anime", "anime-db.public.anime_character", "anime-db.public.anime_character_staff_link", "anime-db.public.anime_episodes", "anime-db.public.anime_staff"],  # Actual Redpanda topics
        apply_function=accept_any_message,  # Accept any non-null message
        poll_timeout=30,  # Time to wait for Kafka message
        poll_interval=5   # Sleep time after reaching log end
    )
    
    # Consumer operator to process the events
    consume_anime_events = ConsumeFromTopicOperator(
        task_id="consume_anime_events",
        kafka_config_id="kafka_default",
        topics=["anime-db.public.anime", "anime-db.public.anime_character", "anime-db.public.anime_character_staff_link", "anime-db.public.anime_episodes", "anime-db.public.anime_staff"],  # Actual Redpanda topics
        apply_function=process_anime_event,
        max_messages=100,  # Process up to 100 messages per run
        commit_cadence="end_of_operator"  # Commit offsets at the end
    )
    
    # Insert data into TimescaleDB
    insert_to_timescale_task = PythonOperator(
        task_id="insert_to_timescale",
        python_callable=insert_to_timescale,
        provide_context=True
    )
    
    # Define task dependencies
    wait_for_anime_events >> consume_anime_events >> insert_to_timescale_task