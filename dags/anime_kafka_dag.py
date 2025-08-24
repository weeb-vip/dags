from airflow import DAG
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageSensor
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import json
import logging

def accept_any_message(message, **kwargs):
    """Apply function for AwaitMessageSensor - accepts any non-null message"""
    return message is not None

def consume_and_process_kafka_events(**context):
    """Custom Kafka consumer that processes messages and returns data via XCom"""
    from confluent_kafka import Consumer
    
    logger = logging.getLogger(__name__)
    
    try:
        # Get connection details
        connection = BaseHook.get_connection('kafka_default')
        config = connection.extra_dejson.copy()
        config.update({
            'group.id': f'anime-consumer-airflow-{datetime.now().strftime("%Y%m%d-%H%M%S")}',  # Unique group to read from beginning
            'enable.auto.commit': False,
            'auto.offset.reset': 'earliest'  # Read from beginning
        })
        
        logger.info(f"Creating Kafka consumer with config: {config}")
        
        # Create consumer
        consumer = Consumer(config)
        
        # Subscribe to topics
        topics = ["anime-db.public.anime", "anime-db.public.anime_character", 
                 "anime-db.public.anime_character_staff_link", "anime-db.public.anime_episodes", 
                 "anime-db.public.anime_staff"]
        consumer.subscribe(topics)
        
        processed_events = []
        timeout_minutes = 30  # Run for up to 30 minutes
        batch_size = 100  # Process in batches for efficiency
        no_message_timeout = 60.0  # Wait 60 seconds for new messages
        
        start_time = datetime.now()
        timeout_delta = timedelta(minutes=timeout_minutes)
        consecutive_empty_polls = 0
        max_empty_polls = 5  # Stop after 5 consecutive empty polls
        
        logger.info(f"Starting continuous consumption for up to {timeout_minutes} minutes...")
        
        while datetime.now() - start_time < timeout_delta:
            msg = consumer.poll(timeout=no_message_timeout)
            
            if msg is None:
                consecutive_empty_polls += 1
                logger.info(f"No message received (attempt {consecutive_empty_polls}/{max_empty_polls})")
                
                if consecutive_empty_polls >= max_empty_polls:
                    logger.info("No new messages after multiple attempts, stopping consumption")
                    break
                continue
            
            consecutive_empty_polls = 0  # Reset counter when message received
            
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
                
            # Process the message
            processed_event = process_anime_event(msg, **context)
            processed_events.append(processed_event)
            
            logger.info(f"Processed message from topic {msg.topic()}, total processed: {len(processed_events)}")
            
            # Commit offset periodically and process in batches
            if len(processed_events) % batch_size == 0:
                consumer.commit()
                logger.info(f"Committed offset after {len(processed_events)} messages")
        
        # Final commit
        consumer.commit()
        consumer.close()
        
        logger.info(f"Successfully processed {len(processed_events)} events")
        
        # Return the processed events for XCom
        return processed_events
        
    except Exception as e:
        logger.error(f"Error in custom Kafka consumer: {str(e)}")
        raise

def process_anime_event(message, **context):
    """Process incoming anime database events"""
    logger = logging.getLogger(__name__)
    
    try:
        # Extract the actual message value from confluent_kafka Message object
        if hasattr(message, 'value'):
            message_bytes = message.value()
            topic = message.topic()
            logger.info(f"Received message from topic: {topic}")
        else:
            message_bytes = message
            topic = 'unknown'
        
        # Parse the message bytes
        if isinstance(message_bytes, bytes):
            message_str = message_bytes.decode('utf-8')
        else:
            message_str = str(message_bytes)
            
        # Log the received event
        logger.info(f"Raw message content: {message_str[:200]}...")  # Log first 200 chars
        
        # Parse JSON event data (Debezium format)
        if message_str.startswith('{'):
            event_data = json.loads(message_str)
        else:
            event_data = {"raw_message": message_str}
        
        # Handle Debezium message structure
        if 'payload' in event_data:
            payload = event_data['payload']
            # Extract table name from Debezium source info
            source_info = payload.get('source', {})
            table_name = source_info.get('table', topic.split('.')[-1] if '.' in topic else 'unknown')
            
            # Map Debezium operation codes to readable names
            op_code = payload.get('op', 'unknown')
            operation_map = {'c': 'INSERT', 'u': 'UPDATE', 'd': 'DELETE', 'r': 'READ'}
            operation = operation_map.get(op_code, op_code)
            
            # Get the actual data (before/after states)
            before_data = payload.get('before')
            after_data = payload.get('after')
            
            logger.info(f"Debezium event - Table: {table_name}, Operation: {operation}")
            
        else:
            # Fallback for non-Debezium messages
            table_name = event_data.get('table', topic.split('.')[-1] if '.' in topic else 'unknown')
            operation = event_data.get('op', event_data.get('operation', 'unknown'))
            before_data = None
            after_data = event_data
        
        logger.info(f"Processing {operation} event for table: {table_name}")
        
        return {
            "processed": True, 
            "table": table_name,
            "operation": operation,
            "before_data": before_data,
            "after_data": after_data,
            "event_data": event_data,
            "topic": topic,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error processing anime event: {str(e)}")
        logger.error(f"Message type: {type(message)}")
        logger.error(f"Message dir: {dir(message)}")
        raise

def insert_to_timescale(**context):
    """Insert processed event data into TimescaleDB"""
    logger = logging.getLogger(__name__)
    
    # Get the processed event data from previous task
    processed_data = context['ti'].xcom_pull(task_ids='consume_anime_events')
    
    if not processed_data:
        logger.warning("No processed data found to insert")
        return "No data to insert"
    
    if isinstance(processed_data, list):
        logger.info(f"Processing {len(processed_data)} events")
    else:
        logger.info("Processing single event")
        processed_data = [processed_data]
    
    try:
        # Connect to TimescaleDB using the 'timescale' connection
        postgres_hook = PostgresHook(postgres_conn_id='timescale')
        
        # Create events table if not exists (optimized for Debezium)
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS anime_events (
            id SERIAL PRIMARY KEY,
            event_timestamp TIMESTAMPTZ NOT NULL,
            source_table VARCHAR(100) NOT NULL,
            operation VARCHAR(20) NOT NULL,
            topic VARCHAR(200),
            before_data JSONB,
            after_data JSONB,
            full_event JSONB,
            processed_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
        
        postgres_hook.run(create_table_sql)
        
        # Create hypertable separately - only if TimescaleDB extension is available
        try:
            extension_check = postgres_hook.get_first("SELECT * FROM pg_extension WHERE extname = 'timescaledb';")
            if extension_check:
                postgres_hook.run("SELECT create_hypertable('anime_events', 'event_timestamp', if_not_exists => TRUE);")
                logger.info("✅ Hypertable created successfully")
            else:
                logger.info("TimescaleDB extension not available, using regular PostgreSQL table")
        except Exception as e:
            logger.warning(f"Hypertable creation failed, continuing with regular table: {e}")
        
        inserted_count = 0
        
        # Insert each event
        for event in processed_data:
            table_name = event.get('table', 'unknown')
            operation = event.get('operation', 'unknown')
            event_timestamp = event.get('timestamp')
            before_data = event.get('before_data')
            after_data = event.get('after_data')
            topic = event.get('topic')
            full_event = event.get('event_data')
            
            # Convert ISO timestamp string to datetime object
            if isinstance(event_timestamp, str):
                try:
                    from datetime import datetime
                    event_timestamp = datetime.fromisoformat(event_timestamp.replace('Z', '+00:00'))
                except ValueError:
                    event_timestamp = datetime.now()
            
            logger.info(f"Inserting {operation} event for table {table_name} into TimescaleDB")
            
            # Insert the event data - let PostgreSQL handle JSONB conversion
            insert_sql = """
            INSERT INTO anime_events (event_timestamp, source_table, operation, topic, before_data, after_data, full_event)
            VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb)
            """
            
            postgres_hook.run(
                insert_sql,
                parameters=(
                    event_timestamp,
                    table_name,
                    operation,
                    topic,
                    json.dumps(before_data) if before_data else None,
                    json.dumps(after_data) if after_data else None,
                    json.dumps(full_event)
                )
            )
            
            inserted_count += 1
            logger.info(f"Successfully inserted event {inserted_count} for table {table_name}")
        
        logger.info(f"Successfully inserted {inserted_count} events into TimescaleDB")
        return f"Inserted {inserted_count} events into TimescaleDB"
        
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
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=35)  # Allow for 30min consumption + buffer
}

with DAG(
    dag_id="anime_kafka_consumer",
    default_args=default_args,
    description="DAG to continuously consume Kafka events and store in TimescaleDB",
    schedule=None,  # Manual trigger for continuous consumption
    catchup=False,
    max_active_runs=1,  # Prevent multiple instances running simultaneously
    tags=["kafka", "anime", "events", "timescale", "continuous"]
) as dag:
    
    # Sensor to wait for messages on anime-db topics
    wait_for_anime_events = AwaitMessageSensor(
        task_id="wait_for_anime_events",
        kafka_config_id="kafka_default",  # Connection ID in Airflow
        topics=["anime-db.public.anime", "anime-db.public.anime_character", "anime-db.public.anime_character_staff_link", "anime-db.public.anime_episodes", "anime-db.public.anime_staff"],  # Actual Redpanda topics
        apply_function="anime_kafka_dag.accept_any_message",  # Accept any non-null message (string reference)
        poll_timeout=30,  # Time to wait for Kafka message
        poll_interval=5   # Sleep time after reaching log end
    )
    
    # Custom consumer operator that guarantees XCom data return
    consume_anime_events = PythonOperator(
        task_id="consume_anime_events",
        python_callable=consume_and_process_kafka_events,
        provide_context=True
    )
    
    # Insert data into TimescaleDB
    insert_to_timescale_task = PythonOperator(
        task_id="insert_to_timescale",
        python_callable=insert_to_timescale,
        provide_context=True
    )
    
    # Define task dependencies
    wait_for_anime_events >> consume_anime_events >> insert_to_timescale_task