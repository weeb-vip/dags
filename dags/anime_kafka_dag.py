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
        max_messages = 5  # Process just a few messages
        
        logger.info(f"Polling for up to {max_messages} messages...")
        
        for i in range(max_messages):
            msg = consumer.poll(timeout=10.0)
            if msg is None:
                logger.info("No more messages available")
                break
                
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
                
            # Process the message using our existing function
            processed_event = process_anime_event(msg, **context)
            processed_events.append(processed_event)
            
            logger.info(f"Processed message {i+1}/{max_messages} from topic {msg.topic()}")
        
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
        
        -- Create hypertable for time-series data if not already created
        SELECT create_hypertable('anime_events', 'event_timestamp', if_not_exists => TRUE);
        """
        
        postgres_hook.run(create_table_sql)
        
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
            
            logger.info(f"Inserting {operation} event for table {table_name} into TimescaleDB")
            
            # Insert the event data (Debezium format)
            insert_sql = """
            INSERT INTO anime_events (event_timestamp, source_table, operation, topic, before_data, after_data, full_event)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
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