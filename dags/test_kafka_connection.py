from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.hooks.client import KafkaAdminClientHook
from datetime import datetime
import logging

def test_kafka_connection(**context):
    """Test Kafka connection and list available topics"""
    logger = logging.getLogger(__name__)
    
    try:
        # Test connection using Kafka hook
        kafka_hook = KafkaAdminClientHook(kafka_config_id='kafka_default')
        
        logger.info("Attempting to connect to Kafka/Redpanda...")
        
        # Get admin client and list topics
        admin_client = kafka_hook.get_conn()
        metadata = admin_client.list_topics(timeout=10)
        
        logger.info("✅ Successfully connected to Kafka/Redpanda!")
        logger.info(f"Available topics: {list(metadata.topics.keys())}")
        
        # Look for anime-db topics specifically
        anime_topics = [topic for topic in metadata.topics.keys() if 'anime-db' in topic]
        logger.info(f"Anime-db topics found: {anime_topics}")
        
        return {
            "connection_status": "success",
            "topics_count": len(metadata.topics),
            "anime_topics": anime_topics
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to connect to Kafka: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        raise

with DAG(
    dag_id="test_kafka_connection",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test", "kafka"]
) as dag:
    
    test_connection = PythonOperator(
        task_id="test_kafka_connection",
        python_callable=test_kafka_connection
    )