from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import logging

def test_timescale_connection(**context):
    """Test TimescaleDB connection and create a simple table"""
    logger = logging.getLogger(__name__)
    
    try:
        # Connect to TimescaleDB
        postgres_hook = PostgresHook(postgres_conn_id='timescale')
        
        logger.info("Testing TimescaleDB connection...")
        
        # Test basic connection
        result = postgres_hook.get_first("SELECT version();")
        logger.info(f"PostgreSQL version: {result}")
        
        # Create a simple test table
        test_table_sql = """
        DROP TABLE IF EXISTS test_anime_events;
        
        CREATE TABLE test_anime_events (
            id SERIAL PRIMARY KEY,
            event_timestamp TIMESTAMPTZ NOT NULL,
            source_table VARCHAR(100) NOT NULL,
            operation VARCHAR(20) NOT NULL,
            test_data JSONB,
            processed_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
        
        postgres_hook.run(test_table_sql)
        logger.info("✅ Test table created successfully")
        
        # Insert test data
        insert_sql = """
        INSERT INTO test_anime_events (event_timestamp, source_table, operation, test_data)
        VALUES (NOW(), 'test_table', 'INSERT', '{"test": "data"}');
        """
        
        postgres_hook.run(insert_sql)
        logger.info("✅ Test data inserted successfully")
        
        # Query the data back
        result = postgres_hook.get_first("SELECT * FROM test_anime_events;")
        logger.info(f"✅ Test data retrieved: {result}")
        
        # Check if TimescaleDB extension is available
        extension_check = postgres_hook.get_first("SELECT * FROM pg_extension WHERE extname = 'timescaledb';")
        if extension_check:
            logger.info("✅ TimescaleDB extension is installed")
            
            # Try to create hypertable
            try:
                postgres_hook.run("SELECT create_hypertable('test_anime_events', 'event_timestamp', if_not_exists => TRUE);")
                logger.info("✅ Hypertable created successfully")
            except Exception as e:
                logger.warning(f"⚠️ Hypertable creation failed: {e}")
                
        else:
            logger.warning("⚠️ TimescaleDB extension not found - using regular PostgreSQL")
        
        return "TimescaleDB connection test successful"
        
    except Exception as e:
        logger.error(f"❌ TimescaleDB connection failed: {str(e)}")
        raise

with DAG(
    dag_id="test_timescale_connection",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test", "timescale"]
) as dag:
    
    test_connection = PythonOperator(
        task_id="test_timescale_connection",
        python_callable=test_timescale_connection
    )