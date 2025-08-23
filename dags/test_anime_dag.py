from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging

def test_anime_dag_import():
    """Test that the anime_kafka_dag can be imported without errors"""
    logger = logging.getLogger(__name__)
    
    try:
        # Try importing the anime kafka DAG
        import sys
        import os
        
        # Add the dags directory to path
        dags_dir = '/opt/airflow/dags/repo/dags'
        if dags_dir not in sys.path:
            sys.path.append(dags_dir)
        
        # Import the DAG module
        import anime_kafka_dag
        
        logger.info("✅ Successfully imported anime_kafka_dag")
        logger.info(f"DAG found: {anime_kafka_dag.dag.dag_id}")
        logger.info(f"Tasks: {[task.task_id for task in anime_kafka_dag.dag.tasks]}")
        
        return {
            "status": "success",
            "dag_id": anime_kafka_dag.dag.dag_id,
            "task_count": len(anime_kafka_dag.dag.tasks)
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to import anime_kafka_dag: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        raise

with DAG(
    dag_id="test_anime_dag_import",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test", "anime"]
) as dag:
    
    test_import = PythonOperator(
        task_id="test_anime_dag_import",
        python_callable=test_anime_dag_import
    )