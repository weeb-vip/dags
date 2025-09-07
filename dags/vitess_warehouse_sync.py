from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

def get_last_sync_timestamp(warehouse_hook, table_name):
    """Get the last sync timestamp for a table"""
    try:
        result = warehouse_hook.get_first(
            f"SELECT last_sync_timestamp FROM sync_metadata WHERE table_name = '{table_name}'"
        )
        return result[0] if result else None
    except:
        # Table might not exist yet, create metadata table
        warehouse_hook.run("""
            CREATE TABLE IF NOT EXISTS sync_metadata (
                table_name VARCHAR(100) PRIMARY KEY,
                last_sync_timestamp TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        return None

def update_sync_timestamp(warehouse_hook, table_name, timestamp):
    """Update the last sync timestamp for a table"""
    warehouse_hook.run(f"""
        INSERT INTO sync_metadata (table_name, last_sync_timestamp, updated_at)
        VALUES ('{table_name}', '{timestamp}', CURRENT_TIMESTAMP)
        ON CONFLICT (table_name) 
        DO UPDATE SET 
            last_sync_timestamp = EXCLUDED.last_sync_timestamp,
            updated_at = CURRENT_TIMESTAMP
    """)

def sync_users_table():
    vitess_hook = MySqlHook(mysql_conn_id='weeb-readonly')
    warehouse_hook = PostgresHook(postgres_conn_id='warehouse')
    
    logger.info("Starting incremental sync for users table")
    
    # Get last sync timestamp
    last_sync = get_last_sync_timestamp(warehouse_hook, 'users')
    current_sync = datetime.now()
    
    # Build incremental query
    if last_sync:
        users_query = f"SELECT * FROM users WHERE updated_at > '{last_sync}' OR created_at > '{last_sync}'"
        logger.info(f"Incremental sync from {last_sync}")
    else:
        users_query = "SELECT * FROM users"
        logger.info("Full sync - first time")
    
    users_data = vitess_hook.get_records(users_query)
    
    if users_data:
        # Get column names to build dynamic table
        columns_result = vitess_hook.get_records("DESCRIBE users")
        columns = [col[0] for col in columns_result]
        
        # Create table with actual schema
        column_defs = []
        for col in columns_result:
            col_name, col_type = col[0], col[1]
            if 'int' in col_type.lower() and col_name == 'id':
                column_defs.append(f"{col_name} INTEGER PRIMARY KEY")
            elif 'timestamp' in col_type.lower() or 'datetime' in col_type.lower():
                column_defs.append(f"{col_name} TIMESTAMP")
            elif 'varchar' in col_type.lower() or 'text' in col_type.lower():
                column_defs.append(f"{col_name} TEXT")
            else:
                column_defs.append(f"{col_name} TEXT")
        
        column_defs.append("synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        
        warehouse_hook.run(f"""
            CREATE TABLE IF NOT EXISTS users (
                {', '.join(column_defs)}
            )
        """)
        
        # Upsert data
        placeholders = ','.join(['%s'] * len(columns))
        columns_str = ','.join(columns)
        update_cols = ','.join([f"{col} = EXCLUDED.{col}" for col in columns if col != 'id'])
        
        for row in users_data:
            warehouse_hook.run(f"""
                INSERT INTO users ({columns_str}) VALUES ({placeholders})
                ON CONFLICT (id) DO UPDATE SET 
                    {update_cols},
                    synced_at = CURRENT_TIMESTAMP
            """, parameters=row)
        
        logger.info(f"Synced {len(users_data)} records from users table")
        
        # Handle deletions by comparing snapshots (weekly)
        if datetime.now().weekday() == 0:  # Monday
            logger.info("Running weekly deletion detection for users")
            detect_deletions(vitess_hook, warehouse_hook, 'users')
    else:
        logger.info("No new/updated data found in users table")
    
    # Update sync timestamp
    update_sync_timestamp(warehouse_hook, 'users', current_sync)

def sync_user_list_table():
    vitess_hook = MySqlHook(mysql_conn_id='weeb-readonly')
    warehouse_hook = PostgresHook(postgres_conn_id='warehouse')
    
    logger.info("Starting incremental sync for user_list table")
    
    last_sync = get_last_sync_timestamp(warehouse_hook, 'user_list')
    current_sync = datetime.now()
    
    if last_sync:
        user_list_query = f"SELECT * FROM user_list WHERE updated_at > '{last_sync}' OR created_at > '{last_sync}'"
        logger.info(f"Incremental sync from {last_sync}")
    else:
        user_list_query = "SELECT * FROM user_list"
        logger.info("Full sync - first time")
    
    user_list_data = vitess_hook.get_records(user_list_query)
    
    if user_list_data:
        # Get column schema
        columns_result = vitess_hook.get_records("DESCRIBE user_list")
        columns = [col[0] for col in columns_result]
        
        column_defs = []
        for col in columns_result:
            col_name, col_type = col[0], col[1]
            if 'int' in col_type.lower() and col_name == 'id':
                column_defs.append(f"{col_name} INTEGER PRIMARY KEY")
            elif 'timestamp' in col_type.lower() or 'datetime' in col_type.lower():
                column_defs.append(f"{col_name} TIMESTAMP")
            elif 'varchar' in col_type.lower() or 'text' in col_type.lower():
                column_defs.append(f"{col_name} TEXT")
            else:
                column_defs.append(f"{col_name} TEXT")
        
        column_defs.append("synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        
        warehouse_hook.run(f"""
            CREATE TABLE IF NOT EXISTS user_list (
                {', '.join(column_defs)}
            )
        """)
        
        placeholders = ','.join(['%s'] * len(columns))
        columns_str = ','.join(columns)
        update_cols = ','.join([f"{col} = EXCLUDED.{col}" for col in columns if col != 'id'])
        
        for row in user_list_data:
            warehouse_hook.run(f"""
                INSERT INTO user_list ({columns_str}) VALUES ({placeholders})
                ON CONFLICT (id) DO UPDATE SET 
                    {update_cols},
                    synced_at = CURRENT_TIMESTAMP
            """, parameters=row)
        
        logger.info(f"Synced {len(user_list_data)} records from user_list table")
        
        if datetime.now().weekday() == 0:
            detect_deletions(vitess_hook, warehouse_hook, 'user_list')
    else:
        logger.info("No new/updated data found in user_list table")
    
    update_sync_timestamp(warehouse_hook, 'user_list', current_sync)

def sync_user_anime_table():
    vitess_hook = MySqlHook(mysql_conn_id='weeb-readonly')
    warehouse_hook = PostgresHook(postgres_conn_id='warehouse')
    
    logger.info("Starting incremental sync for user_anime table")
    
    last_sync = get_last_sync_timestamp(warehouse_hook, 'user_anime')
    current_sync = datetime.now()
    
    if last_sync:
        user_anime_query = f"SELECT * FROM user_anime WHERE updated_at > '{last_sync}' OR created_at > '{last_sync}'"
        logger.info(f"Incremental sync from {last_sync}")
    else:
        user_anime_query = "SELECT * FROM user_anime"
        logger.info("Full sync - first time")
    
    user_anime_data = vitess_hook.get_records(user_anime_query)
    
    if user_anime_data:
        # Get column schema
        columns_result = vitess_hook.get_records("DESCRIBE user_anime")
        columns = [col[0] for col in columns_result]
        
        column_defs = []
        for col in columns_result:
            col_name, col_type = col[0], col[1]
            if 'int' in col_type.lower() and col_name == 'id':
                column_defs.append(f"{col_name} INTEGER PRIMARY KEY")
            elif 'timestamp' in col_type.lower() or 'datetime' in col_type.lower():
                column_defs.append(f"{col_name} TIMESTAMP")
            elif 'varchar' in col_type.lower() or 'text' in col_type.lower():
                column_defs.append(f"{col_name} TEXT")
            else:
                column_defs.append(f"{col_name} TEXT")
        
        column_defs.append("synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        
        warehouse_hook.run(f"""
            CREATE TABLE IF NOT EXISTS user_anime (
                {', '.join(column_defs)}
            )
        """)
        
        placeholders = ','.join(['%s'] * len(columns))
        columns_str = ','.join(columns)
        update_cols = ','.join([f"{col} = EXCLUDED.{col}" for col in columns if col != 'id'])
        
        for row in user_anime_data:
            warehouse_hook.run(f"""
                INSERT INTO user_anime ({columns_str}) VALUES ({placeholders})
                ON CONFLICT (id) DO UPDATE SET 
                    {update_cols},
                    synced_at = CURRENT_TIMESTAMP
            """, parameters=row)
        
        logger.info(f"Synced {len(user_anime_data)} records from user_anime table")
        
        if datetime.now().weekday() == 0:
            detect_deletions(vitess_hook, warehouse_hook, 'user_anime')
    else:
        logger.info("No new/updated data found in user_anime table")
    
    update_sync_timestamp(warehouse_hook, 'user_anime', current_sync)

def detect_deletions(vitess_hook, warehouse_hook, table_name):
    """Compare source and warehouse to detect deletions"""
    logger.info(f"Detecting deletions for {table_name}")
    
    # Get all IDs from source
    source_ids = vitess_hook.get_records(f"SELECT id FROM {table_name}")
    source_id_set = {str(row[0]) for row in source_ids}
    
    # Get all IDs from warehouse
    warehouse_ids = warehouse_hook.get_records(f"SELECT id FROM {table_name}")
    warehouse_id_set = {str(row[0]) for row in warehouse_ids}
    
    # Find deleted records
    deleted_ids = warehouse_id_set - source_id_set
    
    if deleted_ids:
        # Mark as deleted or remove based on your strategy
        deleted_ids_str = ','.join(deleted_ids)
        warehouse_hook.run(f"DELETE FROM {table_name} WHERE id IN ({deleted_ids_str})")
        logger.info(f"Deleted {len(deleted_ids)} records from {table_name}")
    else:
        logger.info(f"No deletions detected for {table_name}")

with DAG(
    dag_id="vitess_warehouse_sync",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["sync", "vitess", "warehouse"],
    default_args={
        'owner': 'data-team',
        'depends_on_past': False,
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
    }
) as dag:
    
    sync_users_task = PythonOperator(
        task_id="sync_users",
        python_callable=sync_users_table
    )
    
    sync_user_list_task = PythonOperator(
        task_id="sync_user_list",
        python_callable=sync_user_list_table
    )
    
    sync_user_anime_task = PythonOperator(
        task_id="sync_user_anime",
        python_callable=sync_user_anime_table
    )
    
    # Set task dependencies - all can run in parallel
    [sync_users_task, sync_user_list_task, sync_user_anime_task]