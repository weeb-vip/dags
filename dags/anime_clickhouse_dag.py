import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from clickhouse_connect import get_client
from sqlalchemy import create_engine
from datetime import datetime, date


def extract_and_load():
    # Get MySQL conn from Airflow
    mysql_conn = BaseHook.get_connection("weeb-readonly")
    mysql_url = (
        f"mysql+pymysql://{mysql_conn.login}:{mysql_conn.password}"
        f"@{mysql_conn.host}:{mysql_conn.port}/{mysql_conn.schema}"
    )

    engine = create_engine(
        mysql_url,
        connect_args={"ssl": {"check_hostname": False}}
    )

    df = pd.read_sql("SELECT id, title_en, episodes, start_date FROM anime", engine)

    # Extract just the year
    df["year"] = pd.to_datetime(df["start_date"], errors="coerce").dt.year.astype("Int64")

    # Drop start_date column
    df.drop(columns=["start_date"], inplace=True)

    # Clean up types
    df["episodes"] = pd.to_numeric(df["episodes"], errors="coerce").astype("Int64")
    df["title_en"] = df["title_en"].astype("string")
    df["id"] = df["id"].astype("string")

    # ClickHouse insert
    ch_conn = BaseHook.get_connection("clickhouse")
    client = get_client(host=ch_conn.host, port=int(ch_conn.port))

    client.command("""
        CREATE TABLE IF NOT EXISTS anime_summary (
            id String,
            title_en Nullable(String),
            episodes Nullable(UInt32),
            year Nullable(UInt16)
        ) ENGINE = MergeTree ORDER BY id
    """)

    client.insert_df("anime_summary", df)


with DAG("anime_to_clickhouse", start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False) as dag:
    PythonOperator(
        task_id="load_anime_data",
        python_callable=extract_and_load
    )