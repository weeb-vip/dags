import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from clickhouse_connect import get_client
from sqlalchemy import create_engine
from datetime import datetime, date
import json


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

    df = pd.read_sql("SELECT id, title_en, episodes, start_date, genres FROM anime", engine)

    # Parse and clean `start_date`
    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
    df = df[df["start_date"].notna()]
    df["start_date"] = df["start_date"].dt.date

    df["episodes"] = pd.to_numeric(df["episodes"], errors="coerce").astype("Int64")
    df["title_en"] = df["title_en"].astype("string")
    df["id"] = df["id"].astype("string")

    # Final drop of any rows that might still be problematic
    df = df.dropna(subset=["start_date"])

    # ClickHouse insert
    ch_conn = BaseHook.get_connection("clickhouse")
    client = get_client(host=ch_conn.host, port=int(ch_conn.port))

    client.command("""
        CREATE TABLE IF NOT EXISTS anime_summary (
            id String,
            title_en Nullable(String),
            episodes Nullable(UInt32),
            start_date Nullable(Date)
        ) ENGINE = MergeTree ORDER BY id
    """)

    client.insert_df("anime_summary", df[["id", "title_en", "episodes", "start_date"]])

    # Extract tags from genres JSON array and normalize
    tag_rows = []
    for _, row in df.iterrows():
        if row.genres:
            try:
                genres = json.loads(row.genres)
                if isinstance(genres, list):
                    for tag in genres:
                        tag_rows.append({
                            "anime_id": row.id,
                            "tag": tag.strip()
                        })
            except Exception:
                continue

    tags_df = pd.DataFrame(tag_rows)

    client.command("""
        CREATE TABLE IF NOT EXISTS anime_tags (
            anime_id String,
            tag String
        ) ENGINE = MergeTree ORDER BY (anime_id, tag)
    """)

    if not tags_df.empty:
        tags_df["anime_id"] = tags_df["anime_id"].astype("string")
        tags_df["tag"] = tags_df["tag"].astype("string")
        client.insert_df("anime_tags", tags_df)


with DAG("anime_to_clickhouse", start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False) as dag:
    PythonOperator(
        task_id="load_anime_data",
        python_callable=extract_and_load
    )