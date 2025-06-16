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

    df = pd.read_sql("SELECT id, genres FROM anime", engine)
    df = df[df["genres"].notna()]  # Remove rows with no genres

    # Extract genres as JSON array and explode into rows
    def parse_genres(value):
        try:
            genres = json.loads(value)
            if isinstance(genres, list):
                return [tag.strip() for tag in genres if isinstance(tag, str)]
        except Exception:
            return []
        return []

    df["genres"] = df["genres"].apply(parse_genres)
    df = df.explode("genres")

    # Create unique tag table
    unique_tags = pd.DataFrame(df["genres"].drop_duplicates()).reset_index(drop=True)
    unique_tags["tag_id"] = unique_tags.index.astype("Int64")

    # Join back to anime_tag map
    df = df.merge(unique_tags, on="genres")
    anime_tags = df[["id", "tag_id"]].rename(columns={"id": "anime_id"})

    # ClickHouse connection
    ch_conn = BaseHook.get_connection("clickhouse")
    client = get_client(host=ch_conn.host, port=int(ch_conn.port))

    # Create tables in ClickHouse
    client.command("""
        CREATE TABLE IF NOT EXISTS tags (
            tag_id UInt32,
            name String
        ) ENGINE = MergeTree ORDER BY tag_id
    """)

    client.command("""
        CREATE TABLE IF NOT EXISTS anime_tags (
            anime_id String,
            tag_id UInt32
        ) ENGINE = MergeTree ORDER BY (anime_id, tag_id)
    """)

    # Insert data
    client.insert_df("tags", unique_tags[["tag_id", "genres"]].rename(columns={"genres": "name"}))
    client.insert_df("anime_tags", anime_tags)


with DAG("anime_tags_to_clickhouse", start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False) as dag:
    PythonOperator(
        task_id="extract_and_load_tags",
        python_callable=extract_and_load
    )