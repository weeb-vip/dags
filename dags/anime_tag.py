import pandas as pd
import uuid
import unicodedata
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine
from datetime import datetime


def clean_tag(tag: str) -> str:
    return unicodedata.normalize("NFKC", tag.strip().lower())


def extract_and_create_tags():
    mysql_conn = BaseHook.get_connection("weeb-readonly")
    mysql_url = (
        f"mysql+pymysql://{mysql_conn.login}:{mysql_conn.password}"
        f"@{mysql_conn.host}:{mysql_conn.port}/{mysql_conn.schema}"
    )
    engine = create_engine(
        mysql_url,
        connect_args={"ssl": {"check_hostname": False}}
    )

    # Read anime with genres
    df = pd.read_sql("SELECT id, genres FROM anime WHERE genres IS NOT NULL", engine)

    tag_map = {}  # normalized_tag -> tag_id
    tag_rows = []
    anime_tag_links = []

    for _, row in df.iterrows():
        anime_id = str(row["id"])
        genres = row["genres"]
        if not genres:
            continue

        for raw_tag in genres.split(","):
            tag = clean_tag(raw_tag)
            if not tag:
                continue
            if tag not in tag_map:
                tag_id = str(uuid.uuid4())
                tag_map[tag] = tag_id
                tag_rows.append({"id": tag_id, "name": tag})
            anime_tag_links.append({"anime_id": anime_id, "tag_id": tag_map[tag]})

    # Upsert tags
    tags_df = pd.DataFrame(tag_rows)
    anime_tag_df = pd.DataFrame(anime_tag_links)

    with engine.begin() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tags (
                id CHAR(36) PRIMARY KEY,
                name VARCHAR(255) UNIQUE NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS anime_tags (
                anime_id CHAR(36) NOT NULL,
                tag_id CHAR(36) NOT NULL,
                PRIMARY KEY (anime_id, tag_id),
                FOREIGN KEY (anime_id) REFERENCES anime(id),
                FOREIGN KEY (tag_id) REFERENCES tags(id)
            )
        """)

    tags_df.to_sql("tags", con=engine, if_exists="append", index=False, method="multi")
    anime_tag_df.to_sql("anime_tags", con=engine, if_exists="append", index=False, method="multi")


with DAG(
    "extract_anime_tags",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    PythonOperator(
        task_id="normalize_and_insert_tags",
        python_callable=extract_and_create_tags,
    )
