from __future__ import annotations

from pathlib import Path

import polars as pl
from google.cloud import bigquery
from google.oauth2 import service_account


PROJECT_ROOT = Path(__file__).resolve().parents[1]
KEY_PATH = PROJECT_ROOT / ".key/testprojects-453622-d1f78fcce8b7.json"
SQL_PATH = Path(__file__).with_name("1_test.sql")
PROJECT_ID = "testprojects-453622"
DATASET_ID = "Psychological_counseling_data"
TARGET_TABLE = f"{PROJECT_ID}.{DATASET_ID}.processed_data"


def create_client() -> bigquery.Client:
    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    return bigquery.Client(credentials=credentials, project=credentials.project_id)


def load_query() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def main() -> None:
    if not KEY_PATH.exists():
        raise FileNotFoundError(f"GCP key not found: {KEY_PATH}")
    if not SQL_PATH.exists():
        raise FileNotFoundError(f"SQL file not found: {SQL_PATH}")

    client = create_client()
    print(f"Client created with project: {client.project}")

    raw_rows = client.query(load_query()).result()
    raw_df = pl.from_arrow(raw_rows.to_arrow())

    if "Index" in raw_df.columns and "index" not in raw_df.columns:
        raw_df = raw_df.rename({"Index": "index"})

    file_names_df = raw_df.with_columns(
        pl.col("FilePath").str.split("/").list.last().alias("FileName")
    )

    parsed_file_name_df = (
        file_names_df.with_columns(
            pl.col("FileName")
            .str.replace(r"\.[^.]+$", "")
            .str.split("_")
            .alias("parts")
        )
        .with_columns(
            pl.col("parts").list.get(0).alias("prefix"),
            pl.col("parts").list.get(1).alias("topic"),
            pl.col("parts").list.get(2).cast(pl.Int64, strict=False).alias("session_no"),
            pl.col("parts").list.get(3).alias("stage"),
            pl.col("parts").list.get(4).alias("file_code"),
        )
        .drop("parts")
    )

    total_rows = parsed_file_name_df.height
    enriched_df = parsed_file_name_df.with_columns(
        pl.len().over("file_code").alias("file_code_row_count"),
        pl.len().over("Speaker").alias("speaker_row_count"),
    )
    enriched_df = enriched_df.with_columns(
        (pl.col("file_code_row_count") / total_rows * 100).round(4).alias("file_code_ratio_pct"),
        (pl.col("speaker_row_count") / total_rows * 100).round(4).alias("speaker_ratio_pct"),
    )

    print(enriched_df.head(20))
    print(enriched_df.shape)

    pandas_df = enriched_df.to_pandas()
    pandas_df.to_gbq(
        destination_table=TARGET_TABLE,
        project_id=PROJECT_ID,
        if_exists="replace",
        credentials=service_account.Credentials.from_service_account_file(KEY_PATH),
    )
    print(f"Table {TARGET_TABLE} uploaded successfully.")


if __name__ == "__main__":
    main()