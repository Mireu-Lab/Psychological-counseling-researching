from __future__ import annotations

from pathlib import Path

import pandas as pd
import torch
from google.cloud import bigquery
from google.oauth2 import service_account
from tqdm.auto import tqdm
from transformers import AutoModel, AutoTokenizer


PROJECT_ROOT = Path(__file__).resolve().parents[1]
KEY_PATH = PROJECT_ROOT / ".key/testprojects-453622-d1f78fcce8b7.json"
SQL_PATH = Path(__file__).with_name("3_test.sql")
PROJECT_ID = "testprojects-453622"
DATASET_ID = "Psychological_counseling_data"
SOURCE_TABLE = f"`{PROJECT_ID}.{DATASET_ID}.morpheme_classification`"
TARGET_TABLE = f"{PROJECT_ID}.{DATASET_ID}.morpheme_classification_kmbert_embedding"
MODEL_NAME = "madatnlp/km-bert"


def create_client() -> bigquery.Client:
    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    return bigquery.Client(credentials=credentials, project=credentials.project_id)


def load_query() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def mean_pooling(last_hidden_state: torch.Tensor, attention_mask: torch.Tensor) -> torch.Tensor:
    mask = attention_mask.unsqueeze(-1).expand(last_hidden_state.size()).float()
    masked = last_hidden_state * mask
    summed = masked.sum(dim=1)
    counts = torch.clamp(mask.sum(dim=1), min=1e-9)
    return summed / counts


def main() -> None:
    if not KEY_PATH.exists():
        raise FileNotFoundError(f"GCP key not found: {KEY_PATH}")
    if not SQL_PATH.exists():
        raise FileNotFoundError(f"SQL file not found: {SQL_PATH}")

    client = create_client()
    print(f"Client created with project: {client.project}")

    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    model = AutoModel.from_pretrained(MODEL_NAME)

    sample_text = "오늘 선생님과 세 번째 상담 시작하도록 할게요."
    sample_inputs = tokenizer(sample_text, return_tensors="pt", truncation=True, max_length=256)
    print(sample_inputs)

    schema_query = """
    SELECT column_name, data_type
    FROM `testprojects-453622.Psychological_counseling_data.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'morpheme_classification'
    ORDER BY ordinal_position
    """
    schema_df = client.query(schema_query).to_dataframe()
    print(schema_df)

    rows_df = client.query(load_query()).to_dataframe()
    print(f"rows: {len(rows_df):,}")

    if rows_df.empty:
        raise ValueError("No rows found to embed.")

    device = "cuda" if torch.cuda.is_available() else "cpu"
    model = model.to(device)
    model.eval()

    texts = rows_df["split_contents"].astype(str).tolist()
    batch_size = 32
    embeddings: list[list[float]] = []

    with torch.no_grad():
        for start in tqdm(range(0, len(texts), batch_size), desc="Embedding"):
            batch_texts = texts[start : start + batch_size]
            encoded = tokenizer(
                batch_texts,
                return_tensors="pt",
                padding=True,
                truncation=True,
                max_length=256,
            )
            encoded = {key: value.to(device) for key, value in encoded.items()}
            outputs = model(**encoded)
            pooled = mean_pooling(outputs.last_hidden_state, encoded["attention_mask"])
            embeddings.extend(pooled.cpu().tolist())

    result_df = rows_df.copy()
    result_df["embedding_model"] = MODEL_NAME
    result_df["embedding_dim"] = len(embeddings[0])
    result_df["content_embedding"] = embeddings
    result_df["embedded_at"] = pd.Timestamp.utcnow()

    schema = [
        bigquery.SchemaField("file_code", "STRING"),
        bigquery.SchemaField("speaker", "STRING"),
        bigquery.SchemaField("session_no", "INT64"),
        bigquery.SchemaField("timeline_index", "INT64"),
        bigquery.SchemaField("end_content_index", "INT64"),
        bigquery.SchemaField("split_row_index", "INT64"),
        bigquery.SchemaField("split_contents", "STRING"),
        bigquery.SchemaField("embedding_model", "STRING"),
        bigquery.SchemaField("embedding_dim", "INT64"),
        bigquery.SchemaField("content_embedding", "FLOAT64", mode="REPEATED"),
        bigquery.SchemaField("embedded_at", "TIMESTAMP"),
    ]

    load_job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema,
    )

    load_job = client.load_table_from_dataframe(
        result_df,
        TARGET_TABLE,
        job_config=load_job_config,
        parquet_compression="SNAPPY",
    )
    load_job.result()

    print(f"Saved {len(result_df):,} rows to {TARGET_TABLE}")
    print(f"Embedding dim: {result_df['embedding_dim'].iloc[0]}")


if __name__ == "__main__":
    main()