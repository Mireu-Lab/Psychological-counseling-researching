from __future__ import annotations

from pathlib import Path

import polars as pl
from google.cloud import bigquery
from google.oauth2 import service_account
from kiwipiepy import Kiwi


PROJECT_ROOT = Path(__file__).resolve().parents[1]
KEY_PATH = PROJECT_ROOT / ".key/testprojects-453622-d1f78fcce8b7.json"
SQL_PATH = Path(__file__).with_name("2_test.sql")
PROJECT_ID = "testprojects-453622"
DATASET_ID = "Psychological_counseling_data"
TARGET_TABLE = f"{PROJECT_ID}.{DATASET_ID}.morpheme_classification"

END_EOMI_TAG = "EF"
END_SYMBOL_TAGS = {"SF", "SE", "SO"}


def create_client() -> bigquery.Client:
    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    return bigquery.Client(credentials=credentials, project=credentials.project_id)


def split_sentences_by_pos(kiwi: Kiwi, text: str | None) -> list[str]:
    if text is None:
        return []

    tokens = kiwi.tokenize(str(text))
    results: list[str] = []
    buffer: list[str] = []
    waiting_end_symbol = False

    for token in tokens:
        buffer.append(token.form)

        if token.tag == END_EOMI_TAG:
            waiting_end_symbol = True
            continue

        if waiting_end_symbol and token.tag in END_SYMBOL_TAGS:
            sentence = "".join(buffer).strip()
            if sentence:
                results.append(sentence)
            buffer = []
            waiting_end_symbol = False
            continue

        waiting_end_symbol = False

    return results


def resolve_col(columns: list[str], candidates: list[str], required: bool = True) -> str | None:
    lowered = {column.lower(): column for column in columns}
    for candidate in candidates:
        found = lowered.get(candidate.lower())
        if found is not None:
            return found
    if required:
        raise ValueError(f"필수 컬럼이 없습니다. 후보={candidates}, 현재={columns}")
    return None


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

    file_code_col = resolve_col(raw_df.columns, ["file_code"])
    content_col = resolve_col(raw_df.columns, ["content"])
    speaker_col = resolve_col(raw_df.columns, ["speaker"], required=False)
    session_no_col = resolve_col(raw_df.columns, ["session_no"], required=False)
    index_col = resolve_col(raw_df.columns, ["index"], required=False)

    sort_cols = [file_code_col]
    if index_col is not None:
        sort_cols.append(index_col)

    kiwi = Kiwi()
    base_df = (
        raw_df.with_row_index("_orig_order")
        .sort(sort_cols + ["_orig_order"])
        .with_columns((pl.col(file_code_col).cum_count().over(file_code_col) - 1).alias("timeline_index"))
    )

    records: list[dict[str, object]] = []
    for row in base_df.iter_rows(named=True):
        for end_content_index, sentence in enumerate(split_sentences_by_pos(kiwi, row.get(content_col))):
            records.append(
                {
                    "file_code": row[file_code_col],
                    "speaker": row.get(speaker_col) if speaker_col is not None else None,
                    "session_no": row.get(session_no_col) if session_no_col is not None else None,
                    "timeline_index": row["timeline_index"],
                    "end_content_index": end_content_index,
                    "split_contents": sentence,
                }
            )

    if records:
        split_df = (
            pl.DataFrame(records)
            .sort(["file_code", "timeline_index", "end_content_index"])
            .with_columns((pl.col("file_code").cum_count().over("file_code") - 1).alias("split_row_index"))
            .select(
                [
                    "file_code",
                    "speaker",
                    "session_no",
                    "timeline_index",
                    "end_content_index",
                    "split_row_index",
                    "split_contents",
                ]
            )
        )
    else:
        split_df = pl.DataFrame(
            schema={
                "file_code": pl.Utf8,
                "speaker": pl.Utf8,
                "session_no": pl.Int64,
                "timeline_index": pl.Int64,
                "end_content_index": pl.Int64,
                "split_row_index": pl.Int64,
                "split_contents": pl.Utf8,
            }
        )

    print(split_df)

    pandas_df = split_df.to_pandas()
    pandas_df.to_gbq(
        destination_table=TARGET_TABLE,
        project_id=PROJECT_ID,
        if_exists="replace",
        credentials=service_account.Credentials.from_service_account_file(KEY_PATH),
    )
    print(f"Table {TARGET_TABLE} uploaded successfully.")


if __name__ == "__main__":
    main()