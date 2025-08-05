import os
from datetime import timezone, datetime
import logging

from dotenv import load_dotenv
from google.cloud import bigquery

# Initialize client
load_dotenv()
client = bigquery.Client(project=os.getenv("GCP_PROJECT_ID"))
dataset_id = os.getenv("BQ_DATASET_ID")

# Table references
video_metadata_table = f"{client.project}.{dataset_id}.video_metadata"
video_stats_table = f"{client.project}.{dataset_id}.video_stats"
channel_metadata_table = f"{client.project}.{dataset_id}.channel_metadata"


def _parse_timestamp(ts: str) -> datetime:
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception as e:
        logging.warning(f"Failed to parse timestamp '{ts}': {e}")
        return datetime.now(timezone.utc)


def _insert_row(table_id: str, row: dict):
    def convert(value):
        if isinstance(value, datetime):
            return value.isoformat()
        return value

    sanitized_row = {k: convert(v) for k, v in row.items()}
    errors = client.insert_rows_json(table_id, [sanitized_row])
    if errors:
        logging.error(f"Error inserting row into {table_id}: {errors}")
    else:
        logging.info(f"Inserted row into {table_id}")


def insert_video_stats(stats_data: dict):
    if "recordedAt" in stats_data and isinstance(stats_data["recordedAt"], str):
        stats_data["recordedAt"] = _parse_timestamp(stats_data["recordedAt"])
    _insert_row(video_stats_table, stats_data)


def insert_bulk_video_stats(rows: list[dict]):
    for row in rows:
        if "recordedAt" in row and isinstance(row["recordedAt"], str):
            row["recordedAt"] = _parse_timestamp(row["recordedAt"])
    errors = client.insert_rows_json(video_stats_table, rows)
    if errors:
        logging.error(f"BigQuery insert errors: {errors}")
    else:
        logging.info(f"Inserted {len(rows)} rows into {video_stats_table}")
