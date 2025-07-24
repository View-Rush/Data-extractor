# storage/bigquery_storage_manager.py

from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from storage.storage_manager import StorageManager
import datetime
import logging

class BigQueryStorageManager(StorageManager):
    def __init__(self, project_id: str, dataset_id: str):
        self.client = bigquery.Client(project=project_id)
        self.dataset_id = dataset_id
        self.video_metadata_table = f"{project_id}.{dataset_id}.video_metadata"
        self.video_stats_table = f"{project_id}.{dataset_id}.video_stats"

    def save_video_metadata(self, video_data: dict):
        if "publishedAt" in video_data and isinstance(video_data["publishedAt"], str):
            video_data["publishedAt"] = self._parse_timestamp(video_data["publishedAt"])
        self._insert_row(self.video_metadata_table, video_data)

    def save_video_stats(self, stats_data: dict):
        if "recordedAt" in stats_data and isinstance(stats_data["recordedAt"], str):
            stats_data["recordedAt"] = self._parse_timestamp(stats_data["recordedAt"])
        self._insert_row(self.video_stats_table, stats_data)

    def get_video_details_by_id(self, video_id: str) -> dict:
        query = f"""
            SELECT * FROM `{self.video_metadata_table}`
            WHERE id = @video_id
            LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("video_id", "STRING", video_id)]
        )
        results = self.client.query(query, job_config=job_config).result()
        rows = list(results)
        return dict(rows[0]) if rows else {}

    def get_all_videos(self) -> list[dict]:
        query = f"SELECT * FROM `{self.video_metadata_table}`"
        results = self.client.query(query).result()
        return [dict(row) for row in results]

    def _insert_row(self, table_id: str, row: dict):
        # Convert datetime fields to ISO strings
        def convert(value):
            if isinstance(value, datetime.datetime):
                return value.isoformat()
            return value

        sanitized_row = {k: convert(v) for k, v in row.items()}

        errors = self.client.insert_rows_json(table_id, [sanitized_row])
        if errors:
            logging.error(f"Error inserting row into {table_id}: {errors}")
        else:
            logging.info(f"Inserted row into {table_id}")

    # TODO
    def _parse_timestamp(self, ts: str) -> datetime.datetime:
        try:
            return datetime.datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception as e:
            logging.warning(f"Failed to parse timestamp '{ts}': {e}")
            return datetime.datetime.utcnow()

    def create_tables(self):
        self._create_video_metadata_table()
        self._create_video_stats_table()

    def _create_video_metadata_table(self):
        schema = [
            bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("publishedAt", "TIMESTAMP"),
            bigquery.SchemaField("channelId", "STRING"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("description", "STRING"),
            bigquery.SchemaField("localizedTitle", "STRING"),
            bigquery.SchemaField("localizedDescription", "STRING"),
            bigquery.SchemaField("thumbnailDefault", "STRING"),
            bigquery.SchemaField("thumbnailMedium", "STRING"),
            bigquery.SchemaField("thumbnailHigh", "STRING"),
            bigquery.SchemaField("tags", "STRING", mode="REPEATED"),
            bigquery.SchemaField("categoryId", "STRING"),
            bigquery.SchemaField("liveBroadcastContent", "STRING"),
            bigquery.SchemaField("defaultLanguage", "STRING"),
            bigquery.SchemaField("defaultAudioLanguage", "STRING"),
            bigquery.SchemaField("videoDuration", "STRING"),
            bigquery.SchemaField("viewCount", "INTEGER"),
            bigquery.SchemaField("likesCount", "INTEGER"),
            bigquery.SchemaField("favouriteCount", "INTEGER"),
            bigquery.SchemaField("commentCount", "INTEGER"),
        ]

        table = bigquery.Table(self.video_metadata_table, schema=schema)
        try:
            self.client.get_table(self.video_metadata_table)
            print(f"Table already exists: {self.video_metadata_table}")
        except NotFound:
            self.client.create_table(table)
            print(f"Created table: {self.video_metadata_table}")

    def _create_video_stats_table(self):
        schema = [
            bigquery.SchemaField("videoId", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("recordedAt", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("dayIndex", "INTEGER"),
            bigquery.SchemaField("viewCount", "INTEGER"),
            bigquery.SchemaField("likeCount", "INTEGER"),
            bigquery.SchemaField("commentCount", "INTEGER"),
        ]

        table = bigquery.Table(self.video_stats_table, schema=schema)
        try:
            self.client.get_table(self.video_stats_table)
            print(f"Table already exists: {self.video_stats_table}")
        except NotFound:
            self.client.create_table(table)
            print(f"Created table: {self.video_stats_table}")
