# storage/sqlite_storage_manager.py

import sqlite3
from src.storage.storage_manager import StorageManager
import os
import json


class SQLiteStorageManager(StorageManager):
    def __init__(self, db_path="data/video_data.db"):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._create_tables()

    def _create_tables(self):
        cursor = self.conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS video_metadata (
                id TEXT PRIMARY KEY,
                publishedAt TEXT,
                channelId TEXT,
                title TEXT,
                description TEXT,
                localizedTitle TEXT,
                localizedDescription TEXT,
                thumbnailDefault TEXT,
                thumbnailMedium TEXT,
                thumbnailHigh TEXT,
                tags TEXT,  -- stored as JSON string
                categoryId TEXT,
                liveBroadcastContent TEXT,
                defaultLanguage TEXT,
                defaultAudioLanguage TEXT,
                videoDuration TEXT,
                viewCount INTEGER,
                likesCount INTEGER,
                favouriteCount INTEGER,
                commentCount INTEGER
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS video_stats (
                videoId TEXT,
                recordedAt TEXT,
                dayIndex INTEGER,
                viewCount INTEGER,
                likeCount INTEGER,
                commentCount INTEGER,
                PRIMARY KEY (videoId, recordedAt)
            )
        ''')

        self.conn.commit()

    def save_video_metadata(self, video_data: dict):
        video_data = video_data.copy()
        tags = json.dumps(video_data.get("tags", []))  # Serialize tags
        video_data["tags"] = tags

        placeholders = ", ".join(["?"] * len(video_data))
        columns = ", ".join(video_data.keys())
        values = tuple(video_data.values())

        sql = f'''
            INSERT OR REPLACE INTO video_metadata ({columns})
            VALUES ({placeholders})
        '''

        self.conn.execute(sql, values)
        self.conn.commit()

    def save_video_stats(self, stats_data: dict):
        placeholders = ", ".join(["?"] * len(stats_data))
        columns = ", ".join(stats_data.keys())
        values = tuple(stats_data.values())

        sql = f'''
            INSERT OR REPLACE INTO video_stats ({columns})
            VALUES ({placeholders})
        '''

        self.conn.execute(sql, values)
        self.conn.commit()

    def get_video_details_by_id(self, video_id: str) -> dict:
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM video_metadata WHERE id = ?", (video_id,))
        row = cursor.fetchone()
        return self._row_to_dict(cursor, row) if row else None

    def get_all_videos(self) -> list[dict]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM video_metadata")
        rows = cursor.fetchall()
        return [self._row_to_dict(cursor, row) for row in rows]

    def _row_to_dict(self, cursor, row):
        result = {col[0]: val for col, val in zip(cursor.description, row)}
        if "tags" in result and result["tags"]:
            try:
                result["tags"] = json.loads(result["tags"])
            except Exception:
                result["tags"] = []
        return result
