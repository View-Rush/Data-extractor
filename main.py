# main.py

import os
from dotenv import load_dotenv
from api.youtube_client import YouTubeClient
from api.quota_manager import YouTubeQuotaManager
from storage.bigquery_storage_manager import BigQueryStorageManager

def main():
    load_dotenv()
    api_keys = os.getenv("YOUTUBE_API_KEYS", "").split(",")

    if not api_keys or not api_keys[0]:
        print("Missing YOUTUBE_API_KEYS in .env file")
        return

    quota_manager = YouTubeQuotaManager(api_keys)

    # Initialize BigQuery storage
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_id = os.getenv("BQ_DATASET_ID")
    storage = BigQueryStorageManager(project_id, dataset_id)
    storage.create_tables()

    yt_client = YouTubeClient(quota_manager, storage)

    video_ids = ["dQw4w9WgXcQ", "Zi_XLOBDo_Y"]

    print("Fetching and storing video details...")
    yt_client.get_video_details(video_ids)

if __name__ == "__main__":
    main()
