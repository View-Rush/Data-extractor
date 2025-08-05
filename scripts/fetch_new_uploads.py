# scripts/fetch_new_uploads.py
import os
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

from db.supabase_client import fetch_channel_upload_playlist_ids_batch, mark_channel_inactive
from src.api.youtube_client import YouTubeClient
from src.api.quota_manager import YouTubeQuotaManager


def main():

    load_dotenv()
    api_keys = os.getenv("YOUTUBE_API_KEYS", "").split(",")

    quota_manager = YouTubeQuotaManager(api_keys)

    yt = YouTubeClient(quota_manager)

    upload_playlist_ids = [playlist_id[0] for playlist_id in fetch_channel_upload_playlist_ids_batch()]

    since_time = datetime.now(timezone.utc) - timedelta(days=1)

    for playlist_id in upload_playlist_ids:
        try:
            videos = yt.get_recent_uploads(playlist_id, since=since_time, max_results=25)
            print(videos)
            print(f"{playlist_id}: {len(videos)} recent videos")
        except RuntimeError as e:
            mark_channel_inactive(playlist_id)
            print(
                f"Failed to fetch for {playlist_id}: {e}. Channel marked as inactive."
            )
        except Exception as e:
            print(f"Failed to fetch for {playlist_id}: {e}")

if __name__ == "__main__":
    main()
