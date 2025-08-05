# scripts/fetch_new_uploads.py
import os
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from dateutil import parser

from api.requests.get_video_details import GetVideoDetails
from db.supabase_client import fetch_channel_upload_playlist_ids_batch, mark_channel_inactive, insert_video, \
    insert_video_schedule
from mappers.map_video_metadata import map_video_metadata
from src.api.youtube_client import YouTubeClient
from src.api.quota_manager import YouTubeQuotaManager


def main():

    load_dotenv()
    api_keys = os.getenv("YOUTUBE_API_KEYS", "").split(",")

    quota_manager = YouTubeQuotaManager(api_keys)

    yt = YouTubeClient(quota_manager)

    # TODO: for all ids
    upload_playlist_ids = [playlist_id[0] for playlist_id in fetch_channel_upload_playlist_ids_batch()[:1]]

    since_time = datetime.now(timezone.utc) - timedelta(days=1000)

    # collect videos so can fetch data with batch API
    video_ids = []

    for playlist_id in upload_playlist_ids:
        try:
            videos = yt.get_recent_uploads(playlist_id, since=since_time, max_results=25)
            if videos:
                video_ids.extend(videos)
            print(f"{playlist_id}: {len(videos)} recent videos")
        except RuntimeError as e:
            mark_channel_inactive(playlist_id)
            print(
                f"Failed to fetch for {playlist_id}: {e}. Channel marked as inactive."
            )
        except Exception as e:
            print(f"Failed to fetch for {playlist_id}: {e}")

    # Step 2: Get video details in batches
    if not video_ids:
        print("No new video IDs found.")
        return

    video_details_fetcher = GetVideoDetails()
    try:
        video_details = quota_manager.execute(video_details_fetcher, video_ids=video_ids)
    except Exception as e:
        print(f"Failed to fetch video details: {e}")
        return

    # Step 3: Map and insert video details
    print(f"Fetched {len(video_details)} full video records")

    for video in video_details:
        try:
            video_record = map_video_metadata(video)
            insert_video(**video_record)

            print(f"Inserted video: {video_record['id']}")

            # Parse published_at to datetime for upload_datetime
            published_at_str = video_record.get("published_at")
            upload_datetime = parser.isoparse(published_at_str) if published_at_str else None

            # Calculate bin_id as the hour of the upload time (0-23)
            bin_id = upload_datetime.hour if upload_datetime else None

            if upload_datetime:
                insert_video_schedule(
                    video_id=video_record["id"],
                    upload_datetime=upload_datetime,
                    current_sample=0,
                    bin_id=bin_id
                )

            print(f"Inserted video schedule: {video_record['id']}")

        except Exception as e:
            print(f"Failed to insert video {video.get('id')}: {e}")

if __name__ == "__main__":
    main()
