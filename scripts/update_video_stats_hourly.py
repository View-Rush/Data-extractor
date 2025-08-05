# scripts/update_video_stats_hourly.py

import os
from datetime import datetime, timezone
from dotenv import load_dotenv

from api.requests.get_video_stats_snapshot import GetVideoStatsSnapshot
from db.bigquery_client import insert_video_stats
from src.api.youtube_client import YouTubeClient
from src.api.quota_manager import YouTubeQuotaManager
from src.db.database_client import update_video_stats, increment_video_sample, \
    fetch_videos_for_bin_paginated, get_current_sample
from src.utils.logger import setup_logger

# Load environment and config
load_dotenv()
logger = setup_logger(__name__)

# Environment config
api_keys = os.getenv("YOUTUBE_API_KEYS", "").split(",")

if not api_keys or not api_keys[0]:
    raise RuntimeError("Missing YOUTUBE_API_KEYS in .env")

quota_manager = YouTubeQuotaManager(api_keys)
yt_client = YouTubeClient(quota_manager)


def main():
    current_hour = datetime.now(timezone.utc).hour
    logger.info(f"Fetching videos in bin {current_hour}...")

    batch_size = 50
    offset = 0
    video_ids = []

    while True:
        batch = fetch_videos_for_bin_paginated(current_hour, batch_size=batch_size, offset=offset)
        if not batch:
            break
        video_ids.extend([row[0] for row in batch])
        offset += batch_size

    if not video_ids:
        logger.info("No videos to process.")
        return

    if not video_ids:
        logger.info("No videos to process.")
        return

    fetcher = GetVideoStatsSnapshot()

    try:
        video_details_list = quota_manager.execute(fetcher, video_ids=video_ids)
    except Exception as e:
        logger.error(f"Failed to fetch video details: {e}")
        return

    logger.info(f"Fetched stats for {len(video_details_list)} videos")

    now = datetime.now(timezone.utc)

    for video in video_details_list:
        try:
            video_id = video["id"]
            stats = video.get("statistics", {})

            view_count = int(stats.get("viewCount", 0))
            like_count = int(stats.get("likeCount", 0))
            favourite_count = int(stats.get("favoriteCount", 0))
            comment_count = int(stats.get("commentCount", 0))

            update_video_stats(
                video_id=video_id,
                view_count=view_count,
                likes_count=like_count,
                favourite_count=favourite_count,
                comment_count=comment_count
            )

            day_index = get_current_sample(video_id)[0]

            logger.info(f"Video {video_id} has {day_index} samples")

            increment_video_sample(video_id)

            # Insert into BigQuery
            record = {
                "videoId": video_id,
                "recordedAt": now.isoformat(),
                "viewCount": view_count,
                "likeCount": like_count,
                "commentCount": comment_count,
                "dayIndex": day_index,
                "fetchedFrom": "hourly_script"
            }

            insert_video_stats(record)
            logger.info(f"Updated stats and inserted BigQuery row for video: {video_id}")

        except Exception as e:
            logger.exception(f"Error processing video {video.get('id')}: {e}")


if __name__ == "__main__":
    main()
