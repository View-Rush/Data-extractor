# main.py

import os
from dotenv import load_dotenv
from api.youtube_client import YouTubeClient
from api.quota_manager import YouTubeQuotaManager

def main():
    load_dotenv()
    api_keys = os.getenv("YOUTUBE_API_KEYS", "").split(",")

    if not api_keys or not api_keys[0]:
        print("Missing YOUTUBE_API_KEYS in .env file (comma-separated)")
        return

    quota_manager = YouTubeQuotaManager(api_keys)
    yt_client = YouTubeClient(quota_manager)

    # Example video IDs
    video_ids = ["dQw4w9WgXcQ", "Zi_XLOBDo_Y"]

    print("Fetching video details:")
    details = yt_client.get_video_details(video_ids)
    for video in details:
        print(f"{video['id']} - {video['snippet']['title']}")

if __name__ == "__main__":
    main()
