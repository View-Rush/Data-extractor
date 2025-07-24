# storage/metadata_store.py

import os
import csv
from datetime import datetime, timedelta
from pathlib import Path

METADATA_FOLDER = Path("data/metadata")
METADATA_FOLDER.mkdir(parents=True, exist_ok=True)

class MetadataStore:
    def __init__(self, filename="video_metadata.csv"):
        self.filepath = METADATA_FOLDER / filename
        self.headers = ["video_id", "channel_id", "published_at", "last_checked", "days_tracked"]

        if not self.filepath.exists():
            self._init_file()

    def _init_file(self):
        with open(self.filepath, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self.headers)
            writer.writeheader()

    def add_video(self, video_id, channel_id, published_at):
        existing = self.load_all()
        if video_id in {v["video_id"] for v in existing}:
            return  # Already exists

        with open(self.filepath, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self.headers)
            writer.writerow({
                "video_id": video_id,
                "channel_id": channel_id,
                "published_at": published_at,
                "last_checked": "",
                "days_tracked": 0
            })

    def load_all(self):
        with open(self.filepath, "r") as f:
            reader = csv.DictReader(f)
            return list(reader)

    def get_active_videos(self, max_days=30):
        all_videos = self.load_all()
        active = []
        now = datetime.utcnow()

        for video in all_videos:
            published = datetime.fromisoformat(video["published_at"].replace("Z", "+00:00"))
            days_since_publish = (now - published).days
            if int(video["days_tracked"]) < max_days and days_since_publish <= max_days:
                active.append(video)

        return active

    def update_video_check(self, video_id):
        videos = self.load_all()
        updated = []
        for video in videos:
            if video["video_id"] == video_id:
                video["last_checked"] = datetime.utcnow().isoformat()
                video["days_tracked"] = str(int(video.get("days_tracked", "0")) + 1)
            updated.append(video)

        # Write back
        with open(self.filepath, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self.headers)
            writer.writeheader()
            writer.writerows(updated)
