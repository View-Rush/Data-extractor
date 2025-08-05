# src/api/requests/get_video_stats_snapshot.py

from googleapiclient.discovery import Resource
from src.api.youtube_api_request import YouTubeAPIRequest


class GetVideoStatsSnapshot(YouTubeAPIRequest):
    """
    Fetches a lightweight snapshot of video statistics (views, likes, comments)
    suitable for time-series tracking and BigQuery logging.

    Args:
        service (Resource): Authorized YouTube API service.
        video_ids (list[str] or str): One or more YouTube video IDs.

    Returns:
        list: List of dictionaries with partial video statistics.
    """

    PART = "id,statistics"
    MAX_IDS_PER_REQUEST = 50
    request_count = 0

    def execute(self, service: Resource, video_ids):
        if isinstance(video_ids, str):
            video_ids = [video_ids]

        all_items = []

        for i in range(0, len(video_ids), self.MAX_IDS_PER_REQUEST):
            chunk = video_ids[i:i + self.MAX_IDS_PER_REQUEST]
            response = service.videos().list(
                part=self.PART,
                id=",".join(chunk)
            ).execute()

            self.request_count += 1
            items = response.get("items", [])
            all_items.extend(items)

        return all_items

    def get_quota(self):
        return self.request_count
