# api/youtube_client.py

from typing import List
from api.quota_manager import YouTubeQuotaManager
from api.requests.get_video_details import GetVideoDetails

class YouTubeClient:
    def __init__(self, quota_manager: YouTubeQuotaManager):
        self.qm = quota_manager

    def get_video_details(self, video_ids: List[str], part: str = None):
        """
        Fetches detailed metadata for one or more videos.

        Args:
            video_ids (list[str]): List of YouTube video IDs.
            part (str): Optional comma-separated string for specific parts to retrieve.

        Returns:
            list: List of video metadata dictionaries.
        """
        request = GetVideoDetails()

        if part is None:
            return self.qm.execute(request, video_ids=video_ids)

        return self.qm.execute(request, video_ids=video_ids, part=part)
