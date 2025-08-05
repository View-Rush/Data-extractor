# api/youtube_client.py
from datetime import datetime
from typing import List

from src.api.requests.get_playlist_videos import GetPlaylistVideos
from src.api.quota_manager import YouTubeQuotaManager
from src.api.requests.get_channel_details import GetChannelDataByHandleOrId
from src.api.requests.get_video_details import GetVideoDetails


class YouTubeClient:
    def __init__(self, quota_manager: YouTubeQuotaManager):
        self.qm = quota_manager

    def fetch_video_details(self, video_ids: List[str], part: str = None):
        """
        Fetches detailed metadata for one or more videos.

        Args:
            video_ids (list[str]): List of YouTube video IDs.
            part (str): Optional comma-separated string for specific parts to retrieve.

        Returns:
            list: List of video metadata dictionaries.
        """
        request = GetVideoDetails()
        raw_items = self.qm.execute(request, video_ids=video_ids) if part is None \
            else self.qm.execute(request, video_ids=video_ids, part=part)

        return raw_items

    def fetch_channel_details(self, channel_ids: List[str], part: str = None):
        """
        Fetches metadata for one or more YouTube channels.

        Args:
            channel_ids (list[str]): List of YouTube channel IDs.
            part (str): Optional comma-separated string for specific parts to retrieve (e.g., "snippet,statistics").

        Returns:
            list: List of channel metadata dictionaries.
        """
        request = GetChannelDataByHandleOrId()
        raw_items = self.qm.execute(request, channel_ids=channel_ids) if part is None \
            else self.qm.execute(request, channel_ids=channel_ids, part=part)

        return raw_items

    def get_recent_uploads(self, channel_id: str, since: datetime = None, max_results: int = 50):
        """
        Fetch recent uploads from a channel's uploads playlist.

        Args:
            channel_id (str): YouTube Channel ID (e.g. 'UC...')
            since (datetime): Only return videos published after this datetime (UTC).
            max_results (int): Maximum number of videos to fetch.

        Returns:
            list of dict: Video items.
        """
        request = GetPlaylistVideos()
        return self.qm.execute(
            request,
            identifier=channel_id,
            max_results=max_results,
            since_datetime=since
        )
