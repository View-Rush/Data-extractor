# api/youtube_client.py

from typing import List
from src.api.quota_manager import YouTubeQuotaManager
from src.api.requests.get_channel_details import GetChannelDataByHandleOrId
from src.api.requests.get_video_details import GetVideoDetails
from src.storage.storage_manager import StorageManager
from src.utils import parse_channel_metadata
from src.utils.video_parser import parse_video_stats, parse_video_metadata


class YouTubeClient:
    def __init__(self, quota_manager: YouTubeQuotaManager, storage: StorageManager):
        self.qm = quota_manager
        self.storage = storage

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
        raw_items = self.qm.execute(request, video_ids=video_ids) if part is None \
            else self.qm.execute(request, video_ids=video_ids, part=part)

        for item in raw_items:
            metadata = parse_video_metadata(item)
            stats = parse_video_stats(item)

            # TODO: Handle metadata already in DB
            self.storage.save_video_metadata(metadata)
            self.storage.save_video_stats(stats)

        return raw_items

    def get_channel_details(self, channel_ids: List[str], part: str = None):
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

        for item in raw_items:
            metadata = parse_channel_metadata(item)
            self.storage.save_channel_metadata(metadata)

        return raw_items
