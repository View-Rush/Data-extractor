# api/requests/get_channel_details.py

from googleapiclient.discovery import Resource
from src.api.youtube_api_request import YouTubeAPIRequest

class GetChannelDataByHandleOrId(YouTubeAPIRequest):
    """
    Retrieves metadata for one or more YouTube channels using either channel IDs
    or handles (e.g., "@veritasium").

    Args:
        service (Resource): Authorized YouTube API service.
        channel_ids (List[str]): List of channel IDs or handles.
        part (str): Comma-separated parts to include in the response.

    Returns:
        dict: Response from the YouTube API.
    """

    DEFAULT_PARTS = "id,snippet,contentDetails,statistics,topicDetails,status"


    def execute(self, service: Resource, channel_ids: list[str], part: str = DEFAULT_PARTS) -> tuple[list[dict], int]:
        request_count = 0
        handles = [c for c in channel_ids if c.startswith("@")]
        ids = [c for c in channel_ids if not c.startswith("@")]

        items = []

        # Handle channel IDs (can be batched)
        if ids:
            for i in range(0, len(ids), 50):  # YouTube API limit is 50 per request
                response = service.channels().list(
                    part=part,
                    id=",".join(ids[i:i + 50])
                ).execute()
                items.extend(response.get("items", []))
                request_count += 1

        # Handle handles (must be called one by one)
        if handles:
            for handle in handles:
                response = service.channels().list(
                    part=part,
                    forHandle=handle
                ).execute()
                items.extend(response.get("items", []))
                request_count += 1

        return items, request_count
