# api/requests/get_video_details.py

from googleapiclient.discovery import Resource
from src.api.youtube_api_request import YouTubeAPIRequest


class GetVideoDetails(YouTubeAPIRequest):
    """
    Retrieves video details from YouTube using one or more video IDs.

    Args:
        service (Resource): Authorized YouTube API service.
        video_ids (list[str] or str): One or more YouTube video IDs.
        part (str): Comma-separated parts to include in the response.

    Returns:
        list: List of video detail items from the YouTube API.
    """

    DEFAULT_PARTS = "id,snippet,contentDetails,statistics,status,topicDetails"
    MAX_IDS_PER_REQUEST = 50

    def execute(self, service: Resource, video_ids: list[str], part: str = DEFAULT_PARTS) -> tuple[list[dict], int]:
        # TODO: handle without errors
        if not isinstance(video_ids, list):
            raise TypeError("video_ids must be a list of strings")

        request_count = 0
        all_items = []

        for i in range(0, len(video_ids), self.MAX_IDS_PER_REQUEST):
            chunk = video_ids[i:i + self.MAX_IDS_PER_REQUEST]
            response = service.videos().list(
                part=part,
                id=",".join(chunk)
            ).execute()

            request_count += 1
            items = response.get("items", [])
            all_items.extend(items)

        return all_items, request_count
