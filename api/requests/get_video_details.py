# api/requests/get_video_details.py

from googleapiclient.discovery import Resource
from api.youtube_api_request import YouTubeAPIRequest


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
    request_count = 0

    def execute(self, service: Resource, video_ids, part: str = DEFAULT_PARTS):
        if isinstance(video_ids, str):
            video_ids = [video_ids]

        all_items = []

        for i in range(0, len(video_ids), self.MAX_IDS_PER_REQUEST):
            chunk = video_ids[i:i + self.MAX_IDS_PER_REQUEST]
            response = service.videos().list(
                part=part,
                id=",".join(chunk)
            ).execute()
            self.request_count += 1
            items = response.get("items", [])
            all_items.extend(items)

        return all_items

    def get_quota(self):
        # Each request costs 1 unit per 'part'
        # TODO: does this scale with multiple parts?
        return self.request_count
