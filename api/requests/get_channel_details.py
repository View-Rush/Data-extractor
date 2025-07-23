# api/requests/get_channel_details.py

from googleapiclient.discovery import Resource

from api.youtube_api_request import YouTubeAPIRequest


class GetChannelDataByHandleOrId(YouTubeAPIRequest):
    """
    Retrieves channel data from YouTube using either a handle (e.g. "@veritasium")
    or a channel ID (e.g. "UCXYZ...").

    Args:
        service (Resource): Authorized YouTube API service.
        identifier (str): Channel handle or channel ID.
        part (str): Comma-separated parts to include in the response.

    Returns:
        dict: Response from the YouTube API.
    """

    DEFAULT_PARTS = "id,snippet,contentDetails,statistics,topicDetails,status"

    def execute(self, service: Resource, identifier: str, part: str = DEFAULT_PARTS):
        if identifier.startswith("@"):
            return service.channels().list(
                part=part,
                forHandle=identifier
            ).execute()
        return service.channels().list(
            part=part,
            id=identifier
        ).execute()

    def get_quota(self):
        # TODO: Implement quota check
        return 1
