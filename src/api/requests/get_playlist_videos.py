# api/requests/get_playlist_videos.py
from datetime import datetime, timezone

from googleapiclient.discovery import Resource
from src.api.youtube_api_request import YouTubeAPIRequest


class GetPlaylistVideos(YouTubeAPIRequest):
    """
    Retrieves the most recent videos from a YouTube playlist,
    using either a playlist ID or a channel ID (which maps to its uploads playlist).

    Args:
        service (Resource): Authorized YouTube API service.
        identifier (str): Playlist ID or Channel ID.
        max_results (int): Number of videos to retrieve (default 50).

    Returns:
        list: List of video items (dicts).
    """
    requests_count = 0

    def execute(self, service: Resource, identifier: str, max_results: int = 50, since_datetime: datetime = None):
        playlist_id = identifier
        if identifier.startswith("UC"):
            channel_response = service.channels().list(
                part="contentDetails",
                id=identifier
            ).execute()
            playlist_id = channel_response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

        self.requests_count += 1
        videos = []
        next_page_token = None

        while len(videos) < max_results:
            remaining = max_results - len(videos)
            response = service.playlistItems().list(
                part="id,snippet,contentDetails",
                playlistId=playlist_id,
                maxResults=min(50, remaining),
                pageToken=next_page_token
            ).execute()

            page_items = response["items"]

            if since_datetime:
                page_items = [
                    item for item in page_items
                    if "videoPublishedAt" in item["contentDetails"]
                       and datetime.strptime(item["contentDetails"]["videoPublishedAt"], "%Y-%m-%dT%H:%M:%SZ")
                       .replace(tzinfo=timezone.utc) > since_datetime
                ]

            videos.extend(page_items)
            self.requests_count += 1

            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break

        return videos

    def get_quota(self):
        # playlistItems.list costs 1 unit per request
        return self.requests_count
