# api/requests/get_playlist_videos.py

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

    def execute(self, service: Resource, identifier: str, max_results: int = 50):
        # If it's a channel ID, get its uploads playlist
        playlist_id = identifier
        if identifier.startswith("UC"):  # Channel ID
            channel_response = service.channels().list(
                part="contentDetails",
                id=identifier
            ).execute()

            uploads = channel_response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
            playlist_id = uploads

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

            videos.extend(response["items"])
            self.requests_count += 1

            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break

        return videos

    def get_quota(self):
        # playlistItems.list costs 1 unit per request
        return self.requests_count

    # TODO: filter by last 24hrs (or any other criteria)
