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

    MAX_RESULTS_PER_PAGE = 50

    def execute(self, service: Resource, identifier: str, since_datetime: datetime = None) \
            -> tuple[list[dict], int]:
        requests_count = 0
        video_ids = []
        next_page_token = None

        # handle channel IDs
        playlist_id = identifier
        if identifier.startswith("UC"):
            channel_response = service.channels().list(
                part="contentDetails",
                id=identifier
            ).execute()
            playlist_id = channel_response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
            requests_count += 1

        while True:
            response = service.playlistItems().list(
                part="contentDetails",
                playlistId=playlist_id,
                maxResults=self.MAX_RESULTS_PER_PAGE,
                pageToken=next_page_token
            ).execute()

            requests_count += 1
            items = response["items"]

            for item in items:
                content = item["contentDetails"]
                published_at_str = content.get("videoPublishedAt")

                if not published_at_str:
                    continue  # skip malformed entries

                published_at = datetime.strptime(published_at_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

                # Stop if this video is older than since_datetime
                if since_datetime and published_at <= since_datetime:
                    return video_ids, requests_count

                video_ids.append(content["videoId"])

            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break

        return video_ids, requests_count
