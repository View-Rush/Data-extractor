# src/mappers/map_channel_metadata.py

from datetime import datetime


def map_channel_metadata(channel: dict) -> dict:
    snippet = channel.get("snippet", {})
    statistics = channel.get("statistics", {})
    content_details = channel.get("contentDetails", {})

    return {
        "id": channel.get("id"),
        "title": snippet.get("title"),
        "custom_url": snippet.get("customUrl"),
        "country": snippet.get("country"),
        "uploads_playlist_id": content_details.get("relatedPlaylists", {}).get("uploads"),
        "view_count": int(statistics.get("viewCount", 0)) if statistics.get("viewCount") is not None else None,
        "subscriber_count": int(statistics.get("subscriberCount", 0)) if statistics.get("subscriberCount") is not None else None,
        "last_checked_at": datetime.utcnow(),  # or your own polling timestamp
        "is_active": True
    }
