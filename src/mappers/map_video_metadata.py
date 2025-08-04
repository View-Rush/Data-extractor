# src/mappers/map_video_metadata.py

from datetime import datetime


def map_video_metadata(video: dict) -> dict:
    snippet = video.get("snippet", {})
    statistics = video.get("statistics", {})
    content_details = video.get("contentDetails", {})

    thumbnails = snippet.get("thumbnails", {})

    return {
        "id": video.get("id"),
        "published_at": snippet.get("publishedAt"),
        "channel_id": snippet.get("channelId"),
        "title": snippet.get("title"),
        "description": snippet.get("description"),
        "localized_title": snippet.get("localized", {}).get("title"),
        "localized_description": snippet.get("localized", {}).get("description"),
        "thumbnail_default": thumbnails.get("default", {}).get("url"),
        "thumbnail_medium": thumbnails.get("medium", {}).get("url"),
        "thumbnail_high": thumbnails.get("high", {}).get("url"),
        "tags": snippet.get("tags", []),
        "category_id": snippet.get("categoryId"),
        "live_broadcast_content": snippet.get("liveBroadcastContent"),
        "default_language": snippet.get("defaultLanguage"),
        "default_audio_language": snippet.get("defaultAudioLanguage"),
        "video_duration": content_details.get("duration"),
        "view_count": int(statistics.get("viewCount", 0)) if statistics.get("viewCount") is not None else None,
        "likes_count": int(statistics.get("likeCount", 0)) if statistics.get("likeCount") is not None else None,
        "favourite_count": int(statistics.get("favoriteCount", 0)) if statistics.get(
            "favoriteCount") is not None else None,
        "comment_count": int(statistics.get("commentCount", 0)) if statistics.get("commentCount") is not None else None,
        "inserted_at": datetime.utcnow()
    }

def map_video_schedule_metadata(video_id: str, upload_datetime: datetime, current_sample: int = 0, bin_id: int = None) -> dict:
    return {
        "video_id": video_id,
        "upload_datetime": upload_datetime,
        "current_sample": current_sample,
        "bin_id": bin_id
    }
