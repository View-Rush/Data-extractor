# utils/video_parser.py

from datetime import datetime

def parse_video_metadata(video: dict) -> dict:
    snippet = video.get("snippet", {})
    content_details = video.get("contentDetails", {})
    statistics = video.get("statistics", {})

    return {
        "id": video.get("id"),
        "publishedAt": snippet.get("publishedAt"),
        "channelId": snippet.get("channelId"),
        "title": snippet.get("title"),
        "description": snippet.get("description"),
        "localizedTitle": snippet.get("localized", {}).get("title"),
        "localizedDescription": snippet.get("localized", {}).get("description"),
        "thumbnailDefault": snippet.get("thumbnails", {}).get("default", {}).get("url"),
        "thumbnailMedium": snippet.get("thumbnails", {}).get("medium", {}).get("url"),
        "thumbnailHigh": snippet.get("thumbnails", {}).get("high", {}).get("url"),
        "tags": snippet.get("tags", []),
        "categoryId": snippet.get("categoryId"),
        "liveBroadcastContent": snippet.get("liveBroadcastContent"),
        "defaultLanguage": snippet.get("defaultLanguage"),
        "defaultAudioLanguage": snippet.get("defaultAudioLanguage"),
        "videoDuration": content_details.get("duration"),
        "viewCount": int(statistics.get("viewCount", 0)),
        "likesCount": int(statistics.get("likeCount", 0)),
        "favouriteCount": int(statistics.get("favoriteCount", 0)),
        "commentCount": int(statistics.get("commentCount", 0)),
    }

def parse_video_stats(video: dict) -> dict:
    statistics = video.get("statistics", {})
    return {
        "videoId": video.get("id"),
        "recordedAt": datetime.utcnow().isoformat(),
        "dayIndex": 0,  # You can update this later if you track time series
        "viewCount": int(statistics.get("viewCount", 0)),
        "likeCount": int(statistics.get("likeCount", 0)),
        "commentCount": int(statistics.get("commentCount", 0)),
    }
