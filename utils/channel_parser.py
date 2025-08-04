def parse_channel_metadata(channel: dict) -> dict:
    snippet = channel.get("snippet", {})
    statistics = channel.get("statistics", {})
    content_details = channel.get("contentDetails", {})

    return {
        "id": channel.get("id"),
        "title": snippet.get("title"),
        "customUrl": snippet.get("customUrl"),
        "country": snippet.get("country"),
        "uploadsPlaylistId": content_details.get("relatedPlaylists", {}).get("uploads"),
        "viewCount": int(statistics.get("viewCount", 0)),
        "subscriberCount": int(statistics.get("subscriberCount", 0)),
    }
