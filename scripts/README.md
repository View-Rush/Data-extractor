# Scripts Documentation

## fetch_new_uploads.py
Fetches new video uploads from YouTube channels within a configurable lookback period.
- **Purpose**: Discovers and stores new videos from monitored channels
- **Schedule**: Run daily or as needed
- **Dependencies**: YouTube API, database connection
- **Lookback**: Configurable via `config.yaml` (`youtube.lookback_days`)

## init_db.py
Initializes the database schema and sets up required tables.
- **Purpose**: Database setup and initialization
- **Usage**: Run once during project setup

## populate_channels_from_a_list.py
Populates the database with channel data from a CSV file.
- **Purpose**: Bulk import channel information
- **Input**: `data/channel_ids.csv`

## update_video_stats_hourly.py
This script periodically updates statistics (views, likes, comments, etc.) for all tracked YouTube videos, enabling time-series analysis of video performance. It operates on an hourly schedule, processing videos that were uploaded in the current hour bin (i.e., videos whose upload hour matches the current UTC hour). The script fetches video IDs in batches using pagination, retrieves the latest statistics for each video from the YouTube API, and updates both the local database and a BigQuery table with the new stats.

DayIndex: which day the entry is related to,
- 0 indexed.
- 0 is day 1, 1 is day 2, etc.
