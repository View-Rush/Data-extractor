# YouTube Data Extractor

A modular and quota-aware data extractor for the YouTube Data API v3 that efficiently collects channel and video metadata with built-in quota management and database storage.

## Overview

This project provides a robust system for extracting YouTube data including channel metadata, video details, and statistics. It features intelligent quota management across multiple API keys, PostgreSQL/BigQuery storage backends, and automated scheduling for continuous data collection.

## Features

- **Quota-Aware API Management**: Intelligent rotation across multiple YouTube API keys to maximize data collection efficiency
- **Multi-Database Support**: PostgreSQL and BigQuery integration for flexible data storage
- **Automated Data Collection**: Scheduled scripts for fetching new uploads and updating video statistics
- **Channel Management**: Tools for populating and managing YouTube channels from lists
- **Video Tracking**: Comprehensive video metadata extraction including statistics, thumbnails, and scheduling data
- **Configurable Logging**: Flexible logging configuration with file and console output options
- **Error Handling**: Robust retry mechanisms and error recovery

## Project Structure

```
├── config.yaml                 # Main configuration file
├── requirements.txt            # Python dependencies
├── scripts/                    # Executable scripts
│   ├── init_db.py             # Database initialization
│   ├── fetch_new_uploads.py   # Fetch recent videos from channels
│   ├── populate_channels_from_a_list.py  # Bulk channel import
│   └── update_video_stats_hourly.py      # Hourly statistics updates
├── src/
│   ├── api/                   # YouTube API integration
│   │   ├── quota_manager.py   # API quota management
│   │   ├── youtube_client.py  # High-level YouTube API client
│   │   └── requests/          # Specific API request implementations
│   ├── db/                    # Database clients and schemas
│   │   ├── database_client.py # PostgreSQL client
│   │   ├── bigquery_client.py # BigQuery client
│   │   └── schemas/           # Database schemas
│   ├── mappers/               # Data transformation utilities
│   └── utils/                 # Logging and utility functions
```

## Prerequisites

- Python 3.8+
- PostgreSQL database (or BigQuery for cloud storage)
- YouTube Data API v3 keys
- Required Python packages (see `requirements.txt`)

## Setup

### 1. Environment Configuration

Create a `.env` file in the project root with the following variables:

```env
# YouTube API Configuration
YOUTUBE_API_KEYS=key1,key2,key3  # Comma-separated list of API keys

# Database Configuration (PostgreSQL)
DATABASE_HOST=your_host
DATABASE_PORT=5432
DATABASE_NAME=your_database
DATABASE_USER=your_username
DATABASE_PASSWORD=your_password
DATABASE_SSLROOTCERT=path/to/cert  # Optional for SSL

# Optional: BigQuery Configuration
GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account.json
BIGQUERY_PROJECT_ID=your_project_id
BIGQUERY_DATASET_ID=your_dataset_id

# Channel List File (for bulk import)
CHANNELS_FILE_NAME=channels.txt
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Database Setup

Initialize the database schema:

```bash
python scripts/init_db.py
```

To drop existing tables before creating new ones:

```bash
python scripts/init_db.py --drop
```

### 4. Configuration

Edit `config.yaml` to customize the application behavior:

```yaml
youtube:
  max_results_per_request: 50  # Results per API call
  lookback_days: 1             # Days to look back for new uploads

logging:
  level: INFO                  # DEBUG, INFO, WARNING, ERROR, CRITICAL
  enable_file_logging: false   # Enable file logging
  file_path: logs/app.log      # Log file path
```

## Usage

### Populate Channels

Add channels to the database from a file containing channel IDs (one per line):

```bash
python scripts/populate_channels_from_a_list.py
```

Create a text file (e.g., `channels.txt`) with YouTube channel IDs:
```
UCxxxxxxxxxxxxxxxxxxxxxxxx
UCyyyyyyyyyyyyyyyyyyyyyy
```

### Fetch New Uploads

Collect recent uploads from all active channels:

```bash
python scripts/fetch_new_uploads.py
```

This script:
- Fetches new videos from each channel's upload playlist
- Extracts comprehensive video metadata
- Stores data in the configured database
- Manages API quota efficiently

### Update Video Statistics

Update view counts, likes, and other statistics for existing videos:

```bash
python scripts/update_video_stats_hourly.py
```

## Data Model

### Channels Table
- Channel metadata (title, subscriber count, view count)
- Upload playlist tracking
- Activity status and last check timestamps

### Videos Table
- Complete video metadata (title, description, thumbnails)
- Statistics (views, likes, comments)
- Localization data and tags
- Publishing and insertion timestamps

### Video Schedule Table
- Upload scheduling metadata
- Sampling configuration for statistics collection

### Config Table
- Application configuration persistence
- Runtime settings management

## API Quota Management

The system includes intelligent quota management:

- **Multiple API Keys**: Automatically rotates between available API keys
- **Quota Tracking**: Monitors total quota usage across all operations
- **Retry Logic**: Handles rate limits and temporary failures
- **Cost Optimization**: Efficient batching of API requests

## Logging

Comprehensive logging with configurable levels:

- **Console Output**: Real-time operation status
- **File Logging**: Optional persistent log files
- **Structured Logging**: Consistent format across all components
- **Debug Mode**: Detailed API request/response logging

## Error Handling

Robust error handling includes:

- **API Failures**: Automatic retry with exponential backoff
- **Database Errors**: Transaction rollback and connection recovery
- **Invalid Data**: Graceful handling of malformed API responses
- **Quota Exhaustion**: Intelligent key rotation and scheduling

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For questions, issues, or contributions, please open an issue on the GitHub repository.
