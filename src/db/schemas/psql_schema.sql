-- Channels Table
CREATE TABLE IF NOT EXISTS channels (
    id TEXT PRIMARY KEY,
    title TEXT,
    custom_url TEXT,
    country TEXT,
    uploads_playlist_id TEXT,
    view_count BIGINT,
    subscriber_count BIGINT,
    last_checked_at TIMESTAMP,
    last_uploaded_at TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Videos Table
CREATE TABLE IF NOT EXISTS videos (
    id TEXT PRIMARY KEY,
    published_at TIMESTAMP,
    channel_id TEXT REFERENCES channels(id),
    title TEXT,
    description TEXT,
    localized_title TEXT,
    localized_description TEXT,
    thumbnail_default TEXT,
    thumbnail_medium TEXT,
    thumbnail_high TEXT,
    tags TEXT[], -- PostgreSQL supports array types
    category_id TEXT,
    live_broadcast_content TEXT,
    default_language TEXT,
    default_audio_language TEXT,
    video_duration TEXT,
    view_count BIGINT,
    likes_count BIGINT,
    favourite_count BIGINT,
    comment_count BIGINT,
    inserted_at TIMESTAMP DEFAULT NOW()
);

-- Video Scheduling Metadata Table
CREATE TABLE IF NOT EXISTS video_schedule (
    video_id TEXT PRIMARY KEY REFERENCES videos(id),
    upload_datetime TIMESTAMP,
    current_sample INT DEFAULT 0, -- Day index (1–30)
    bin_id INT -- Used for bin-based sampling rotation
);

-- Config Table (singleton config)
CREATE TABLE IF NOT EXISTS config (
    key TEXT PRIMARY KEY,
    value TEXT
);

-- Insert default config if needed
INSERT INTO config (key, value)
VALUES ('current_bin_id', '0')
ON CONFLICT (key) DO NOTHING;
