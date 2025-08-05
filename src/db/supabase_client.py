# src/db/supabase_client.py

import os
import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv

load_dotenv()

# Environment variables
DB_HOST = os.getenv("SUPABASE_HOST")
DB_PORT = os.getenv("SUPABASE_PORT", "5432")
DB_NAME = os.getenv("SUPABASE_DB")
DB_USER = os.getenv("SUPABASE_USER")
DB_PASSWORD = os.getenv("SUPABASE_PASSWORD")

# Initialize connection pool
try:
    conn_pool = psycopg2.pool.SimpleConnectionPool(
        1, 10,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME
    )
except Exception as e:
    raise RuntimeError(f"Unable to connect to Supabase DB: {e}")


def get_conn():
    try:
        return conn_pool.getconn()
    except Exception as e:
        raise RuntimeError(f"Error acquiring DB connection: {e}")


def release_conn(conn):
    try:
        conn_pool.putconn(conn)
    except Exception as e:
        raise RuntimeError(f"Error releasing DB connection: {e}")


def execute_query(query: str, params: tuple = None, fetch: bool = False):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            result = cur.fetchall() if fetch else None
            conn.commit()
            return result
    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"Database query failed: {e}")
    finally:
        release_conn(conn)


def insert_video(video_id, title, publish_time):
    query = """
    INSERT INTO videos (video_id, title, publish_time)
    VALUES (%s, %s, %s)
    ON CONFLICT (video_id) DO NOTHING;
    """
    execute_query(query, (video_id, title, publish_time))


def fetch_unpolled_channels(limit=100):
    query = """
    SELECT channel_id FROM channels
    WHERE last_polled IS NULL OR last_polled < NOW() - INTERVAL '6 hours'
    LIMIT %s;
    """
    return execute_query(query, (limit,), fetch=True)


def insert_channel(channel: dict):
    query = """
    INSERT INTO channels (
        id,
        title,
        custom_url,
        country,
        uploads_playlist_id,
        view_count,
        subscriber_count,
        last_checked_at,
        is_active
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO UPDATE
    SET
        title = EXCLUDED.title,
        custom_url = EXCLUDED.custom_url,
        country = EXCLUDED.country,
        uploads_playlist_id = EXCLUDED.uploads_playlist_id,
        view_count = EXCLUDED.view_count,
        subscriber_count = EXCLUDED.subscriber_count,
        last_checked_at = EXCLUDED.last_checked_at,
        is_active = EXCLUDED.is_active;
    """

    params = (
        channel["id"],
        channel["title"],
        channel["custom_url"],
        channel["country"],
        channel["uploads_playlist_id"],
        channel["view_count"],
        channel["subscriber_count"],
        channel["last_checked_at"],
        channel["is_active"],
    )

    execute_query(query, params)

def fetch_channels_batch(limit=100, offset=0):
    query = """
            SELECT id
            FROM channels
            WHERE is_active = TRUE
            LIMIT %s OFFSET %s; \
            """
    return execute_query(query, (limit, offset), fetch=True)

def fetch_channel_upload_playlist_ids_batch(limit=100, offset=0):
    query = """
            SELECT uploads_playlist_id
            FROM channels
            WHERE is_active = TRUE
            LIMIT %s OFFSET %s; \
            """
    return execute_query(query, (limit, offset), fetch=True)

def fetch_all_channels(batch_size=100):
    all_channels = []
    offset = 0

    while True:
        batch = fetch_channels_batch(limit=batch_size, offset=offset)
        if not batch:
            break
        all_channels.extend(batch)
        offset += batch_size

    return all_channels

def mark_channel_inactive(channel_id: str):
    query = """
    UPDATE channels
    SET is_active = FALSE
    WHERE uploads_playlist_id = %s;
    """
    execute_query(query, (channel_id,))

def insert_video(
    id,
    published_at,
    channel_id,
    title,
    description,
    localized_title,
    localized_description,
    thumbnail_default,
    thumbnail_medium,
    thumbnail_high,
    tags,
    category_id,
    live_broadcast_content,
    default_language,
    default_audio_language,
    video_duration,
    view_count,
    likes_count,
    favourite_count,
    comment_count,
    inserted_at
):
    query = """
    INSERT INTO videos (
        id,
        published_at,
        channel_id,
        title,
        description,
        localized_title,
        localized_description,
        thumbnail_default,
        thumbnail_medium,
        thumbnail_high,
        tags,
        category_id,
        live_broadcast_content,
        default_language,
        default_audio_language,
        video_duration,
        view_count,
        likes_count,
        favourite_count,
        comment_count,
        inserted_at
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO NOTHING;
    """

    params = (
        id,
        published_at,
        channel_id,
        title,
        description,
        localized_title,
        localized_description,
        thumbnail_default,
        thumbnail_medium,
        thumbnail_high,
        tags,
        category_id,
        live_broadcast_content,
        default_language,
        default_audio_language,
        video_duration,
        view_count,
        likes_count,
        favourite_count,
        comment_count,
        inserted_at
    )

    execute_query(query, params)
