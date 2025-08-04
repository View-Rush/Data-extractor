# src/db/supabase_client.py

import os
import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv

load_dotenv()  # Load .env if available

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
