# src/db/init_db.py

import os
import psycopg2
import argparse
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
    return psycopg2.connect(
        dbname=os.getenv("DATABASE_NAME"),
        user=os.getenv("DATABASE_USER"),
        password=os.getenv("DATABASE_PASSWORD"),
        host=os.getenv("DATABASE_HOST"),
        port=os.getenv("DATABASE_PORT", "5432"),
        sslmode='verify-full',
        sslrootcert=os.getenv("DATABASE_SSLROOTCERT")
    )

def execute_sql_file(path, drop=False):
    with open(path, "r") as f:
        raw_sql = f.read()

    if drop:
        print("WARNING: Dropping tables not implemented. You must drop manually.")

    conn = get_db_connection()
    with conn:
        with conn.cursor() as cur:
            cur.execute(raw_sql)
    print("Schema applied successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize Supabase schema.")
    parser.add_argument("--drop", action="store_true", help="(Optional) Drop tables before creating.")
    args = parser.parse_args()

    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    schema_file = os.path.join(BASE_DIR, "src/db/schemas", "psql_schema.sql")

    if not os.path.exists(schema_file):
        raise FileNotFoundError(f"Schema file not found: {schema_file}")

    execute_sql_file(schema_file, drop=args.drop)
