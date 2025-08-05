import os
from dotenv import load_dotenv

from src.mappers.map_channel_metadata import map_channel_metadata
from src.api.youtube_client import YouTubeClient
from src.api.quota_manager import YouTubeQuotaManager
from src.db.supabase_client import insert_channel, fetch_all_channels
from src.utils.logger import setup_logger
import yaml


def load_config(path="config.yaml"):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, "..", path)
    config_path = os.path.abspath(config_path)

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at: {config_path}")

    with open(config_path, "r") as f:
        return yaml.safe_load(f)


config = load_config()
logger = setup_logger(__name__, config["logging"])


def main():
    load_dotenv()

    # Load API keys
    api_keys = os.getenv("YOUTUBE_API_KEYS", "").split(",")
    if not api_keys or not api_keys[0]:
        logger.error("Missing YOUTUBE_API_KEYS in .env file")
        return

    quota_manager = YouTubeQuotaManager(api_keys)
    yt_client = YouTubeClient(quota_manager)

    file_name = os.getenv("CHANNELS_FILE_NAME")

    channel_ids_initial = read_channels_list_from_file(file_name=file_name)

    existing_ids = [i[0] for i in fetch_all_channels()]
    channel_ids = list(filter(lambda cid: cid not in existing_ids, channel_ids_initial))

    logger.info(f"Fetching details for {len(channel_ids)} channels...")

    try:
        channel_data = yt_client.fetch_channel_details(channel_ids)
        if not channel_data:
            logger.warning("No data returned from API.")
            return

        logger.info("Mapping and inserting channels...")
        for channel in channel_data:
            mapped = map_channel_metadata(channel)
            insert_channel(mapped)
            logger.info(f"Inserted channel: {mapped['id']} - {mapped['title']}")

        logger.info("Done.")
    except Exception as e:
        logger.exception(f"Error occurred while fetching or inserting channels: {e}")


def read_channels_list_from_file(file_name="channels.txt"):
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    file_name = os.path.join(BASE_DIR, file_name)

    with open(file_name, "r") as file:
        lines = file.readlines()

    channel_ids = [line.strip() for line in lines if line.strip() != "channel_id"]
    return channel_ids


if __name__ == "__main__":
    main()
