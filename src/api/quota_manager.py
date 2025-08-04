# api/quota_manager.py

from googleapiclient.discovery import build
from time import sleep
from src.api.youtube_api_request import YouTubeAPIRequest

class YouTubeQuotaManager:
    def __init__(self, api_keys: list[str], max_retries: int = 3):
        self.api_keys = api_keys
        self.max_retries = max_retries
        self.clients = [build("youtube", "v3", developerKey=key) for key in api_keys]
        self.current_index = 0

    def _get_next_client(self):
        self.current_index = (self.current_index + 1) % len(self.clients)
        return self.clients[self.current_index]

    def _get_client(self):
        return self.clients[self.current_index]

    def execute(self, request_obj: YouTubeAPIRequest, *args, **kwargs):
        retries = 0
        while retries < self.max_retries:
            try:
                client = self._get_client()
                result = request_obj.execute(client, *args, **kwargs)
                return result
            except Exception as e:
                print(f"[QuotaManager] Error (key {self.current_index}): {e}")
                retries += 1
                self._get_next_client()
                sleep(1)
        raise Exception("All API keys failed or quota exceeded.")

    def get_active_key(self):
        return self.api_keys[self.current_index]

    # TODO: adding more keys
    # TODO: Keeping track of quota usage
    # TODO: Connect to a database