# storage/storage_manager.py

from abc import ABC, abstractmethod

class StorageManager(ABC):
    @abstractmethod
    def save_video_metadata(self, video_data: dict):
        pass

    @abstractmethod
    def save_video_stats(self, stats_data: dict):
        pass

    @abstractmethod
    def get_video_by_id(self, video_id: str) -> dict:
        pass

    @abstractmethod
    def get_all_videos(self) -> list[dict]:
        pass
