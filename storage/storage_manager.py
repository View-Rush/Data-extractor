# storage/storage_manager.py

from abc import ABC, abstractmethod

class StorageManager(ABC):
    # Video-related methods
    @abstractmethod
    def save_video_metadata(self, video_data: dict):
        pass

    @abstractmethod
    def save_video_stats(self, stats_data: dict):
        pass

    @abstractmethod
    def get_video_details_by_id(self, video_id: str) -> dict:
        pass

    @abstractmethod
    def get_all_videos(self) -> list[dict]:
        pass

    # Channel-related methods
    @abstractmethod
    def save_channel_metadata(self, channel_data: dict):
        pass

    @abstractmethod
    def get_channel_by_id(self, channel_id: str) -> dict:
        pass

    @abstractmethod
    def get_all_channels(self) -> list[dict]:
        pass
