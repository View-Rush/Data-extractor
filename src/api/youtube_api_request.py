# api/youtube_api_request.py

from abc import ABC, abstractmethod
from googleapiclient.discovery import Resource


class YouTubeAPIRequest(ABC):
    @abstractmethod
    def execute(self, service: Resource, *args, **kwargs):
        pass

    @abstractmethod
    def get_quota(self):
        pass
