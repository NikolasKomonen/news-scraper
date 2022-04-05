from pymongo.mongo_client import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection

NEWS_SCRAPER_DATABASE = "news_scraper"


class Mongo:
    def __init__(self, host_url: str | None = None) -> None:
        self.client = MongoClient(host_url, connect=True)

    def get_database(self, name: str):
        return self.client.get_database(name)
