# app/db/mongo_client.py

from pymongo import MongoClient
from app.logger import logger


class MongoDBClient:
    """Handles MongoDB connections and queries."""

    def __init__(self, connection_string: str, database: str):
        self.connection_string = connection_string
        self.database_name = database
        try:
            self.client = MongoClient(self.connection_string)
            self.db = self.client[self.database_name]
            logger.info(f"Connected to MongoDB database: {self.database_name}")
        except Exception as e:
            logger.exception("Failed to connect to MongoDB.")
            raise e

    def fetch_one(self, collection: str, query: dict):
        """Fetch one document from a MongoDB collection."""
        try:
            col = self.db[collection]
            result = col.find_one(query)
            logger.info(f"Fetched document from {collection}: {query}")
            return result
        except Exception as e:
            logger.exception(f"MongoDB query failed: {query}")
            return None
