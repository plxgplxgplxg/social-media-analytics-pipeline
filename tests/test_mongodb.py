import unittest
from unittest.mock import MagicMock
import pymongo

from scripts.init_mongodb import init_mongodb
from config.mongo_config import POSTS_COLLECTION, SENTIMENT_COLLECTION, TRENDING_COLLECTION

class MongoDBTests(unittest.TestCase):
    def test_init_mongodb_creates_indexes(self):
        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_client.__getitem__.return_value = mock_db
        
        mock_posts = MagicMock()
        mock_sentiment = MagicMock()
        mock_trending = MagicMock()
        
        def mock_get_collection(name):
            if name == POSTS_COLLECTION: return mock_posts
            if name == SENTIMENT_COLLECTION: return mock_sentiment
            if name == TRENDING_COLLECTION: return mock_trending
            return MagicMock()
            
        mock_db.__getitem__.side_effect = mock_get_collection
        
        init_mongodb(mock_client, "test_db")
        
        self.assertEqual(mock_posts.create_index.call_count, 4)
        self.assertEqual(mock_sentiment.create_index.call_count, 2)
        self.assertEqual(mock_trending.create_index.call_count, 3)

if __name__ == "__main__":
    unittest.main()
