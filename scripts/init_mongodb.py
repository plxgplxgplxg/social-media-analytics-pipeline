import logging
import sys
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pymongo import MongoClient
import pymongo
from config.mongo_config import MONGO_URI, MONGO_DATABASE, POSTS_COLLECTION, SENTIMENT_COLLECTION, TRENDING_COLLECTION

logging.basicConfig(level=logging.INFO)

def init_mongodb(client: MongoClient, db_name: str):
    db = client[db_name]
    
    # 1. Posts Collection Indexes
    posts = db[POSTS_COLLECTION]
    try:
        posts.create_index([("id", pymongo.ASCENDING)], unique=True)
    except pymongo.errors.DuplicateKeyError as e:
        logging.warning(f"Could not create unique index on 'id' due to existing duplicates: {e}")
    posts.create_index([("published_at", pymongo.DESCENDING)])
    posts.create_index([("sentiment", pymongo.ASCENDING)])
    logging.info(f"Created indexes for {POSTS_COLLECTION}")

    # 2. Sentiment Metrics Indexes
    sentiment = db[SENTIMENT_COLLECTION]
    sentiment.create_index([("window_start", pymongo.DESCENDING)])
    logging.info(f"Created indexes for {SENTIMENT_COLLECTION}")

    # 3. Trending Topics Indexes
    trending = db[TRENDING_COLLECTION]
    trending.create_index([("window_start", pymongo.DESCENDING)])
    logging.info(f"Created indexes for {TRENDING_COLLECTION}")

def main():
    client = MongoClient(MONGO_URI)
    init_mongodb(client, MONGO_DATABASE)
    logging.info("MongoDB initialization completed.")

if __name__ == "__main__":
    main()
