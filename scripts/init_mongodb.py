import logging
import sys
from pathlib import Path
from datetime import timedelta
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pymongo import MongoClient
import pymongo
from config.mongo_config import (
    MONGO_DATABASE,
    MONGO_METRICS_TTL_DAYS,
    MONGO_POSTS_TTL_DAYS,
    MONGO_URI,
    POSTS_COLLECTION,
    SENTIMENT_COLLECTION,
    TRENDING_COLLECTION,
)

logging.basicConfig(level=logging.INFO)

def init_mongodb(client: MongoClient, db_name: str):
    db = client[db_name]
    posts_ttl_seconds = int(timedelta(days=MONGO_POSTS_TTL_DAYS).total_seconds())
    metrics_ttl_seconds = int(
        timedelta(days=MONGO_METRICS_TTL_DAYS).total_seconds()
    )
    
    # 1. Posts Collection Indexes
    posts = db[POSTS_COLLECTION]
    try:
        posts.create_index([("id", pymongo.ASCENDING)], unique=True)
    except pymongo.errors.DuplicateKeyError as e:
        logging.warning(f"Could not create unique index on 'id' due to existing duplicates: {e}")
    posts.create_index([("published_at", pymongo.DESCENDING)])
    posts.create_index([("sentiment", pymongo.ASCENDING)])
    posts.create_index(
        [("ingested_at", pymongo.ASCENDING)],
        expireAfterSeconds=posts_ttl_seconds,
    )
    logging.info(f"Created indexes for {POSTS_COLLECTION}")

    # 2. Sentiment Metrics Indexes
    sentiment = db[SENTIMENT_COLLECTION]
    sentiment.create_index([("window_start", pymongo.DESCENDING)])
    sentiment.create_index(
        [("window_end", pymongo.ASCENDING)],
        expireAfterSeconds=metrics_ttl_seconds,
    )
    logging.info(f"Created indexes for {SENTIMENT_COLLECTION}")

    # 3. Trending Topics Indexes
    trending = db[TRENDING_COLLECTION]
    trending.create_index([("window_start", pymongo.DESCENDING)])
    trending.create_index(
        [
            ("window_start", pymongo.ASCENDING),
            ("window_end", pymongo.ASCENDING),
            ("keyword", pymongo.ASCENDING),
        ],
        unique=True,
    )
    trending.create_index(
        [("window_end", pymongo.ASCENDING)],
        expireAfterSeconds=metrics_ttl_seconds,
    )
    logging.info(f"Created indexes for {TRENDING_COLLECTION}")

def main():
    client = MongoClient(MONGO_URI)
    init_mongodb(client, MONGO_DATABASE)
    logging.info("MongoDB initialization completed.")

if __name__ == "__main__":
    main()
