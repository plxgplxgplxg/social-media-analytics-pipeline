import os

from config.env import PROJECT_ROOT  # noqa: F401


MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "analytics")
POSTS_COLLECTION = os.getenv("MONGO_POSTS_COLLECTION", "posts")
SENTIMENT_COLLECTION = os.getenv(
    "MONGO_SENTIMENT_COLLECTION", "sentiment_metrics"
)
TRENDING_COLLECTION = os.getenv(
    "MONGO_TRENDING_COLLECTION", "trending_topics"
)
MONGO_POSTS_TTL_DAYS = int(os.getenv("MONGO_POSTS_TTL_DAYS", "7"))
MONGO_METRICS_TTL_DAYS = int(os.getenv("MONGO_METRICS_TTL_DAYS", "30"))
