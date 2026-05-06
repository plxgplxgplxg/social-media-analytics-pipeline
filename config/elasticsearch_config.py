import os

from config.env import PROJECT_ROOT  # noqa: F401


ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200")
POSTS_INDEX = os.getenv("ELASTICSEARCH_POSTS_INDEX", "posts")
