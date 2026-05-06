import os

from config.env import PROJECT_ROOT  # noqa: F401


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_REGION = os.getenv("MINIO_REGION", "us-east-1")

RAW_POSTS_BUCKET = os.getenv("MINIO_RAW_BUCKET", "raw-posts")
CLEAN_POSTS_BUCKET = os.getenv("MINIO_CLEAN_BUCKET", "clean-posts")
AGGREGATES_BUCKET = os.getenv("MINIO_AGGREGATES_BUCKET", "aggregates")
CHECKPOINTS_BUCKET = os.getenv("MINIO_CHECKPOINTS_BUCKET", "checkpoints")
