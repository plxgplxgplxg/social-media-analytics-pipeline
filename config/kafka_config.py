import os

from config.env import PROJECT_ROOT  # noqa: F401


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
RAW_POSTS_TOPIC = os.getenv("KAFKA_RAW_TOPIC", "raw_posts")
PROCESSED_POSTS_TOPIC = os.getenv("KAFKA_PROCESSED_TOPIC", "processed_posts")
AGGREGATED_METRICS_TOPIC = os.getenv("KAFKA_AGGREGATED_TOPIC", "aggregated_metrics")

TOPIC_SPECS = [
    {
        "name": RAW_POSTS_TOPIC,
        "partitions": 3,
        "replication_factor": 1,
        "config": {"retention.ms": str(7 * 24 * 60 * 60 * 1000)},
    },
    {
        "name": PROCESSED_POSTS_TOPIC,
        "partitions": 3,
        "replication_factor": 1,
        "config": {"retention.ms": str(3 * 24 * 60 * 60 * 1000)},
    },
    {
        "name": AGGREGATED_METRICS_TOPIC,
        "partitions": 1,
        "replication_factor": 1,
        "config": {"retention.ms": str(24 * 60 * 60 * 1000)},
    },
]
