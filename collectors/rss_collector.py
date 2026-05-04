import logging
import os
import sys
import time
import uuid
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import feedparser
from kafka.errors import NoBrokersAvailable

from collectors.common import (
    clean_text,
    create_producer,
    fetch_payload,
    now_iso8601,
    publish_events,
    to_iso8601,
)
from config.kafka_config import KAFKA_BOOTSTRAP_SERVERS, RAW_POSTS_TOPIC


FETCH_INTERVAL_SECONDS = int(os.getenv("RSS_FETCH_INTERVAL_SECONDS", "60"))
RSS_FEEDS = [
    {"name": "VnExpress", "url": "https://vnexpress.net/rss/tin-moi-nhat.rss"},
    {"name": "Tuoi Tre", "url": "https://tuoitre.vn/rss/tin-moi-nhat.rss"},
    {"name": "BBC Vietnamese", "url": "https://feeds.bbci.co.uk/vietnamese/rss.xml"},
]


def build_event(entry: feedparser.FeedParserDict, feed_name: str) -> dict:
    content_blocks = getattr(entry, "content", None) or [{}]
    content_value = content_blocks[0].get("value", "")
    return {
        "id": getattr(entry, "id", None) or getattr(entry, "link", None) or str(uuid.uuid4()),
        "source": "rss",
        "title": clean_text(getattr(entry, "title", "")),
        "content": clean_text(getattr(entry, "summary", "") or content_value),
        "url": getattr(entry, "link", ""),
        "author": getattr(entry, "author", None),
        "published_at": to_iso8601(getattr(entry, "published_parsed", None)),
        "subreddit": None,
        "feed_name": feed_name,
        "ingested_at": now_iso8601(),
    }


def fetch_new_entries(seen_ids: set[str]) -> list[dict]:
    events = []
    max_retries = 3

    for feed in RSS_FEEDS:
        for attempt in range(max_retries):
            try:
                payload = fetch_payload(feed["url"])
                parsed = feedparser.parse(payload)
                
                if getattr(parsed, "bozo", 0) and isinstance(parsed.bozo_exception, Exception):
                    logging.warning("Failed to parse feed %s: %s", feed["name"], parsed.bozo_exception)
                    break
                    
                added_count = 0
                for entry in parsed.entries:
                    dedup_key = getattr(entry, "id", None) or getattr(entry, "link", None)
                    if dedup_key and dedup_key in seen_ids:
                        continue
                    if dedup_key:
                        seen_ids.add(dedup_key)
                    events.append(build_event(entry, feed["name"]))
                    added_count += 1
                
                logging.info("Feed %s: Fetched %d new posts", feed["name"], added_count)
                break
            except Exception as e:
                logging.warning("Error fetching feed %s (attempt %d/%d): %s", feed["name"], attempt + 1, max_retries, e)
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    logging.error("Failed to fetch feed %s after %d retries", feed["name"], max_retries)
                    
    return events


def main() -> None:
    try:
        producer = create_producer()
    except NoBrokersAvailable:
        logging.error("Cannot connect to Kafka at %s", KAFKA_BOOTSTRAP_SERVERS)
        return

    seen_ids: set[str] = set()
    logging.info("Publishing RSS posts to Kafka topic %s", RAW_POSTS_TOPIC)

    try:
        while True:
            events = fetch_new_entries(seen_ids)
            published = publish_events(producer, events)
            logging.info("Fetched and published %s RSS posts", published)
            time.sleep(FETCH_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logging.info("Stopping RSS collector")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
