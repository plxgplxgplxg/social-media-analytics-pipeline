import json
import logging
import sys
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pymongo import MongoClient
from elasticsearch import Elasticsearch
from kafka import KafkaAdminClient
import urllib.request
import urllib.error

from config.kafka_config import KAFKA_BOOTSTRAP_SERVERS
from config.mongo_config import MONGO_URI
from config.elasticsearch_config import ELASTICSEARCH_HOST
from config.minio_config import MINIO_ENDPOINT

def check_kafka() -> bool:
    try:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, request_timeout_ms=3000)
        admin.list_topics()
        admin.close()
        return True
    except Exception:
        return False

def check_mongodb() -> bool:
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        client.admin.command('ping')
        client.close()
        return True
    except Exception:
        return False

def check_elasticsearch() -> bool:
    try:
        es = Elasticsearch([ELASTICSEARCH_HOST], request_timeout=3)
        return es.ping()
    except Exception:
        return False

def check_minio() -> bool:
    try:
        req = urllib.request.Request(f"{MINIO_ENDPOINT}/minio/health/live")
        with urllib.request.urlopen(req, timeout=3) as response:
            return response.status == 200
    except Exception:
        return False

def main():
    status = {
        "kafka": "ok" if check_kafka() else "error",
        "mongodb": "ok" if check_mongodb() else "error",
        "elasticsearch": "ok" if check_elasticsearch() else "error",
        "minio": "ok" if check_minio() else "error"
    }
    
    print(json.dumps(status, indent=2))
    
    if any(v == "error" for v in status.values()):
        sys.exit(1)
    else:
        sys.exit(0)

if __name__ == "__main__":
    main()
