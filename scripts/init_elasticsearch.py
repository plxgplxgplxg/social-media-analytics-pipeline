import json
import logging
import sys
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from elasticsearch import Elasticsearch
from config.elasticsearch_config import ELASTICSEARCH_HOST, POSTS_INDEX

logging.basicConfig(level=logging.INFO)

def init_elasticsearch(es_client: Elasticsearch, index_name: str, mapping_path: Path):
    if not mapping_path.exists():
        logging.error(f"Mapping file not found at {mapping_path}")
        return False
        
    with open(mapping_path, 'r', encoding='utf-8') as f:
        mapping = json.load(f)

    if not es_client.indices.exists(index=index_name):
        es_client.indices.create(index=index_name, body=mapping)
        logging.info(f"Created index '{index_name}' with mapping.")
    else:
        # We can try to put mapping, but some changes might require reindex
        try:
            es_client.indices.put_mapping(index=index_name, body=mapping["mappings"])
            logging.info(f"Updated mapping for existing index '{index_name}'.")
        except Exception as e:
            logging.warning(f"Could not update mapping for existing index: {e}")
    return True

def main():
    es = Elasticsearch([ELASTICSEARCH_HOST])
    mapping_file = PROJECT_ROOT / "schemas" / "elasticsearch_mappings.json"
    init_elasticsearch(es, POSTS_INDEX, mapping_file)

if __name__ == "__main__":
    main()
