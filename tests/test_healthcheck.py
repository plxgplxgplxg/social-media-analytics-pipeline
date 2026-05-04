import json
import unittest
from unittest.mock import patch, MagicMock

from scripts.healthcheck import check_kafka, check_mongodb, check_elasticsearch, check_minio, main

class HealthcheckTests(unittest.TestCase):
    @patch("scripts.healthcheck.KafkaAdminClient")
    def test_check_kafka_success(self, mock_kafka):
        mock_admin = MagicMock()
        mock_kafka.return_value = mock_admin
        self.assertTrue(check_kafka())
        mock_admin.list_topics.assert_called_once()

    @patch("scripts.healthcheck.MongoClient")
    def test_check_mongodb_error(self, mock_mongo):
        mock_mongo.side_effect = Exception("Connection refused")
        self.assertFalse(check_mongodb())

    @patch("scripts.healthcheck.Elasticsearch")
    def test_check_elasticsearch_success(self, mock_es):
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_es.return_value = mock_client
        self.assertTrue(check_elasticsearch())

    @patch("scripts.healthcheck.urllib.request.urlopen")
    def test_check_minio_error(self, mock_urlopen):
        mock_urlopen.side_effect = Exception("Timeout")
        self.assertFalse(check_minio())

    @patch("scripts.healthcheck.sys.exit")
    @patch("scripts.healthcheck.check_kafka", return_value=True)
    @patch("scripts.healthcheck.check_mongodb", return_value=True)
    @patch("scripts.healthcheck.check_elasticsearch", return_value=False)
    @patch("scripts.healthcheck.check_minio", return_value=True)
    def test_main_exits_with_error_code(self, mock_minio, mock_es, mock_mongo, mock_kafka, mock_exit):
        main()
        mock_exit.assert_called_with(1)

if __name__ == "__main__":
    unittest.main()
