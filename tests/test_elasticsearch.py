import unittest
from unittest.mock import MagicMock, patch
from pathlib import Path

from scripts.init_elasticsearch import init_elasticsearch

class ElasticsearchTests(unittest.TestCase):
    def test_init_elasticsearch_creates_index(self):
        mock_es = MagicMock()
        mock_es.indices.exists.return_value = False
        
        with patch("pathlib.Path.exists", return_value=True):
            with patch("builtins.open", unittest.mock.mock_open(read_data='{"mappings": {}}')):
                result = init_elasticsearch(mock_es, "test_index", Path("dummy.json"))
                
        self.assertTrue(result)
        mock_es.indices.create.assert_called_once()
        mock_es.indices.put_mapping.assert_not_called()

    def test_init_elasticsearch_updates_mapping(self):
        mock_es = MagicMock()
        mock_es.indices.exists.return_value = True
        
        with patch("pathlib.Path.exists", return_value=True):
            with patch("builtins.open", unittest.mock.mock_open(read_data='{"mappings": {}}')):
                result = init_elasticsearch(mock_es, "test_index", Path("dummy.json"))
                
        self.assertTrue(result)
        mock_es.indices.create.assert_not_called()
        mock_es.indices.put_mapping.assert_called_once()

if __name__ == "__main__":
    unittest.main()
