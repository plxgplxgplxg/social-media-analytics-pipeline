import json
import unittest
from pathlib import Path
from tempfile import NamedTemporaryFile

from collectors.historical_replay_producer import load_records, normalize_event
from schemas.post_schema import POST_FIELDS

class PipelineTests(unittest.TestCase):
    def test_normalize_event_defaults(self) -> None:
        event = normalize_event({"id": "1", "title": "hello"})
        self.assertTrue(set(POST_FIELDS).issubset(set(event)))
        self.assertEqual(event["id"], "1")
        self.assertEqual(event["title"], "hello")
        self.assertEqual(event["source"], "historical")
        self.assertIsNotNone(event["ingested_at"])

    def test_load_json_file(self) -> None:
        data = [{"id": "1", "title": "post1"}, {"id": "2", "title": "post2"}]
        with NamedTemporaryFile(suffix=".json", delete=False, mode="w", encoding="utf-8") as f:
            json.dump(data, f)
            temp_path = Path(f.name)
            
        try:
            records = load_records(temp_path)
            self.assertEqual(len(records), 2)
            self.assertEqual(records[0]["id"], "1")
            self.assertEqual(records[1]["title"], "post2")
            self.assertEqual(records[0]["source"], "historical")
        finally:
            temp_path.unlink()

    def test_load_csv_file(self) -> None:
        csv_content = "id,title\n1,post1\n2,post2\n"
        with NamedTemporaryFile(suffix=".csv", delete=False, mode="w", encoding="utf-8") as f:
            f.write(csv_content)
            temp_path = Path(f.name)
            
        try:
            records = load_records(temp_path)
            self.assertEqual(len(records), 2)
            self.assertEqual(records[0]["id"], "1")
            self.assertEqual(records[1]["title"], "post2")
            self.assertEqual(records[0]["source"], "historical")
        finally:
            temp_path.unlink()

    def test_unsupported_format(self) -> None:
        with NamedTemporaryFile(suffix=".txt", delete=False, mode="w", encoding="utf-8") as f:
            f.write("hello")
            temp_path = Path(f.name)
            
        try:
            with self.assertRaises(ValueError):
                load_records(temp_path)
        finally:
            temp_path.unlink()

if __name__ == "__main__":
    unittest.main()
