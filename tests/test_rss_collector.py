import unittest
from unittest.mock import patch, MagicMock
from urllib.error import URLError

from collectors.common import clean_text
from collectors.rss_collector import build_event, fetch_new_entries
from schemas.post_schema import POST_FIELDS

class MockFeedEntry:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

class RssCollectorTests(unittest.TestCase):
    def test_clean_text_html(self):
        html_text = "<p>This is <b>bold</b> and \n newline</p>"
        self.assertEqual(clean_text(html_text), "This is bold and newline")
        self.assertEqual(clean_text(None), "")

    def test_build_event_valid(self):
        entry = MockFeedEntry(
            id="123",
            title="Test Title",
            summary="Test Summary",
            link="http://test.com",
            author="John Doe"
        )
        event = build_event(entry, "Test Feed")
        self.assertTrue(set(POST_FIELDS).issubset(set(event.keys())))
        self.assertEqual(event["id"], "123")
        self.assertEqual(event["title"], "Test Title")
        self.assertEqual(event["content"], "Test Summary")
        self.assertEqual(event["author"], "John Doe")
        self.assertEqual(event["feed_name"], "Test Feed")

    def test_build_event_missing_fields(self):
        # Entry without author or id
        entry = MockFeedEntry(
            title="Test Title",
            link="http://test.com"
        )
        event = build_event(entry, "Test Feed")
        self.assertIsNone(event["author"])
        self.assertEqual(event["id"], "http://test.com") # Falls back to link
        
    @patch("collectors.rss_collector.fetch_payload")
    @patch("collectors.rss_collector.feedparser.parse")
    def test_fetch_dedup(self, mock_parse, mock_fetch):
        # Setup mock feed with two duplicate entries
        mock_entry = MockFeedEntry(id="123", title="Test", link="http://test.com")
        mock_parse.return_value = MagicMock(entries=[mock_entry, mock_entry], bozo=0)
        
        seen_ids = set()
        # Should only return 1 event because the second is a duplicate
        with patch('collectors.rss_collector.RSS_FEEDS', [{"name": "Test", "url": "http://test.com"}]):
            events = fetch_new_entries(seen_ids)
            
        self.assertEqual(len(events), 1)
        self.assertIn("123", seen_ids)

    @patch("collectors.rss_collector.fetch_payload")
    def test_bad_feed_url(self, mock_fetch):
        # Simulate network error
        mock_fetch.side_effect = URLError("Network unreachable")
        
        seen_ids = set()
        with patch('collectors.rss_collector.RSS_FEEDS', [{"name": "Test", "url": "http://bad.com"}]):
            # Should not crash, should log error and return empty list
            events = fetch_new_entries(seen_ids)
            
        self.assertEqual(len(events), 0)

if __name__ == '__main__':
    unittest.main()
