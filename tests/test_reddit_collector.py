import os
import unittest
from unittest.mock import patch, MagicMock

from collectors.reddit_collector import build_event, fetch_new_posts, main
from schemas.post_schema import POST_FIELDS

class MockSubmission:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)
        if not hasattr(self, "selftext"):
            self.selftext = ""
        if not hasattr(self, "url"):
            self.url = ""
        if not hasattr(self, "author"):
            self.author = MagicMock(name="author")
            self.author.name = "TestAuthor"

class RedditCollectorTests(unittest.TestCase):
    def test_missing_credentials(self):
        # Temporarily clear env vars to test the missing credentials check
        with patch.dict(os.environ, clear=True):
            with self.assertRaises(RuntimeError) as context:
                main()
            self.assertIn("Missing Reddit credentials", str(context.exception))

    def test_build_event_structure(self):
        submission = MockSubmission(
            id="123",
            title="Test Title",
            selftext="Test Body",
            url="http://reddit.com/test",
            created_utc=1600000000.0
        )
        
        event = build_event(submission, "test_subreddit")
        self.assertTrue(set(POST_FIELDS).issubset(set(event.keys())))
        self.assertEqual(event["id"], "123")
        self.assertEqual(event["title"], "Test Title")
        self.assertEqual(event["content"], "Test Body")
        self.assertEqual(event["subreddit"], "test_subreddit")
        self.assertEqual(event["author"], "TestAuthor")

    @patch("collectors.reddit_collector.praw")
    def test_fetch_dedup(self, mock_praw):
        mock_client = MagicMock()
        mock_subreddit = MagicMock()
        mock_client.subreddit.return_value = mock_subreddit
        
        # Simulate reddit returning the same submission twice
        mock_submission = MockSubmission(
            id="123", title="Test", created_utc=1600000000.0
        )
        mock_subreddit.new.return_value = [mock_submission, mock_submission]
        
        seen_ids = set()
        with patch("collectors.reddit_collector.SUBREDDITS", ["test_subreddit"]):
            events = fetch_new_posts(mock_client, seen_ids)
            
        # Should only be 1 event because the second is deduplicated
        self.assertEqual(len(events), 1)
        self.assertIn("123", seen_ids)

if __name__ == '__main__':
    unittest.main()
