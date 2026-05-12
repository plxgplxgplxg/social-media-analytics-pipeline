import os
import csv
import time
import logging
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv
load_dotenv()
API_KEY = os.getenv("YOUTUBE_API_KEY")       
REGION_CODE = "VN"                  
MAX_TRENDING_VIDEOS = 50          
MAX_COMMENTS_PER_VIDEO = 100        
OUTPUT_DIR = "youtube_data"        

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("crawler.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


def get_youtube_client():
    return build("youtube", "v3", developerKey=API_KEY)


def fetch_trending_videos(youtube, region_code=REGION_CODE, max_results=MAX_TRENDING_VIDEOS):
    """Fetch trending videos for a given region."""
    log.info(f"Fetching top {max_results} trending videos for region: {region_code}")
    try:
        response = youtube.videos().list(
            part="snippet,statistics,contentDetails",
            chart="mostPopular",
            regionCode=region_code,
            maxResults=max_results
        ).execute()

        videos = []
        for item in response.get("items", []):
            snippet = item.get("snippet", {})
            stats = item.get("statistics", {})
            content = item.get("contentDetails", {})

            videos.append({
                "video_id": item["id"],
                "title": snippet.get("title", ""),
                "channel_id": snippet.get("channelId", ""),
                "channel_title": snippet.get("channelTitle", ""),
                "published_at": snippet.get("publishedAt", ""),
                "description": snippet.get("description", "")[:300],  # Truncate long descriptions
                "tags": "|".join(snippet.get("tags", [])),
                "category_id": snippet.get("categoryId", ""),
                "duration": content.get("duration", ""),
                "view_count": stats.get("viewCount", 0),
                "like_count": stats.get("likeCount", 0),
                "comment_count": stats.get("commentCount", 0),
                "thumbnail_url": snippet.get("thumbnails", {}).get("high", {}).get("url", ""),
                "crawled_at": datetime.now().isoformat()
            })

        log.info(f"Fetched {len(videos)} trending videos.")
        return videos

    except HttpError as e:
        log.error(f"Error fetching trending videos: {e}")
        return []


def fetch_channel_info(youtube, channel_ids):
    """Fetch channel details for a list of channel IDs."""
    log.info(f"Fetching info for {len(channel_ids)} channels...")
    channels = []

    for i in range(0, len(channel_ids), 50):
        batch = channel_ids[i:i+50]
        try:
            response = youtube.channels().list(
                part="snippet,statistics,brandingSettings",
                id=",".join(batch)
            ).execute()

            for item in response.get("items", []):
                snippet = item.get("snippet", {})
                stats = item.get("statistics", {})
                channels.append({
                    "channel_id": item["id"],
                    "channel_name": snippet.get("title", ""),
                    "description": snippet.get("description", "")[:300],
                    "country": snippet.get("country", ""),
                    "published_at": snippet.get("publishedAt", ""),
                    "subscriber_count": stats.get("subscriberCount", 0),
                    "video_count": stats.get("videoCount", 0),
                    "view_count": stats.get("viewCount", 0),
                    "thumbnail_url": snippet.get("thumbnails", {}).get("high", {}).get("url", ""),
                    "crawled_at": datetime.now().isoformat()
                })

            time.sleep(0.5)  # Rate limiting

        except HttpError as e:
            log.error(f"Error fetching channel info: {e}")

    log.info(f"Fetched info for {len(channels)} channels.")
    return channels


def fetch_comments(youtube, video_id, max_results=MAX_COMMENTS_PER_VIDEO):
    """Fetch top-level comments for a video."""
    comments = []
    next_page_token = None

    try:
        while len(comments) < max_results:
            request_params = {
                "part": "snippet",
                "videoId": video_id,
                "maxResults": min(100, max_results - len(comments)),
                "order": "relevance",
                "textFormat": "plainText"
            }
            if next_page_token:
                request_params["pageToken"] = next_page_token

            response = youtube.commentThreads().list(**request_params).execute()

            for item in response.get("items", []):
                top_comment = item["snippet"]["topLevelComment"]["snippet"]
                comments.append({
                    "comment_id": item["id"],
                    "video_id": video_id,
                    "author": top_comment.get("authorDisplayName", ""),
                    "author_channel_id": top_comment.get("authorChannelId", {}).get("value", ""),
                    "text": top_comment.get("textDisplay", ""),
                    "like_count": top_comment.get("likeCount", 0),
                    "reply_count": item["snippet"].get("totalReplyCount", 0),
                    "published_at": top_comment.get("publishedAt", ""),
                    "updated_at": top_comment.get("updatedAt", ""),
                    "crawled_at": datetime.now().isoformat()
                })

            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break

    except HttpError as e:
        if "commentsDisabled" in str(e):
            log.warning(f"Comments disabled for video {video_id}")
        else:
            log.error(f"Error fetching comments for {video_id}: {e}")

    return comments


def save_to_csv(data, filename, fieldnames):
    """Save a list of dicts to a CSV file."""
    if not data:
        log.warning(f"No data to save for {filename}")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filepath = os.path.join(OUTPUT_DIR, filename)

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

    log.info(f"Saved {len(data)} rows → {filepath}")


def run_crawler():
    log.info("=" * 50)
    log.info("YouTube Big Data Crawler Started")
    log.info("=" * 50)

    youtube = get_youtube_client()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # ── Step 1: Trending Videos ──
    videos = fetch_trending_videos(youtube)

    if not videos:
        log.error("No videos fetched. Check your API key and region code.")
        return

    save_to_csv(
        videos,
        f"trending_videos_{timestamp}.csv",
        fieldnames=list(videos[0].keys())
    )

    # ── Step 2: Channel Info ──
    channel_ids = list(set(v["channel_id"] for v in videos))
    channels = fetch_channel_info(youtube, channel_ids)

    save_to_csv(
        channels,
        f"channels_{timestamp}.csv",
        fieldnames=list(channels[0].keys()) if channels else []
    )

    # ── Step 3: Comments for Each Video ──
    all_comments = []
    for i, video in enumerate(videos):
        video_id = video["video_id"]
        title = video["title"][:50]
        log.info(f"[{i+1}/{len(videos)}] Fetching comments for: {title}")

        comments = fetch_comments(youtube, video_id)
        all_comments.extend(comments)

        time.sleep(1)  # Be polite to the API

    if all_comments:
        save_to_csv(
            all_comments,
            f"comments_{timestamp}.csv",
            fieldnames=list(all_comments[0].keys())
        )

    
    log.info("=" * 50)
    log.info("Crawl Complete! Summary:")
    log.info(f"  Videos collected   : {len(videos)}")
    log.info(f"  Channels collected : {len(channels)}")
    log.info(f"  Comments collected : {len(all_comments)}")
    log.info(f"  Output folder      : {OUTPUT_DIR}/")
    log.info("=" * 50)


if __name__ == "__main__":
    run_crawler()
