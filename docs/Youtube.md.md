# YouTube Big Data Crawler

Crawls trending YouTube videos and collects:
- Video metadata (title, views, likes, duration, tags, etc.)
- Channel information (subscribers, total views, country, etc.)
- Comments (text, author, likes, replies, etc.)

All data is saved as CSV files ready for big data analysis.

---

## Setup

### 1. Get a YouTube API Key
1. Go to https://console.cloud.google.com/
2. Create a new project
3. Enable **YouTube Data API v3**
4. Go to Credentials → Create → **API Key**
5. Copy the key

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure the Crawler
Open `youtube_crawler.py` and edit the config section at the top:

```python
API_KEY      = "YOUR_API_KEY_HERE"   # Paste your API key
REGION_CODE  = "US"                  # Change to your region (VN, GB, JP, etc.)
MAX_TRENDING_VIDEOS  = 50            # Up to 50 (YouTube API limit per request)
MAX_COMMENTS_PER_VIDEO = 100         # Comments to collect per video
OUTPUT_DIR   = "youtube_data"        # Output folder name
```

### 4. Run the Crawler
```bash
python youtube_crawler.py
```

---

## Output Files

All files are saved in the `youtube_data/` folder with a timestamp:

| File | Description |
|---|---|
| `trending_videos_TIMESTAMP.csv` | Video metadata |
| `channels_TIMESTAMP.csv` | Channel information |
| `comments_TIMESTAMP.csv` | Video comments |
| `crawler.log` | Execution log |

---

## Output Schema

### trending_videos.csv
| Column | Description |
|---|---|
| video_id | Unique YouTube video ID |
| title | Video title |
| channel_id | Channel ID |
| channel_title | Channel name |
| published_at | Upload date/time |
| description | Video description (first 300 chars) |
| tags | Tags separated by `\|` |
| category_id | YouTube category ID |
| duration | ISO 8601 duration (e.g. PT5M30S) |
| view_count | Total views |
| like_count | Total likes |
| comment_count | Total comments |
| thumbnail_url | High-res thumbnail URL |
| crawled_at | Timestamp when data was collected |

### channels.csv
| Column | Description |
|---|---|
| channel_id | Unique channel ID |
| channel_name | Channel name |
| description | Channel description |
| country | Country code |
| published_at | Channel creation date |
| subscriber_count | Number of subscribers |
| video_count | Total videos published |
| view_count | Total channel views |
| thumbnail_url | Channel profile picture URL |
| crawled_at | Timestamp when data was collected |

### comments.csv
| Column | Description |
|---|---|
| comment_id | Unique comment ID |
| video_id | Parent video ID |
| author | Comment author name |
| author_channel_id | Author's channel ID |
| text | Comment text |
| like_count | Number of likes on comment |
| reply_count | Number of replies |
| published_at | Comment publish date |
| updated_at | Last edited date |
| crawled_at | Timestamp when data was collected |

---

## API Quota Notes

YouTube Data API v3 gives **10,000 units/day** for free.

| Operation | Cost |
|---|---|
| Trending videos (50) | ~1 unit |
| Channel info (50) | ~1 unit |
| Comments per video | ~1 unit per page (100 comments) |
| **Total for full run** | ~100–200 units |

You can safely run this crawler **multiple times per day** within the free tier.

---

## Region Codes (Examples)

| Code | Region |
|---|---|
| US | United States |
| VN | Vietnam |
| GB | United Kingdom |
| JP | Japan |
| KR | South Korea |
| IN | India |
| FR | France |
| DE | Germany |
