# Báo Cáo Tổng Hợp: Kiến Trúc Hệ Thống Phân Tích Dữ Liệu Mạng Xã Hội (Data Pipeline)

Tài liệu này cung cấp cái nhìn tổng quan về luồng dữ liệu (data flow) trong hệ thống, các lưu ý quan trọng về môi trường triển khai, và tài liệu hướng dẫn mở rộng để tích hợp thêm các nguồn dữ liệu mới (Reddit, X/Twitter, Facebook).

---

## 1. Luồng Xử Lý Dữ Liệu Cốt Lõi (Core Pipeline)

Hệ thống được thiết kế theo kiến trúc **Kappa**, trong đó toàn bộ dữ liệu được xử lý dưới dạng luồng sự kiện (event streams) theo thời gian thực.

1. **Thu thập dữ liệu (Ingestion/Collectors):** 
   - Module `collectors/rss_collector.py` định kỳ truy vấn các nguồn báo điện tử (VnExpress, Tuổi Trẻ, BBC) thông qua giao thức RSS.
   - Quá trình này được trang bị cơ chế chịu lỗi mạng: tự động thử lại với thời gian trễ tăng dần (exponential backoff) nhằm đảm bảo tính ổn định.
2. **Trạm trung chuyển (Message Broker):** 
   - Dữ liệu thô (Raw Events) được đẩy trực tiếp vào **Apache Kafka** (topic `raw_posts`). Kafka đóng vai trò như một bộ đệm (buffer) khổng lồ, tách biệt hoàn toàn pha thu thập và pha xử lý.
3. **Xử lý luồng (Stream Processing):** 
   - **Apache Spark Structured Streaming** (`spark_jobs/stream_processor.py`) tiêu thụ dữ liệu liên tục từ Kafka.
   - Các tác vụ xử lý bao gồm: chuẩn hóa văn bản (loại bỏ HTML rác), loại bỏ bản ghi trùng lặp (deduplication) dựa trên khóa định danh, trích xuất từ khóa (keyword extraction), và phân tích cảm xúc (Sentiment Analysis) sử dụng `TextBlob` để gán nhãn Tích cực/Tiêu cực/Trung lập.
4. **Lưu trữ đa tầng (Polyglot Persistence Sink):** 
   - Dữ liệu sau khi làm sạch được ghi song song ra các hệ thống lưu trữ đích để phục vụ nhiều mục đích khác nhau:
     - **MinIO (S3-compatible):** Ghi file `.parquet` nén để lưu trữ lâu dài (Data Lake) và huấn luyện AI.
     - **MongoDB:** Lưu trữ NoSQL dạng Document, phục vụ truy vấn ứng dụng và lưu số liệu thống kê.
     - **Elasticsearch:** Lập chỉ mục toàn văn bản (Full-text index) để hỗ trợ tìm kiếm siêu tốc.
     - **Kafka:** Đẩy lại vào topic `processed_posts` để các Microservices khác có thể tiêu thụ nếu cần.
5. **Trực quan hóa (Visualization):** 
   - **Kibana & Grafana** được kết nối với Elasticsearch để hiển thị các biểu đồ biến động thể tích bài viết và phân bổ cảm xúc theo thời gian thực.

---

## 2. Cấu Hình Môi Trường & Lưu Ý Triển Khai

Trong quá trình phát triển và kiểm thử, một số vấn đề về khả năng tương thích đã được ghi nhận và khắc phục. Dưới đây là các lưu ý bắt buộc khi triển khai trên môi trường mới:

### 2.1. Giới hạn phiên bản Python (PySpark Compatibility)
- **Vấn đề:** PySpark 3.5.1 trên nền tảng Windows gặp lỗi nghiêm trọng (`EOFException` / `[JAVA_GATEWAY_EXITED]`) khi thực thi Python UDFs (User Defined Functions) trên nền Python 3.13 hoặc khi tương tác với thư viện Numpy 2.x.
- **Giải pháp bắt buộc:** Toàn bộ tiến trình Spark phải được khởi chạy trong môi trường **Python 3.11** (`venv311`). Không nâng cấp phiên bản Python cho môi trường chạy Spark ở thời điểm hiện tại để đảm bảo độ ổn định mức Production.

### 2.2. Khởi tạo kho dữ liệu NLTK
- Bộ phân tích ngôn ngữ tự nhiên `TextBlob` yêu cầu kho ngữ liệu `punkt` để thực hiện tách từ (tokenization).
- Sau khi cài đặt các thư viện trong `requirements.txt`, kỹ sư hệ thống phải chạy lệnh: `python -m textblob.download_corpora` trước khi khởi động Spark.

### 2.3. Cập nhật `requirements.txt`
Dự án đã chuẩn hóa tệp `requirements.txt` bằng việc bổ sung các thư viện quản trị:
- `pytest`: Phục vụ bộ Unit Test cho toàn bộ tiến trình.
- `python-dotenv`: Quản lý linh hoạt các biến môi trường cấu hình động (`init_mongodb.py`, `init_elasticsearch.py`) mà không cần mã hóa cứng (hardcode).

---

## 3. Cơ Chế Giám Sát Hệ Thống (Monitoring & Healthchecks)

- **Docker Healthchecks:** Mọi containers (`kafka`, `zookeeper`, `mongodb`, `elasticsearch`, `minio`) đều được định nghĩa tham số `healthcheck` trong `docker-compose.yml`. Các service phụ thuộc (như `kafka` phụ thuộc `zookeeper`) chỉ được khởi tạo khi service nền tảng đã đạt trạng thái `healthy`.
- **Script kiểm định cục bộ:** Tệp `scripts/healthcheck.py` cung cấp khả năng tự chẩn đoán (self-diagnose), kết nối đến toàn bộ 4 hạ tầng lưu trữ và trả về trạng thái chuẩn JSON. Exit code của script có thể dùng cho hệ thống CI/CD để chặn triển khai nếu hạ tầng lỗi.

---

## 4. Hướng Dẫn Mở Rộng Hệ Thống (System Extensibility)

Nhờ kiến trúc phân tách cao (Decoupled Architecture) xoay quanh Apache Kafka, hệ thống có thể dễ dàng mở rộng để thu thập thêm dữ liệu từ các mạng xã hội khác mà không cần sửa đổi bất kỳ dòng code nào ở tầng xử lý (Spark) hay lưu trữ (DBs).

### 4.1. Tích hợp nguồn dữ liệu Reddit API
Mã nguồn cho Reddit Collector đã được viết sẵn tại `collectors/reddit_collector.py`. Để kích hoạt:
1. Đăng ký ứng dụng tại cổng Developer của Reddit để lấy API Key.
2. Cập nhật các biến môi trường vào tệp `.env`:
   - `REDDIT_CLIENT_ID`
   - `REDDIT_CLIENT_SECRET`
   - `REDDIT_USER_AGENT`
3. Chạy script một cách độc lập: `python collectors/reddit_collector.py`. Script này sẽ tuân thủ cấu trúc Schema chuẩn và đẩy trực tiếp vào Kafka.

### 4.2. Tích hợp nguồn dữ liệu từ MXH khác (Facebook, X/Twitter, TikTok)
Khi cần thiết lập thêm các kênh dữ liệu mới, kỹ sư chỉ cần phát triển một Collector độc lập hoàn toàn. Quy trình thực hiện:

1. **Xây dựng module truy xuất (Extractor):** Viết mã Python để gọi Graph API của Facebook, Twitter API (X), hoặc cào dữ liệu (Web Scraping).
2. **Tuân thủ Hợp đồng Dữ liệu (Data Contract):** Đây là yêu cầu quan trọng nhất. Dữ liệu sau khi thu thập từ bất kỳ MXH nào đều phải được ánh xạ (map) chuẩn xác về `schemas/post_schema.py`. Một JSON chuẩn bắt buộc phải có các trường:
   ```json
   {
       "id": "chuỗi_định_danh_độc_nhất_từ_mxh",
       "source": "tên_mạng_xã_hội",
       "title": "tiêu_đề",
       "content": "nội_dung_chính",
       "url": "đường_dẫn_bài_viết",
       "author": "tên_tác_giả",
       "published_at": "ISO-8601_datetime_string",
       "ingested_at": "ISO-8601_datetime_string"
   }
   ```
3. **Đẩy vào Kafka:** Sử dụng hàm tiện ích `publish_events` trong `collectors/common.py` để đẩy gói tin JSON này vào topic `raw_posts`. 
4. **Kết quả:** Ngay lập tức, luồng Spark Streaming hiện hữu sẽ tự động tiêu thụ, xử lý cảm xúc và đẩy dữ liệu mới lên Dashboard mà không cần cấu hình thêm.
