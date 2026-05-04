# Hướng Dẫn Sử Dụng và Kiểm Thử (User Guide & Testing Manual)

Tài liệu này hướng dẫn chi tiết các thao tác khởi chạy dự án từ đầu (User Guide) cũng như cách chạy các kịch bản kiểm thử (Testing) để đảm bảo hệ thống vận hành trơn tru.

---

## Phần 1: Hướng Dẫn Sử Dụng (User Guide)

### 1.1. Khởi chạy hạ tầng Docker
Mở Terminal tại thư mục gốc của dự án và khởi chạy toàn bộ dịch vụ cốt lõi:
```powershell
docker compose up -d
```
*Các dịch vụ được kích hoạt: Kafka, Zookeeper, MinIO, MongoDB, Elasticsearch, Kibana, Grafana.*

### 1.2. Thiết lập Môi trường Python
Hệ thống **bắt buộc** chạy trên Python 3.11 để tương thích với Apache Spark trên Windows.
```powershell
# Khởi tạo và kích hoạt môi trường ảo
py -3.11 -m venv venv311
.\venv311\Scripts\Activate.ps1

# Cài đặt thư viện phụ thuộc
pip install -r requirements.txt

# Tải tài nguyên ngôn ngữ cho TextBlob (Rất quan trọng)
python -m textblob.download_corpora

# Tạo file biến môi trường (nếu chưa có)
cp .env.example .env
```

### 1.3. Khởi tạo Cơ sở dữ liệu và Kafka
Chỉ cần chạy các lệnh này ở lần thiết lập đầu tiên để tạo Topics, Index Mapping và Collections:
```powershell
.\venv311\Scripts\Activate.ps1
python batch_tools/create_topics.py
python scripts/init_elasticsearch.py
python scripts/init_mongodb.py
```

### 1.4. Kích hoạt Luồng Dữ Liệu
Để pipeline hoạt động, cần chạy 2 tiến trình song song (mở 2 cửa sổ Terminal độc lập):

**Terminal 1 - Chạy Bộ thu thập (Collector):**
```powershell
.\venv311\Scripts\Activate.ps1
python collectors/rss_collector.py
```

**Terminal 2 - Chạy Bộ xử lý lõi (Spark Stream Processor):**
```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-11"  # Thay bằng đường dẫn cài Java 11 thực tế trên máy bạn
$env:HADOOP_HOME="$PWD\hadoop"
$env:PATH="$env:JAVA_HOME\bin;$env:HADOOP_HOME\bin;" + $env:PATH
$env:PYSPARK_PYTHON="$PWD\venv311\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON="$PWD\venv311\Scripts\python.exe"

.\venv311\Scripts\Activate.ps1
python spark_jobs/stream_processor.py
```

### 1.5. Giám sát Dashboard
Khi cả 2 tiến trình đang chạy, mở trình duyệt để xem kết quả:
- **Grafana Dashboard:** [http://localhost:3000](http://localhost:3000) (Tài khoản mặc định: `admin`/`admin`). 
- **Kibana Data Explorer:** [http://localhost:5601](http://localhost:5601) (Vào phần *Discover* -> Chọn *processed_posts*).
- **MinIO Data Lake:** [http://localhost:9001](http://localhost:9001) (Tài khoản: `minioadmin`/`minioadmin123` -> Kiểm tra bucket `clean-posts`).

---

## Phần 2: Hướng Dẫn Kiểm Thử (Testing Manual)

Hệ thống được trang bị bộ kiểm thử tự động toàn diện và các công cụ chẩn đoán thủ công.

### 2.1. Chạy Bộ Unit Test (Kiểm thử tự động)
Hệ thống sử dụng `pytest` để giả lập và kiểm tra từng hàm độc lập mà không cần kết nối tới dịch vụ thực tế.
```powershell
.\venv311\Scripts\Activate.ps1
pytest -v
```
**Các bài test bao gồm:**
- `test_pipeline.py`: Kiểm thử cấu trúc và ép kiểu dữ liệu sự kiện (Event Schema).
- `test_rss_collector.py`: Kiểm thử cơ chế loại bỏ HTML, chống lỗi link hỏng, và gộp bài trùng.
- `test_reddit_collector.py`: Kiểm thử việc xử lý API của Reddit.
- `test_healthcheck.py`: Kiểm thử logic phát hiện lỗi hạ tầng.
- `test_elasticsearch.py` & `test_mongodb.py`: Đảm bảo kịch bản khởi tạo Mapping/Index chính xác.

### 2.2. Kiểm tra Sức Khỏe Hạ Tầng (Healthcheck)
Để chắc chắn các dịch vụ trong Docker đang hoạt động và chấp nhận kết nối:
```powershell
.\venv311\Scripts\Activate.ps1
python scripts/healthcheck.py
```
Nếu tất cả hiển thị `"ok"`, hệ thống hạ tầng hoàn toàn khỏe mạnh.

### 2.3. Kiểm thử Thủ Công (Manual Diagnostics)

**a. Xem luồng tin thô đi vào Kafka:**
Nếu nghi ngờ Collector không đẩy được dữ liệu lên Kafka, hãy đứng ở Terminal dự án và gõ:
```powershell
docker exec -it sma-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic raw_posts --from-beginning
```

**b. Xem luồng tin sạch đi ra từ Spark:**
Nếu nghi ngờ Spark xử lý lỗi, hãy kiểm tra topic đầu ra:
```powershell
docker exec -it sma-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic processed_posts --from-beginning
```

**c. Bơm dữ liệu giả để test Pipeline (Replay Data):**
Thay vì phải đợi tin tức mới từ RSS, bạn có thể bơm lại toàn bộ file mẫu có sẵn vào thẳng Kafka để test xem tốc độ xử lý của Spark ra sao. Mở 1 Terminal mới chạy:
```powershell
.\venv311\Scripts\Activate.ps1
python collectors/historical_replay_producer.py sample_data/posts.json --sleep-seconds 0.5
```
Sau đó kiểm tra biểu đồ trên Grafana, bạn sẽ thấy cột biểu đồ dựng đứng lên do lượng lớn dữ liệu được bơm vào cùng lúc.
