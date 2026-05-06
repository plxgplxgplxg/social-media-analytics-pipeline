# Monitoring Placeholders

Thư mục này giữ các artifact monitoring/smoke-check mức demo:

- `elasticsearch-smoke-check-job.yaml`: job kiểm tra nhanh Elasticsearch và Kibana sau khi deploy.

Repo này chưa bundle full Prometheus stack vì mục tiêu là demo/submission local-first. Luồng observability chính vẫn là:

- Kafka UI cho topic flow
- Spark UI cho micro-batch/execution
- MinIO Console cho Parquet/checkpoint
- Kibana cho search/dashboard
- script smoke-check local cho xác minh stack
