# Cấu trúc thư mục dự án

```plaintext
Stock-Market-ETL/
├── main.py
├── demo.py : demo query ui
├── README.md
├── extract/
│   ├── crawl data from web
├── load/
│   ├── parse HTML to extract table structure then upload to MinIO as parquet
├── minio_utils/
│   ├── __init__.py
│   └── minio_client.py
├── transform/
│   ├── transform and upload to MinIO as delta format
├── hive_metastore
|   ├── hive configuration and registering for delta table
├── trino_config
|   ├── configuration for query engine
```
- Change logs: https://docs.google.com/spreadsheets/d/1OtZ5dOQT_BJD4e0hYIlsiwHSkiLpegukf0JIdGKC7_w/edit?usp=sharing

- Docs (draft): https://docs.google.com/document/d/16sjfthd2gej7x0Ln8kI0lfna23lkCRFiex0-kYZs-bE/edit?usp=sharing

