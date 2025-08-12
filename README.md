# Cấu trúc thư mục dự án

```plaintext
Stock-Market-ETL/
├── main.py
├── README.md
├── extract/
│   ├── __init__.py
│   ├── base_crawler.py
│   ├── history_crawler.py
│   ├── income_statement_crawler.py
│   ├── most_active_quote_crawler.py
│   ├── profile_crawler.py
│   ├── statistics_crawler.py
├── load/
│   ├── __init__.py
│   ├── base_parser.py
│   ├── history_parser.py
│   ├── income_statement_parser.py
│   ├── most_active_quote_parser.py
│   ├── parse_utils.py
│   ├── profile_parser.py
│   └── statistics_parser.py
├── minio_utils/
│   ├── __init__.py
│   └── minio_client.py
├── transform/
│   ├── base_transformer.py
│   ├── data_cleaner.py
│   ├── transform_balance_sheet.py
│   ├── transform_cash_flow.py
│   ├── transform_company.py
│   ├── transform_history.py
│   ├── transform_income_statement.py
│   └── transform_statistics.py
```
- Change logs: https://docs.google.com/spreadsheets/d/1OtZ5dOQT_BJD4e0hYIlsiwHSkiLpegukf0JIdGKC7_w/edit?usp=sharing

- Docs (draft): https://docs.google.com/document/d/16sjfthd2gej7x0Ln8kI0lfna23lkCRFiex0-kYZs-bE/edit?usp=sharing

