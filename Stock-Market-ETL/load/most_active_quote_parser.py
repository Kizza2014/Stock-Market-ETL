from bs4 import BeautifulSoup
from .base_parser import BaseParser
from minio_utils import MinioClient
import os
import json


LANDING_BUCKET = os.getenv("LANDING_BUCKET", "landing")  # Lấy tên bucket từ biến môi trường
ROOT_SAVE_PATH = os.getenv("ACTIVE_TICKERS_ROOT_PATH", "type=active_tickers")  # Lấy tên root path từ biến môi trường
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")  # Lấy tên bucket từ biến môi trường


class MostActiveQuoteParser(BaseParser):
    def __init__(self):
        super().__init__()

    def parse_all_html(self, parse_date):
        # tạo minio client để đọc dữ liệu
        minio_client = MinioClient()

        # kiểm tra nội dung bucket
        files_path = os.path.join(ROOT_SAVE_PATH, f"date={parse_date}")
        files_list = minio_client.list_files_in_folder(LANDING_BUCKET, files_path)
        html_files = [f for f in files_list if f.endswith('.html')]
        if not html_files:
            print(f"No HTML files found in bucket '{LANDING_BUCKET}' at path '{files_path}'.")
            return

        print(f"Parsing HTML files in bucket '{LANDING_BUCKET}' at path '{files_path}'...")
        rows_data =[]
        schema = None
        for file in html_files:
            print(f"\nParsing file: {file}")
            html = minio_client.read_html_content_from_minio(LANDING_BUCKET, file)
            if html:
                parsed_data, parsed_schema = self.parse_html(html)

            # Kiểm tra schema
            if schema is None:
                schema = parsed_schema
            if parsed_schema != schema:
                print(f"Cảnh báo: Schema không khớp trong file {file}")
            rows_data.extend(parsed_data)
        
        # kết quả được lưu vào bucket bronze dưới dạng parquet
        save_path = os.path.join(ROOT_SAVE_PATH, f"date={parse_date}", "most_active_quotes_parsed.parquet")
        minio_client.upload_to_minio_as_parquet(rows_data, schema, save_path, BRONZE_BUCKET)
        print(f"Đã xử lí {len(rows_data)} dòng dữ liệu với schema: {schema}")

        # lưu lại kết quả parse
        most_active_tickers = [row[0] for row in rows_data]
        self.parsing_results["data_type"] = "active_tickers"
        self.parsing_results["total_tickers"] = len(most_active_tickers)
        self.parsing_results["parse_date"] = parse_date
        results_json = json.dumps(self.parsing_results, indent=4)
        results_path = os.path.join(ROOT_SAVE_PATH, f"date={parse_date}", "parsing_results.json")
        minio_client.upload_json_content_to_minio(BRONZE_BUCKET, results_json, results_path)

        # trả về danh sách tickers đã crawl
        return most_active_tickers

    def parse_html(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table')
        rows_data = []
        schema = None
        if table:
            rows = table.find_all('tr')

            # Lấy schema từ hàng đầu tiên
            schema = [cell.get_text(strip=True) for cell in rows[0].find_all(['th'])]
            schema = self.normalize_schema(schema)

            for row in rows[1:]:
                cells = row.find_all(['td'])
                cell_values = [cell.get_text(strip=True) for cell in cells]
                if cell_values:
                    rows_data.append(cell_values)
            print(f"Tổng số dòng: {len(rows_data)}")
        return rows_data, schema


# for testing purposes
if __name__ == "__main__":
    pass