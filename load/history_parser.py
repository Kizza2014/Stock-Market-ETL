import json
import os
from bs4 import BeautifulSoup
from .base_parser import BaseParser
from minio_utils import MinioClient


LANDING_BUCKET = os.getenv("LANDING_BUCKET", "landing")  # Lấy tên bucket từ biến môi trường
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")
ROOT_SAVE_PATH = "type=history"


class HistoryParser(BaseParser):
    def __init__(self):
        super().__init__()

    def parse_html(self, html_content, minio_client, save_path):
        ticker = save_path.split('/')[-1].split('_')[0].upper()  # Lấy ticker từ đường dẫn lưu
        soup = BeautifulSoup(html_content, 'html.parser')
        table = soup.find('table')
        rows_data = []
        if table:
            rows = table.find_all('tr')

            # Lấy schema từ hàng đầu tiên
            schema = [cell.get_text(strip=True) for cell in rows[0].find_all(['th'])]
            schema.insert(0, "ticker")  # Thêm ticker vào schema
            schema = self.normalize_schema(schema)

            for row in rows[1:]:
                cells = row.find_all(['td'])
                cell_values = [cell.get_text(strip=True) for cell in cells]
                if cell_values and len(cell_values) > 2: # loại bỏ cả những ngày không giao dịch
                    # thêm ticker vào bản ghi
                    cell_values.insert(0, ticker)
                    rows_data.append(cell_values)
            print(f"Tổng số dòng: {len(rows_data) - 1}") # trừ đi header row
        
        # lưu dữ liệu vào minio dưới dạng parquet
        if len(rows_data) > 0:
            minio_client.upload_to_minio_as_parquet(rows_data, schema, save_path, BRONZE_BUCKET)
            return True

        return False


    def parse_all_html(self, parse_date):
        # tạo minio client 1 lần duy nhất
        minio_client = MinioClient()

        # kiểm tra xem dữ liệu đã được crawl chưa
        files_path = os.path.join(ROOT_SAVE_PATH, f"date={parse_date}")
        files_list = minio_client.list_files_in_folder(LANDING_BUCKET, files_path)
        html_files = [f for f in files_list if f.endswith('.html')]
        
        # parse html cho từng mã và lưu vào file csv
        for html_file in html_files:
            filename = html_file.split('/')[-1]
            ticker = filename.split('_')[0].upper()  # lấy ticker từ tên file

            if ticker in ["BTI", "CFG"]:   # do 2 mã này tạm thời lỗi html, sau khi khắc phục sẽ bỏ dòng này
                continue

            # kết quả được lưu cùng đường dẫn với file HTML
            save_path = os.path.join(files_path, f"{ticker}_history_parsed.parquet")

            print(f"\nParsing file: {filename}")
            html_content = minio_client.read_html_content_from_minio(LANDING_BUCKET, html_file)
            parse_status = self.parse_html(html_content, minio_client, save_path)
            if parse_status:
                print(f"Parsed {ticker} history successfully and saved to {save_path}")
                self.parsing_results["total_succeeded"] = self.parsing_results.get("total_succeeded", 0) + 1
            else:
                print(f"Failed to parse {ticker} history")
                self.parsing_results["total_failed"] = self.parsing_results.get("total_failed", 0) + 1

        print(f"\nĐã xử lý xong {len(html_files)} file HTML.")

        # lưu lại kết quả vào minio
        self.parsing_results["data_type"] = "history"
        self.parsing_results["total_tickers"] = len(html_files)
        self.parsing_results["parse_date"] = parse_date
        results_json = json.dumps(self.parsing_results, indent=4)
        results_path = os.path.join(files_path, "parsing_results.json")
        minio_client.upload_json_content_to_minio(BRONZE_BUCKET, results_json, results_path)
