import json
from bs4 import BeautifulSoup
import os
from .base_parser import BaseParser
from minio_utils import MinioClient


LANDING_BUCKET = os.getenv("LANDING_BUCKET", "landing")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")
ROOT_SAVE_PATH = os.getenv("STATISTICS_ROOT_PATH", "type=statistics")


class StatisticsParser(BaseParser):
    def __init__(self):
        super().__init__()
        self.parsing_results["data_type"] = "statistics"

    def parse_all_html(self, parse_date):
        # tạo minio client 1 lần duy nhất
        minio_client = MinioClient()

        # kiểm tra xem dữ liệu ngày đó đã được crawl chưa
        files_path = os.path.join(ROOT_SAVE_PATH, f"date={parse_date}")
        files_list = minio_client.list_files_in_folder(LANDING_BUCKET, files_path)
        available_html = [file for file in files_list if file.endswith('.html')]
        self.parsing_results["total_tickers"] = len(available_html)

        if available_html:
            for html_file in available_html:
                filename = html_file.split("/")[-1]
                ticker = filename.split('_')[0].upper()

                print(f"\nParsing key statistics for {ticker} from {filename}")
                try:
                    html_content = minio_client.read_html_content_from_minio(LANDING_BUCKET, html_file)
                    soup = BeautifulSoup(html_content, 'html.parser')

                    # Tìm container bảng
                    table_container = soup.find('div', class_='table-container')
                    if not table_container:
                        print(f"Không tìm thấy bảng trong file {filename}.")
                        continue

                    # Tìm bảng <table>
                    table = table_container.find('table')
                    if not table:
                        print(f"Không tìm thấy thẻ <table> trong file {filename}.")
                        continue

                    # Lấy header (bảng này cũng bị transposed)
                    header_row = table.find('thead').find('tr')
                    time_row = [th.get_text(strip=True).lower().replace(' ', '_') for th in header_row.find_all('th')]
                    time_row[0] = "breakdown"

                    # Lấy dữ liệu các dòng
                    rows_data = []
                    for tr in table.find('tbody').find_all('tr'):
                        values = [td.get_text(strip=True) for td in tr.find_all('td')]
                        rows_data.append(values)

                    # thêm hàng ticker
                    ticker_row = ["ticker"] + [ticker]*(len(time_row) - 1)
                    rows_data.insert(0, ticker_row)  # Thêm ticker vào đầu danh sách
                    rows_data.insert(1, time_row)  
                    transposed_data = self.transpose_data(rows_data)
                    schema = transposed_data[0]  # Lấy header từ dữ liệu đã chuyển đổi
                    transposed_data = transposed_data[1:]  # Bỏ header ra khỏi dữ liệu

                    # lưu kết quả vào minio dưới dạng parquet
                    save_path = os.path.join(files_path, f"{ticker}_statistics_parsed.parquet")
                    minio_client.upload_to_minio_as_parquet(transposed_data, schema, save_path, BRONZE_BUCKET)

                    print(f"Status: succeeded")
                    self.parsing_results["total_succeeded"] = self.parsing_results.get("total_succeeded", 0) + 1
                except Exception as e:
                    print(f"Status: failed - {e}")
                    self.parsing_results["total_failed"] = self.parsing_results.get("total_failed", 0) + 1
                    continue

        # lưu lại kết quả parse
        self.parsing_results["parse_date"] = parse_date
        results_json = json.dumps(self.parsing_results, indent=4)
        results_path = os.path.join(files_path, "_parsing_results.json")
        minio_client.upload_json_content_to_minio(BRONZE_BUCKET, results_json, results_path)


if __name__ == "__main__":
    pass