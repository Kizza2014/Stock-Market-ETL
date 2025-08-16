import json
from bs4 import BeautifulSoup
import os
from minio_utils import MinioClient
from .base_parser import BaseParser


LANDING_BUCKET = os.getenv("LANDING_BUCKET", "landing")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")
ROOT_SAVE_PATH = os.getenv("CASH_FLOW_ROOT_PATH", "type=cash_flow")


class CashFlowParser(BaseParser):
    def __init__(self):
        super().__init__()
        self.parsing_results["data_type"] = "cash_flow"

    def parse_all_html(self, parse_date):
        # tạo minio client
        minio_client = MinioClient()

        # kiểm tra xem dữ liệu đã được crawl chưa
        files_path = os.path.join(ROOT_SAVE_PATH, f"date={parse_date}")
        files_list = minio_client.list_files_in_folder(LANDING_BUCKET, files_path)
        if not files_list:
            print(f"Không tìm thấy file nào trong {files_path}.")
            return
        
        html_files = [f for f in files_list if f.endswith('.html')]

        for html_file in html_files:
            filename = html_file.split('/')[-1]
            ticker = filename.split('_')[0]  # lấy ticker từ tên file
            
            print(f"\nParsing for {ticker} from {html_file}")
            html = minio_client.read_html_content_from_minio(LANDING_BUCKET, html_file)
            soup = BeautifulSoup(html, 'html.parser')
            
            try:
                # Tìm container bảng
                table_container = soup.find('div', class_='tableContainer')
                if not table_container:
                    print(f"Không tìm thấy bảng trong file {html_file}.")
                    continue

                # Lấy header
                header_row = table_container.find('div', class_='tableHeader').find('div', class_='row')
                time_row = [col.get_text(strip=True).lower().replace(' ', '_') for col in header_row.find_all('div', class_='column')]

                # Lấy dữ liệu các dòng
                rows_data = []
                for row in table_container.find('div', class_='tableBody').find_all('div', class_='row'):
                    breakdown_div = row.find('div', class_='column')
                    row_title = breakdown_div.find('div', class_='rowTitle')
                    if row_title:
                        # Lấy thuộc tính title nếu có, nếu không thì lấy text
                        breakdown = row_title.get('title') or row_title.get_text(strip=True)
                    else:
                        # Nếu không có rowTitle, lấy toàn bộ text (loại bỏ thẻ con như button)
                        breakdown = ''.join([t for t in breakdown_div.strings]).strip()
        
                    # Lấy các giá trị
                    values = [col.get_text(strip=True) for col in row.find_all('div', class_='column') if 'sticky' not in col.get('class', [])]
                    values.insert(0, breakdown)
                    rows_data.append(values)

                ticker_row = ["ticker"] + [ticker] * (len(time_row) - 1)
                rows_data.insert(0, ticker_row)  # Thêm ticker_row vào đầu danh sách
                rows_data.insert(1, time_row)  # Thêm time_row vào vị trí thứ hai

                # chuyển đổi dữ liệu từ dạng hàng sang dạng cột
                transposed_data = self.transpose_data(rows_data)
                schema = self.normalize_schema(transposed_data[0]) # lấy schema từ hàng đầu tiên
                transposed_data = transposed_data[1:]  # loại bỏ schema khỏi dữ liệu
                

                print(f"Tổng số dòng: {len(transposed_data)}")
                
                # lưu parquet lên minio
                save_path = os.path.join(files_path, f"{ticker}_cash_flow_parsed.parquet")
                minio_client.upload_to_minio_as_parquet(transposed_data, schema, save_path, BRONZE_BUCKET)

                # ghi log
                print("Status: succeeded")
                self.parsing_results["tickers"][ticker] = "succeeded"
                self.parsing_results["total_succeeded"] += 1
            except Exception as e:
                print("Status: failed")
                self.parsing_results["tickers"][ticker] = "failed"
                self.parsing_results["total_failed"] += 1

        # lưu lại logs
        self.parsing_results["parse_date"] = parse_date
        self.parsing_results["total_tickers"] = len(html_files)
        results_json = json.dumps(self.parsing_results, indent=4)
        results_path = os.path.join(files_path, "_parsing_results.json")
        minio_client.upload_json_content_to_minio(BRONZE_BUCKET, results_json, results_path)