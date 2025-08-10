from selenium.webdriver.common.by import By
import time
import os
from .base_crawler import BaseCrawler
from minio_utils import MinioClient
from urllib.parse import urljoin
import json


URL = "https://finance.yahoo.com/markets/stocks/most-active/"
ROOT_SAVE_PATH = os.getenv("ACTIVE_TICKERS_ROOT_PATH", "type=active_tickers")  # Lấy tên root path từ biến môi trường
BUCKET_NAME = os.getenv("LANDING_BUCKET", "landing")  # Lấy tên bucket từ biến môi trường
MAX_ATTEMPT = 5

class MostActiveQuoteCrawler(BaseCrawler):
    def __init__(self):
        super().__init__()
        self.crawling_results["data_type"] = "active_tickers"

    def crawl_tickers_from_idx(self, url, start=0, count=50):
        # connect to the URL with pagination parameters
        url_with_params = urljoin(url, f"?start={start}&count={count}")
        self.driver.get(url_with_params)
        print(f"\nCrawling {self.driver.title} from index {start} with count {count}")
        html_content = self.driver.page_source

        print("Checking html content...")
        if self.is_error_html_content(html_content):
            return None
        return html_content

    def crawl_all_tickers(self, crawl_date, count=50, wait_time=2):
        # connect to the URL
        self.driver.get(URL)
        print(f"Crawling all tickers from {self.driver.title}")
        total = self.get_total() # get the total number of most active tickers

        # tạo minio client 1 lần duy nhất
        minio_client = MinioClient()
        minio_client.ensure_bucket_exists(BUCKET_NAME)

        for i in range(0, total, count):
            success = False

            # Thử lại nhiều lần nếu có lỗi
            for attempt in range(1, MAX_ATTEMPT + 1):
                try:
                    print(f"Attempt {attempt} for count from {i} to {i + count}")
                    html_content = self.crawl_tickers_from_idx(URL, start=i, count=count)
                    if html_content:
                        success = True
                        # upload html lên minio
                        object_name = os.path.join(ROOT_SAVE_PATH, f"date={crawl_date}", f"stocks_most_active_page_{i // count + 1}.html")
                        minio_client.upload_html_content_to_minio(BUCKET_NAME, html_content, object_name)
                        time.sleep(wait_time) # Thêm thời gian chờ để tránh bị chặn
                        break
                        
                except Exception as e:
                    print(f"Attempt {attempt} failed: {e}")
                    if attempt < MAX_ATTEMPT:
                        retry_wait = attempt * 2  # exponential backoff
                        print(f"Waiting {retry_wait} seconds before retry...")
                        time.sleep(retry_wait)
                    
            if not success:
                print(f"Failed to crawl from {i} to {i + count} after {MAX_ATTEMPT} attempts")

        # lưu lại kết quả vào minio
        self.crawling_results["total_tickers"] = total
        self.crawling_results["crawl_date"] = crawl_date
        results_path = os.path.join(ROOT_SAVE_PATH, f"date={crawl_date}", "crawling_results.json")
        results_json = json.dumps(self.crawling_results, indent=4)
        minio_client.upload_json_content_to_minio(BUCKET_NAME, results_json, results_path)

    def get_total(self):
        try:
            total_div = self.driver.find_element(By.CSS_SELECTOR, 'div.total.yf-1tdhqb1')
            total_text = total_div.text
            print(f"Total text: {total_text}")
            # lấy số cuối cùng (ví dụ: "1-80 of 80" -> 80)
            import re
            match = re.search(r'of (\d+)', total_text)
            if match:
                total_number = int(match.group(1))
                print(f"Tổng số: {total_number}")
                return total_number
            return None
        except Exception as e:
            print(f"Không tìm thấy tổng số: {e}")
            return None


if __name__ == "__main__":
    pass