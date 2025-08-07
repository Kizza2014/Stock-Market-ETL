from selenium.webdriver.common.by import By
import time
from datetime import date
import os
from .base_crawler import BaseCrawler
from minio_utils import upload_html_content_to_minio, create_minio_client
from urllib.parse import urljoin
from dotenv import load_dotenv


load_dotenv()  # Load environment variables from .env file
URL = "https://finance.yahoo.com/markets/stocks/most-active/"
SAVE_PATH = "active_tickers" # lưu trữ trên local, sau này sẽ thay bằng đường dẫn đến S3 bucket
BUCKET_NAME = os.getenv("LANDING_BUCKET", "landing")  # Lấy tên bucket từ biến môi trường

class MostActiveQuoteCrawler(BaseCrawler):
    def __init__(self):
        super().__init__()

    def crawl_tickers_from_idx(self, url, start=0, count=50):
        # connect to the URL with pagination parameters
        url_with_params = urljoin(url, f"?start={start}&count={count}")
        self.driver.get(url_with_params)
        print(f"\nCrawling {self.driver.title} from index {start} with count {count}")
        html = self.driver.page_source
        return html

    def crawl_all_tickers(self, url, save_path, count=50):
        # connect to the URL
        self.driver.get(url)
        print(f"Crawling all tickers from {self.driver.title}")
        total = self.get_total() # get the total number of most active tickers

        # tạo minio client 1 lần duy nhất
        minio_client = create_minio_client()

        for i in range(0, total, count):
            html = self.crawl_tickers_from_idx(url, start=i, count=count)
            # tạo minio client và upload file
            object_name = os.path.join(save_path, f"stocks_most_active_page_{i // count + 1}.html")
            upload_html_content_to_minio(minio_client, BUCKET_NAME, html, object_name)
            time.sleep(2) # Thêm thời gian chờ để tránh bị chặn

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
    print("\n\n================== MOST ACTIVE QUOTES CRAWLING ==================\n")

    # đường dẫn lưu rawl html và parsed csv
    crawl_date = date.today().strftime("%Y_%m_%d")
    print(f"Crawling date: {crawl_date}")
    path = os.path.join(SAVE_PATH, f"{crawl_date}")
    print(f"Path to save crawled data: {path}")
    # create_folder_if_not_exists(path)

    # crawl dữ liệu
    crawler = MostActiveQuoteCrawler()
    crawler.crawl_all_tickers(URL, save_path=path)
    print("\nCrawling completed.")
    crawler.quit()
    
    # # parse dữ liệu và lưu vào csv
    # parser = MostActiveQuoteParser()
    # parser.parse_all_html(path=path)
    # print("\nParsing completed.")

    print("\n\n================== MOST ACTIVE QUOTES CRAWLING COMPLETED ==================\n")
