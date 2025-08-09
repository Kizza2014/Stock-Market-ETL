from selenium.webdriver.common.by import By
import time
import os
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from .base_crawler import BaseCrawler
from minio_utils import MinioClient
import json


BASE_URL = "https://finance.yahoo.com/quote"
ROOT_SAVE_PATH = os.getenv("HISTORY_ROOT_PATH", "type=history")
MAX_ATTEMPT = 5  # số lần thử tối đa khi crawl dữ liệu lịch sử
LANDING_BUCKET = os.getenv("LANDING_BUCKET", "landing")


class HistoryCrawler(BaseCrawler):
    def __init__(self):
        super().__init__()
        self.crawling_results["data_type"] = "history"

    def crawl_all_daily_histories(self, tickers, crawl_date, wait_time=5):
        tickers = [ticker.upper() for ticker in tickers]  # đảm bảo ticker là chữ hoa

        # tạo minio client để upload dữ liệu 1 lần duy nhất
        minio_client = MinioClient()

        for ticker in tickers:
            success = False

            # Thử lại nhiều lần nếu có lỗi
            for attempt in range(1, MAX_ATTEMPT + 1):
                try:
                    print(f"\nTicker {ticker} - Attempt {attempt}/{MAX_ATTEMPT}")
                    # Thử crawl ticker
                    if self._crawl_single_ticker(ticker, crawl_date, minio_client, wait_time):
                        success = True
                        self.mark_ticker_as_succeeded(ticker)
                        break
                        
                except Exception as e:
                    print(f"Attempt {attempt} failed for {ticker}: {e}")
                    if attempt < MAX_ATTEMPT:
                        retry_wait = attempt * 2  # exponential backoff
                        print(f"Waiting {retry_wait} seconds before retry...")
                        time.sleep(retry_wait)
                    
            if not success:
                print(f"Failed to crawl {ticker} after {MAX_ATTEMPT} attempts")
                self.mark_ticker_as_failed(ticker)

        # lưu lại kết quả
        self.crawling_results["total_tickers"] = len(tickers)
        self.crawling_results["crawl_date"] = crawl_date
        results_json = json.dumps(self.crawling_results, indent=4)
        results_path = os.path.join(ROOT_SAVE_PATH, f"date={crawl_date}", "crawling_results.json")
        minio_client.upload_json_content_to_minio(LANDING_BUCKET, results_json, results_path)

    def _crawl_single_ticker(self, ticker, crawl_date, minio_client, wait_time):
        # url riêng của từng ticker
        url = BASE_URL + "/" + ticker + "/history/"
        print(f"URL: {url}")
        
        self.driver.get(url)
        print(f"Crawling all daily histories of {ticker} from {self.driver.title}")

        # chuyển đến phần xem dữ liệu 5 năm gần nhất
        if not self.show_5_years_histories():
            return False

        # đợi render đầy đủ bảng lịch sử
        time.sleep(wait_time)

        # lấy toàn bộ HTML sau khi đã render
        html = self.driver.page_source

        # kiểm tra xem html có rỗng hay chứa lỗi không
        print("Checking html content...")
        if self.is_error_html_content(html):
            print(f"HTML content for {ticker} is empty or contains an error.")
            return False

        # lưu lại html
        html_path = os.path.join(ROOT_SAVE_PATH, f"date={crawl_date}", f"{ticker}_history.html")
        minio_client.upload_html_content_to_minio(LANDING_BUCKET, html, html_path)
        print(f"Successfully crawled {ticker}")
        return True

    def show_5_years_histories(self, wait_time=20):
        try:
            # click vào tuỳ chọn thời gian
            wait = WebDriverWait(self.driver, wait_time)
            button1 = wait.until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, "button.tertiary-btn.menuBtn.yf-1epmntv")
                )
            )
            button1.click()
            print("Đã click vào nút chọn khoảng thời gian!")

            # chọn dữ liệu 5 năm gần nhất
            wait = WebDriverWait(self.driver, wait_time)
            button2 = wait.until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, "button.tertiary-btn[value='5_Y']")
                )
            )
            button2.click()
            print("Đã click vào nút chọn 5 năm gần nhất!")
            return True
        except Exception as e:
            print(f"Lỗi khi click nút lịch sử: {e}")
            return False

# for testing purposes, this will not be executed when imported as a module
if __name__ == "__main__":
    pass
        