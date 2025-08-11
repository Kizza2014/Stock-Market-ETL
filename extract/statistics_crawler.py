import time
import os
from .base_crawler import BaseCrawler
from  minio_utils import MinioClient


BASE_URL = "https://finance.yahoo.com/quote"
ROOT_SAVE_PATH = os.getenv("STATISTICS_ROOT_PATH", "type=statistics")
LANDING_BUCKET = os.getenv("LANDING_BUCKET", "landing")
MAX_ATTEMPT = 5


class StatisticsCrawler(BaseCrawler):
    def __init__(self):
        super().__init__()
        self.crawling_results["data_type"] = "statistics"

    def crawl_statistics(self, tickers, crawl_date, min_delay=4, max_delay=10):
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
                    if self._crawl_single_ticker(ticker, crawl_date, minio_client, min_delay, max_delay):
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

    def _crawl_single_ticker(self, ticker, crawl_date, minio_client, min_delay, max_delay):
        # url riêng của từng ticker
        url = BASE_URL + "/" + ticker + "/key-statistics/"
        print(f"URL: {url}")
        
        self.driver.get(url)
        print(f"Crawling statistics of {ticker} from {self.driver.title}")

        # đợi render đầy đủ
        self.wait_random_delay(min_delay, max_delay)

        # lấy toàn bộ HTML sau khi đã render
        html = self.driver.page_source

        # kiểm tra xem html có rỗng hay chứa lỗi không
        print("Checking html content...")
        if self.is_error_html_content(html):
            print(f"HTML content for {ticker} is empty or contains an error.")
            return False

        # lưu lại html
        html_path = os.path.join(ROOT_SAVE_PATH, f"date={crawl_date}", f"{ticker}_statistics.html")
        minio_client.upload_html_content_to_minio(LANDING_BUCKET, html, html_path)
        print(f"Successfully crawled {ticker}")
        return True


if __name__ == "__main__":
    pass