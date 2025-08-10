import time
import os
import json
from .base_crawler import BaseCrawler
from minio_utils import MinioClient


BASE_URL = "https://finance.yahoo.com/quote"
ROOT_SAVE_PATH = os.getenv("INCOME_STATEMENT_ROOT_PATH", "type=income_statement")
LANDING_BUCKET = os.getenv("LANDING_BUCKET", "landing")
MAX_ATTEMPT = 5


class IncomeStatementCrawler(BaseCrawler):
    def __init__(self):
        super().__init__()
        self.crawling_results["data_type"] = "income_statement"

    def crawl_income_statement(self, tickers, crawl_date, wait_time=4):
        # tạo minio client 1 lần duy nhất
        minio_client = MinioClient()

        tickers = [ticker.upper() for ticker in tickers]  # chuyển tất cả mã thành chữ hoa
        self.crawling_results["total_tickers"] = len(tickers)
        for ticker in tickers:
            print(f"\nTicker {ticker}:")
            try:
                url = BASE_URL + "/" + ticker + "/financials/"
                self.driver.get(url)
                print(f"\nCrawling income statement from {self.driver.title}")
                time.sleep(wait_time)  # đợi trang load xong
                
                ## bấm vào nút "Quarterly" để hiển thị dữ liệu theo từng quý
                self.show_quarterly_data()

                # Lấy nội dung HTML của trang
                html_content = self.driver.page_source

                # lưu lại html vào minio
                save_path = os.path.join(ROOT_SAVE_PATH, f"date={crawl_date}", f"{ticker}_income_statement.json")
                minio_client.upload_html_content_to_minio(LANDING_BUCKET, html_content, save_path)
                self.mark_ticker_as_succeeded(ticker)
            except Exception as e:
                print(f"Error: no income statement data found for {ticker}")
                self.mark_ticker_as_failed(ticker)

        # lưu lại kết quả crawl vào minio
        self.crawling_results["crawl_date"] = crawl_date
        results_json = json.dumps(self.crawling_results, indent=4)
        results_path = os.path.join(ROOT_SAVE_PATH, f"date={crawl_date}", f"crawling_results.json")
        minio_client.upload_json_content_to_minio(LANDING_BUCKET, results_json, results_path)


if __name__ == "__main__":
    pass