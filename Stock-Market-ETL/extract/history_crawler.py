from selenium.webdriver.common.by import By
import time
from datetime import date
import os
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from .base_crawler import BaseCrawler
from urllib.parse import urljoin
from minio_utils import create_minio_client, upload_html_content_to_minio
from dotenv import load_dotenv


load_dotenv()  # Load environment variables from .env file
BASE_URL = "https://finance.yahoo.com/quote/"
SAVE_PATH = "./data_test/crawl_5_years_history/" # lưu trữ trên local, sau này sẽ thay bằng đường dẫn đến S3 bucket
MAX_ATTEMP = 5  # số lần thử tối đa khi crawl dữ liệu lịch sử
BUCKET_NAME = os.getenv("LANDING_BUCKET", "landing") 


class HistoryCrawler(BaseCrawler):
    def __init__(self):
        super().__init__()

    def crawl_all_daily_histories(self, tickers, save_path, wait_time=20):
        tickers = [ticker.upper() for ticker in tickers]  # đảm bảo ticker là chữ hoa

        # tạo minio client để upload dữ liệu 1 lần duy nhất
        minio_client = create_minio_client()

        for ticker in tickers:
            print(f"\nTicker {ticker}:")
            try:
                url = urljoin(BASE_URL, ticker, "history")
                self.driver.get(url)
                print(f"Crawling all daily histories of {ticker} from {self.driver.title}")

                # chuyển đến phần xem dữ liệu 5 năm gần nhất
                self.show_5_years_histories()

                # đợi render đầy đủ bảng lịch sử
                time.sleep(wait_time)

                # lấy toàn bộ HTML sau khi đã render
                html = self.driver.page_source

                # lưu lại html
                html_path = os.path.join(save_path, f"{ticker}_history.html")
                upload_html_content_to_minio(minio_client, BUCKET_NAME, html, html_path)
            except Exception as e:
                print(f"Error while crawling {ticker}: {e}")
                continue

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
                    (By.CSS_SELECTOR, "button.tertiary-btn.tw-w-full.tw-justify-center.yf-1epmntv[value='5_Y']")
                )
            )
            button2.click()
            print("Đã click vào nút chọn 5 năm gần nhất!")
        except Exception as e:
            print(f"Lỗi khi click nút lịch sử: {e}")

# for testing purposes, this will not be executed when imported as a module
if __name__ == "__main__":
    print("\n\n================== HISTORY CRAWLING ==================\n")

    # đường dẫn dùng để lưu raw html và parsed csv
    crawl_date = date.today().strftime("%Y_%m_%d")
    print(f"Crawling date: {crawl_date}")
    path = os.path.join(SAVE_PATH, f"crawled_on_{crawl_date}")
    print(f"Path to save crawled data: {path}")
    if not os.path.exists(path):
        os.makedirs(path)

    # # đường dẫn lưu logs
    # logs_path = os.path.join(path, "logs")
    # create_folder_if_not_exists(logs_path)

    # # crawl dữ liệu lịch sử 5 năm cho toàn bộ các active quotes đã được crawl
    # for _ in range(MAX_ATTEMP):
    #     crawler = HistoryCrawler()

    #     # tìm file logs mới nhất, nếu không có thì crawl lại từ đầu
    #     log_files = [f for f in os.listdir(logs_path) if f.endswith('.json')]
    #     log_files.sort(reverse=True)  # sắp xếp theo thứ tự giảm dần
    #     if log_files:
    #         latest_log_file = log_files[0]
    #         log_file_name = os.path.splitext(latest_log_file)[0]
    #         attempt = int(log_file_name.split("_")[-1]) + 1  # thứ tự của lần crawl
    #         print(f"\nĐang sử dụng lại file logs: {latest_log_file} (attempt {attempt})")
    #         # đọc nội dung file logs
    #         with open(os.path.join(logs_path, latest_log_file), 'r') as f:
    #             logs = json.load(f)
    #         tickers = logs.get("need_to_crawl_again", [])
    #         if not tickers:
    #             print("\nKhông có mã nào cần crawl lại, kết thúc quá trình.")
    #             break  # nếu không có mã nào cần crawl lại thì kết thúc vòng lặp
    #     else:
    #         print("\nKhông tìm thấy file logs, crawl toàn bộ mã")
    #         attempt = 1  # nếu chưa có file logs nào thì đây là lần crawl đầu tiên
    #         most_active_quotes_path = f"./data_test/crawl_active_tickers/crawled_on_{crawl_date}/most_active_quotes_parsed.csv"  
    #         active_quotes_df = pd.read_csv(most_active_quotes_path)
    #         tickers = active_quotes_df['symbol'].unique().tolist()

    #         # trong thực tế, chỉ thực hiện crawl 1 lần cho những mã chưa xuất hiện trong database, các mã đã có chỉ crawl daily
    #         # -> cần thêm logic xử lí sau này

    #     print(f"\nAttempt {attempt} - Tổng số mã cần crawl: {len(tickers)}")
        
    #     # bắt đầu crawl dữ liệu lịch sử
    #     crawler.crawl_all_daily_histories(tickers, path)
    #     print("\nCrawling completed.")
    #     crawler.quit()


        # # parse dữ liệu và lưu vào csv
        # parser = HistoryParser()
        # parse_results = parser.parse_all_html(path=path, tickers=tickers)

        # # ghi lại logs
        # log_file_path = os.path.join(logs_path, f"attempt_{attempt}.json")
        # with open(log_file_path, 'w') as f:
        #     f.write(json.dumps(parse_results, indent=4, ensure_ascii=False))
        # print(f"\nĐã lưu log vào file: {log_file_path}")

        # print("\nParsing completed.")

    # end of crawling and parsing
    print("\n\n================== HISTORY CRAWLING COMPLETED ==================\n")

        