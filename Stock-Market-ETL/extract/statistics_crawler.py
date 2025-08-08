import time
from datetime import date
import os
import json
import pandas as pd
from .base_crawler import BaseCrawler
from urllib.parse import urljoin


BASE_URL = "https://finance.yahoo.com/quote/"
SAVE_PATH = "./data_test/crawl_statistics/"
MAX_ATTEMPT = 5


class StatisticsCrawler(BaseCrawler):
    def __init__(self):
        super().__init__()

    def crawl_statistics(self, tickers, save_path, wait_time=5):
        tickers = [ticker.upper() for ticker in tickers]  # chuyển tất cả mã thành chữ hoa
        for ticker in tickers:
            # crawl key statistics
            url = urljoin(BASE_URL, urljoin(ticker, "key-statistics")) # url riêng của từng ticker
            print(f"\nTicker {ticker}: {url}")
            self.driver.get(url)
            print(f"Crawling key statistics from {self.driver.title}")
            time.sleep(wait_time)  # đợi trang load xong
            
            html = self.driver.page_source
            html_path = os.path.join(save_path, f"{ticker}_key_statistics.html")
            # save_html(html, html_path)


if __name__ == "__main__":
    pass
    # print("\n\n================== STATISTICS CRAWLING ==================\n")

    # # đường dẫn lưu rawl html và parsed csv
    # crawl_date = date.today().strftime("%Y_%m_%d")
    # path = os.path.join(SAVE_PATH, f"crawled_on_{crawl_date}")
    # create_folder_if_not_exists(path)

    # # đường dẫn lưu logs
    # logs_path = os.path.join(path, "logs")
    # create_folder_if_not_exists(logs_path)

    # for _ in range(MAX_ATTEMPT):
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

    #     print(f"Attempt {attempt} - Tickers to crawl: {len(tickers)}")
    #     # crawl dữ liệu
    #     crawler = StatisticsCrawler()
    #     crawler.crawl_statistics(tickers, path)
    #     print("Crawling completed.")
    #     crawler.quit()

    #     # parse dữ liệu
    #     parser = StatisticsParser()
    #     parse_results = parser.parse_all_html(path)
    #     print("Parsing completed.")

    #     # lưu logs
    #     log_file_path = os.path.join(logs_path, f"attempt_{attempt}.json")
    #     with open(log_file_path, 'w') as f:
    #         f.write(json.dumps(parse_results, indent=4, ensure_ascii=False))
    #     print(f"\nĐã lưu log vào file: {log_file_path}") 

    # print("\n\n================== STATISTICS CRAWLING COMPLETED ==================\n")
    