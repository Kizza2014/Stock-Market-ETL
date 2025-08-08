import time
from datetime import date
import os
import json
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from .base_crawler import BaseCrawler
from urllib.parse import urljoin


BASE_URL = "https://finance.yahoo.com/quote/"
SAVE_PATH = "./data_test/crawl_financials/"
MAX_ATTEMPT = 5


class FinancialCrawler(BaseCrawler):
    def __init__(self):
        super().__init__()

    def show_quarterly_data(self, wait_time=20):
        try:
            # Đợi nút quarterly xuất hiện rồi mới click
            quarterly_btn = WebDriverWait(self.driver, wait_time).until(
                EC.element_to_be_clickable((By.ID, "tab-quarterly"))
            )
            quarterly_btn.click()
            print("Đã bấm nút Quarterly.")
            time.sleep(2)  # đợi dữ liệu quarterly load lại
        except Exception as e:
            print(f"Error clicking Quarterly button: {str(e)}")

    def crawl_financials(self, tickers, save_path, wait_time=5):
        # endpoints cho từng loại dữ liệu 
        # eg: https://finance.yahoo.com/quote/AAPL/financials : dữ liệu income statement
        #     https://finance.yahoo.com/quote/AAPL/balance-sheet : dữ liệu balance sheet
        #     https://finance.yahoo.com/quote/AAPL/cash-flow : dữ liệu cash flow
        data_type_endpoints ={
            "income_statement": "financials",
            "balance_sheet": "balance-sheet",
            "cash_flow": "cash-flow"
        }
        tickers = [ticker.upper() for ticker in tickers]  # chuyển tất cả mã thành chữ hoa
        for ticker in tickers:
            print(f"\nTicker {ticker}:")
            try:
                for data_type, endpoint in data_type_endpoints.items():
                    # crawl loại dữ liệu tương ứng
                    url = urljoin(BASE_URL, urljoin(ticker, endpoint))
                    self.driver.get(url)
                    print(f"\nCrawling {data_type} from {self.driver.title}")
                    time.sleep(wait_time)  # đợi trang load xong
                    
                    ## bấm vào nút "Quarterly" để hiển thị dữ liệu theo từng quý
                    self.show_quarterly_data()

                    html = self.driver.page_source
                    html_path = os.path.join(save_path, data_type, f"{ticker}_{data_type}.html")
                    # create_folder_if_not_exists(os.path.dirname(html_path))  # tạo thư mục nếu chưa có
                    # save_html(html, html_path)
            except Exception as e:
                print(f"Error: no financials data found for {ticker}")

    def transpose_data(self, rows_data):
        # Chuyển đổi dữ liệu từ dạng hàng sang dạng cột
        transposed = list(map(list, zip(*rows_data)))
        return transposed


if __name__ == "__main__":
    pass
    # print("\n\n================== FINANCIALS CRAWLING ==================\n")

    # # đường dẫn lưu raw html và parsed csv
    # crawl_date = date.today().strftime("%Y_%m_%d")
    # print(f"Crawling date: {crawl_date}")
    # path = os.path.join(SAVE_PATH, f"crawled_on_{crawl_date}")
    # print(f"Path to save crawled data: {path}")
    # # tạo thư mục lưu từng loại dữ liệu
    # create_folder_if_not_exists(path)

    # # đường dẫn lưu logs
    # logs_path = os.path.join(path, "logs")
    # create_folder_if_not_exists(logs_path)

    # # bắt đầu crawl với số lần retry tối đa
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


    #     print(f"\nAttempt {attempt} - Tickers to crawl: {len(tickers)}")
    #     # crawl dữ liệu
    #     crawler = FinancialCrawler()
    #     crawler.crawl_financials(tickers, path)
    #     print("\nCrawling completed.")
    #     crawler.quit()

    #     # parse dữ liệu
    #     parser = FinancialParser()
    #     parse_results = parser.parse_all_html(path)
    #     print("\nParsing completed.")

    #     # ghi lại logs
    #     for data_type, result in parse_results.items():
    #         with open(os.path.join(logs_path, f"{data_type}_attempt_{attempt}.json"), 'w') as f:
    #             f.write(json.dumps(result, indent=4))
    #     print(f"\nLogs saved for attempt {attempt}.")


    # print("\n\n================== FINANCIALS CRAWLING COMPLETED  ==================\n")
