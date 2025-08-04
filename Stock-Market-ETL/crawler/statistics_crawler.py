import time
from datetime import date
import os
from crawl_utils import save_html, save_to_csv, check_valid_folder, create_folder_if_not_exists
from bs4 import BeautifulSoup
import json
import pandas as pd
from base_crawler import BaseCrawler


BASE_URL = "https://finance.yahoo.com/quote/"
SAVE_PATH = "./data_test/crawl_statistics/"
MAX_ATTEMPT = 5


class FinancialCrawler(BaseCrawler):
    def __init__(self):
        super().__init__()

    def crawl_statistics(self, ticker, save_path, wait_time=5):
        # crawl key statistics
        url = os.path.join(BASE_URL, ticker, "key-statistics")
        self.driver.get(url)
        print(f"Crawling key statistics from {self.driver.title}")
        time.sleep(wait_time)  # đợi trang load xong
        
        html = self.driver.page_source
        html_path = os.path.join(save_path, f"{ticker.upper()}_key_statistics.html")
        save_html(html, html_path)

    def quit(self):
        self.driver.quit()


class FinancialParser:
    def parse_all_html(self, path):
        check_valid_folder(path)

        avalable_html = [file for file in os.listdir(path) if file.endswith('.html')]
        parse_results = {
            "parse_date": date.today().strftime("%Y-%m-%d"),
            "data_type": "key_statistics",
            "total_tickers": len(avalable_html),
            "tickers": {},
            "need_to_crawl_again": [],
            "total_succeeded": 0,
            "total_failed": 0
        }
        for html_file in avalable_html:
            ticker = html_file.split('_')[0]
            save_path = os.path.join(path, f"{ticker.upper()}_key_statistics_parsed.csv")

            print(f"\nParsing key statistics for {ticker} from {html_file}")
            try:
                with open(os.path.join(path, html_file), 'r', encoding='utf-8') as f:
                    html = f.read()
                soup = BeautifulSoup(html, 'html.parser')
                
                # Tìm container bảng
                table_container = soup.find('div', class_='table-container')
                if not table_container:
                    print(f"Không tìm thấy bảng trong file {html_file}.")
                    continue

                # Tìm bảng <table>
                table = table_container.find('table')
                if not table:
                    print(f"Không tìm thấy thẻ <table> trong file {html_file}.")
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

                rows_data.insert(0, time_row)  # Thêm header vào đầu danh sách
                transposed_data = self.transpose_data(rows_data)
                header_row = transposed_data[0]  # Lấy header từ dữ liệu đã chuyển đổi
                transposed_data = transposed_data[1:]  # Bỏ header ra khỏi dữ liệu

                # Lưu ra CSV
                save_to_csv(transposed_data, header_row, save_path)

                print(f"Status: succeeded")
                parse_results["tickers"][ticker] = "succeeded"
                parse_results["total_succeeded"] += 1
            except Exception as e:
                print(f"Status: failed")
                parse_results["need_to_crawl_again"].append(ticker)
                parse_results["tickers"][ticker] = "failed"
                parse_results["total_failed"] += 1
                continue

        return parse_results

    def transpose_data(self, rows_data):
        # Chuyển đổi dữ liệu từ dạng hàng sang dạng cột
        transposed = list(map(list, zip(*rows_data)))
        return transposed


if __name__ == "__main__":
    print("\n\n================== STATISTICS CRAWLING ==================\n")

    # đường dẫn lưu rawl html và parsed csv
    crawl_date = date.today().strftime("%Y_%m_%d")
    path = os.path.join(SAVE_PATH, f"crawled_on_{crawl_date}")
    create_folder_if_not_exists(path)

    # đường dẫn lưu logs
    logs_path = os.path.join(path, "logs")
    create_folder_if_not_exists(logs_path)

    for _ in range(MAX_ATTEMPT):
        # tìm file logs mới nhất, nếu không có thì crawl lại từ đầu
        log_files = [f for f in os.listdir(logs_path) if f.endswith('.json')]
        log_files.sort(reverse=True)  # sắp xếp theo thứ tự giảm dần
        if log_files:
            latest_log_file = log_files[0]
            log_file_name = os.path.splitext(latest_log_file)[0]
            attempt = int(log_file_name.split("_")[-1]) + 1  # thứ tự của lần crawl
            print(f"\nĐang sử dụng lại file logs: {latest_log_file} (attempt {attempt})")
            # đọc nội dung file logs
            with open(os.path.join(logs_path, latest_log_file), 'r') as f:
                logs = json.load(f)
            tickers = logs.get("need_to_crawl_again", [])
            if not tickers:
                print("\nKhông có mã nào cần crawl lại, kết thúc quá trình.")
                break  # nếu không có mã nào cần crawl lại thì kết thúc vòng lặp
        else:
            print("\nKhông tìm thấy file logs, crawl toàn bộ mã")
            attempt = 1  # nếu chưa có file logs nào thì đây là lần crawl đầu tiên
            most_active_quotes_path = f"./data_test/crawl_active_tickers/crawled_on_{crawl_date}/most_active_quotes_parsed.csv"  
            active_quotes_df = pd.read_csv(most_active_quotes_path)
            tickers = active_quotes_df['symbol'].unique().tolist()

            # trong thực tế, chỉ thực hiện crawl 1 lần cho những mã chưa xuất hiện trong database, các mã đã có chỉ crawl daily
            # -> cần thêm logic xử lí sau này

        print(f"Attempt {attempt} - Tickers to crawl: {len(tickers)}")
        # crawl dữ liệu
        crawler = FinancialCrawler()
        for ticker in tickers:
            print(f"\nTicker {ticker.upper()}:")
            crawler.crawl_statistics(ticker, path)
        print("Crawling completed.")
        crawler.quit()

        # parse dữ liệu
        parser = FinancialParser()
        parse_results = parser.parse_all_html(path)
        print("Parsing completed.")

        # lưu logs
        log_file_path = os.path.join(logs_path, f"attempt_{attempt}.json")
        with open(log_file_path, 'w') as f:
            f.write(json.dumps(parse_results, indent=4, ensure_ascii=False))
        print(f"\nĐã lưu log vào file: {log_file_path}") 

    print("\n\n================== STATISTICS CRAWLING COMPLETED ==================\n")
    