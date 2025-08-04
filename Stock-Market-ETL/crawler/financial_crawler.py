import time
from datetime import date
import os
from crawl_utils import save_html, save_to_csv, create_folder_if_not_exists, check_valid_folder
from bs4 import BeautifulSoup
import json
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from base_crawler import BaseCrawler


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

    def crawl_financials(self, ticker, save_path, wait_time=5):
        try:
            # crawl income statement
            income_statement_url = os.path.join(BASE_URL, ticker, "financials")
            self.driver.get(income_statement_url)
            print(f"\nCrawling income statement from {self.driver.title}")
            time.sleep(wait_time)  # đợi trang load xong
            
            ## show quarterly data
            self.show_quarterly_data()

            html = self.driver.page_source
            html_path = os.path.join(save_path, "income_statement", f"{ticker.upper()}_income_statement.html")
            create_folder_if_not_exists(os.path.dirname(html_path))  # tạo thư mục nếu chưa có
            save_html(html, html_path)

            # crawl balance sheet
            balance_sheet_url = os.path.join(BASE_URL, ticker, "balance-sheet")
            self.driver.get(balance_sheet_url)
            print(f"\nCrawling balance sheet from {self.driver.title}")
            time.sleep(wait_time)  # đợi trang load xong

            ## show quarterly data
            self.show_quarterly_data()

            html = self.driver.page_source
            html_path = os.path.join(save_path, "balance_sheet", f"{ticker.upper()}_balance_sheet.html")
            create_folder_if_not_exists(os.path.dirname(html_path))  # tạo thư mục nếu chưa có
            save_html(html, html_path)

            # crawl cash flow
            cash_flow_url = os.path.join(BASE_URL, ticker, "cash-flow")
            self.driver.get(cash_flow_url)
            print(f"\nCrawling cash flow from {self.driver.title}")
            time.sleep(wait_time)  # đợi trang load xong

            ## show quarterly data
            self.show_quarterly_data()

            html = self.driver.page_source
            html_path = os.path.join(save_path, "cash_flow", f"{ticker.upper()}_cash_flow.html")
            create_folder_if_not_exists(os.path.dirname(html_path))  # tạo thư mục nếu chưa có
            save_html(html, html_path)
        except Exception as e:
            print(f"Error: no financials data found for {ticker}")

    def quit(self):
        self.driver.quit()


class FinancialParser:
    def parse_all_html(self, path):
        check_valid_folder(path)

        # parse income statement
        income_statement_path = os.path.join(path, "income_statement")
        income_statement_results = self.parse_transposed_table(income_statement_path, data_type="income_statement")

        # parse balance sheet
        balance_sheet_path = os.path.join(path, "balance_sheet")
        balance_sheet_results = self.parse_transposed_table(balance_sheet_path, data_type="balance_sheet")

        # parse cash flow
        cash_flow_path = os.path.join(path, "cash_flow")
        cash_flow_results = self.parse_transposed_table(cash_flow_path, data_type="cash_flow")

        return income_statement_results, balance_sheet_results, cash_flow_results

    def parse_transposed_table(self, html_path, data_type):
        available_html = [file for file in os.listdir(html_path) if file.endswith('.html')]
        parse_results = {
            "parse_date": date.today().strftime("%Y-%m-%d"),
            "data_type": data_type,
            "total_tickers": len(available_html),
            "tickers": {},
            "need_to_crawl_again": []
        }
        for html_file in available_html:
            ticker = html_file.split('_')[0]
            save_path = os.path.join(html_path, f"{ticker.upper()}_{data_type}_parsed.csv")
            
            print(f"\nParsing {data_type} for {ticker} from {html_file}")
            with open(os.path.join(html_path, html_file), 'r', encoding='utf-8') as f:
                html = f.read()
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

                rows_data.insert(0, time_row)  # Thêm time_row vào đầu danh sách

                # chuyển đổi dữ liệu từ dạng hàng sang dạng cột
                transposed_data = self.transpose_data(rows_data)
                schema = transposed_data[0]  # lấy schema từ hàng đầu tiên
                transposed_data = transposed_data[1:]  # loại bỏ schema khỏi dữ liệu

                print(f"Tổng số dòng: {len(transposed_data)}")
                save_to_csv(transposed_data, schema, save_path)

                # ghi log
                print("Status: succeeded")
                parse_results["tickers"][ticker] = "succeeded"
                parse_results["total_succeeded"] = parse_results.get("total_succeeded", 0) + 1
            except Exception as e:
                print("Status: failed")
                parse_results["tickers"][ticker] = "failed"
                parse_results["total_failed"] = parse_results.get("total_failed", 0) + 1
                parse_results["need_to_crawl_again"].append(ticker)
            
        return parse_results
            

    def transpose_data(self, rows_data):
        # Chuyển đổi dữ liệu từ dạng hàng sang dạng cột
        transposed = list(map(list, zip(*rows_data)))
        return transposed


if __name__ == "__main__":
    print("\n\n================== FINANCIALS CRAWLING ==================\n")

    # đường dẫn lưu rawl html và parsed csv
    crawl_date = date.today().strftime("%Y_%m_%d")
    print(f"Crawling date: {crawl_date}")
    path = os.path.join(SAVE_PATH, f"crawled_on_{crawl_date}")
    print(f"Path to save crawled data: {path}")
    # tạo thư mục lưu từng loại dữ liệu
    create_folder_if_not_exists(path)

    # đường dẫn lưu logs
    logs_path = os.path.join(path, "logs")
    create_folder_if_not_exists(logs_path)

    # bắt đầu crawl với số lần retry tối đa
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


        print(f"\nAttempt {attempt} - Tickers to crawl: {len(tickers)}")
        # crawl dữ liệu
        crawler = FinancialCrawler()
        for ticker in tickers:
            print(f"\nTicker {ticker}:")
            crawler.crawl_financials(ticker, path)
        print("\nCrawling completed.")
        crawler.quit()

        # parse dữ liệu
        parser = FinancialParser()
        income_statement_results, balance_sheet_results, cash_flow_results = parser.parse_all_html(path)
        print("\nParsing completed.")

        # ghi lại logs
        with open(os.path.join(logs_path, f"income_statement_attempt_{attempt}.json"), 'w') as f:
            f.write(json.dumps(income_statement_results, indent=4))
        with open(os.path.join(logs_path, f"balance_sheet_attempt_{attempt}.json"), 'w') as f:
            f.write(json.dumps(balance_sheet_results, indent=4))
        with open(os.path.join(logs_path, f"cash_flow_attempt_{attempt}.json"), 'w') as f:
            f.write(json.dumps(cash_flow_results, indent=4))
        print(f"\nLogs saved for attempt {attempt}.")


    print("\n\n================== FINANCIALS CRAWLING COMPLETED  ==================\n")
