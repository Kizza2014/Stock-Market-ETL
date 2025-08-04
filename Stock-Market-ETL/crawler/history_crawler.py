from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
from datetime import date
import os
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from crawl_utils import save_html, save_to_csv
import pandas as pd
import json


BASE_URL = "https://finance.yahoo.com/quote/"
SAVE_PATH = "./data_test/crawl_5_years_history/" # lưu trữ trên local, sau này sẽ thay bằng đường dẫn đến S3 bucket
MAX_ATTEMP = 5  # số lần thử tối đa khi crawl dữ liệu lịch sử


class HistoryCrawler:
    def __init__(self):
        self.driver = self.setup_driver()

    def setup_driver(self):
        options = Options()
        options.add_argument("--start-maximized")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-extensions")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--ignore-certificate-errors")
        options.add_argument("--ignore-ssl-errors")
        options.add_argument("--disable-web-security")
        options.add_argument("--allow-running-insecure-content")
        options.add_argument('--headless=new')
        return webdriver.Chrome(options=options)

    def crawl_all_daily_histories(self, ticker, save_path, wait_time=20):
        url = os.path.join(BASE_URL, ticker, "history")
        self.driver.get(url)
        print(f"Crawling all daily histories from {self.driver.title}")

        # chuyển đến phần xem dữ liệu 5 năm gần nhất
        self.show_5_years_histories()

        # đợi render đầy đủ bảng lịch sử
        time.sleep(wait_time)

        # lấy toàn bộ HTML sau khi đã render
        html = self.driver.page_source

        # lưu lại html
        html_path = os.path.join(save_path, f"{ticker.upper()}_history.html")
        save_html(html, html_path)

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

    def quit(self):
        self.driver.quit()


class HistoryParser:
    def normalize_schema(self, schema):
        # Chuyển đổi schema về dạng chữ thường và loại bỏ khoảng trắng
        return [col.lower().replace(' ', '_') for col in schema]

    def parse_html(self, html_path, save_path):
        with open(html_path, 'r', encoding='utf-8') as f:
            html = f.read()
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table')
        rows_data = []
        if table:
            rows = table.find_all('tr')

            # Lấy schema từ hàng đầu tiên
            schema = [cell.get_text(strip=True) for cell in rows[0].find_all(['th'])]
            schema = self.normalize_schema(schema)

            for row in rows[1:]:
                cells = row.find_all(['td'])
                cell_values = [cell.get_text(strip=True) for cell in cells]
                if cell_values:
                    rows_data.append(cell_values)
            print(f"Tổng số dòng: {len(rows_data) - 1}") # trừ đi header row
        
        # lưu dữ liệu vào file CSV
        save_to_csv(rows_data, schema, save_path)

        return len(rows_data) > 0 # nếu có dữ liệu, ngược lại tính là fail


    def parse_all_html(self, path, tickers):
        # kiểm tra xem dữ liệu ngày đó đã được crawl chưa
        if not os.path.exists(path):
            print(f"Thư mục {path} không tồn tại.")
            exit(1)
        if not os.listdir(path):
            print(f"Thư mục {path} không chứa file HTML nào.")
            exit(1)

        # parse html cho từng mã và lưu vào file csv
        available_html = [file for file in os.listdir(path) if file.endswith('.html') and file.split('_')[0] in tickers]
        parse_results = {
            "parse_date": date.today().strftime("%Y-%m-%d"),
            "total_tickers": len(available_html),
            "tickers": {}
        }
        for html_file in available_html:
            # lấy ticker từ tên file
            ticker = html_file.split('_')[0]

            # kết quả được lưu cùng đường dẫn với file HTML
            save_path = os.path.join(path, f"{ticker.upper()}_history_parsed.csv")
            
            print(f"\nParsing file: {html_file}")
            html_path = os.path.join(path, html_file)
            parse_status = self.parse_html(html_path, save_path)
            print(f"Status: {'succeeded' if parse_status else 'failed'}")
            parse_results["tickers"][html_file.split("_")[0]] = "succeeded" if parse_status else "failed"
        print(f"\nĐã xử lý xong {len(available_html)} file HTML.")

        parse_results["total_succeeded"] = sum(1 for status in parse_results["tickers"].values() if status == "succeeded")
        parse_results["total_failed"] = sum(1 for status in parse_results["tickers"].values() if status == "failed")
        parse_results["need_to_crawl_again"] = [file.split("_")[0] for file, status in parse_results["tickers"].items() if status == "failed"]
        return parse_results


if __name__ == "__main__":
    print("\n\n================== HISTORY CRAWLING ==================\n")

    # đường dẫn dùng để lưu raw html và parsed csv
    crawl_date = date.today().strftime("%Y_%m_%d")
    print(f"Crawling date: {crawl_date}")
    path = os.path.join(SAVE_PATH, f"crawled_on_{crawl_date}")
    print(f"Path to save crawled data: {path}")
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Đã tạo thư mục lưu trữ: {path}")

    # đường dẫn lưu logs
    logs_path = os.path.join(path, "logs")
    if not os.path.exists(logs_path):
        os.makedirs(logs_path)
        print(f"Đã tạo thư mục logs: {logs_path}")

    # crawl dữ liệu lịch sử 5 năm cho toàn bộ các active quotes đã được crawl
    for _ in range(MAX_ATTEMP):
        crawler = HistoryCrawler()

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

        print(f"\nAttempt {attempt} - Tổng số mã cần crawl: {len(tickers)}")
        for ticker in tickers:
            print(f"\nTicker: {ticker}")
            crawler.crawl_all_daily_histories(ticker, path)
        print("\nCrawling completed.")
        crawler.quit()


        # parse dữ liệu và lưu vào csv
        parser = HistoryParser()
        parse_results = parser.parse_all_html(path=path, tickers=tickers)

        # ghi lại logs
        log_file_path = os.path.join(logs_path, f"attempt_{attempt}.json")
        with open(log_file_path, 'w') as f:
            f.write(json.dumps(parse_results, indent=4, ensure_ascii=False))
        print(f"\nĐã lưu log vào file: {log_file_path}")

        print("\nParsing completed.")

    # end of crawling and parsing
    print("\n\n================== HISTORY CRAWLING COMPLETED ==================\n")

        