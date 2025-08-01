from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
from datetime import date
import os
from crawl_utils import save_html, save_to_csv
from bs4 import BeautifulSoup


BASE_URL = "https://finance.yahoo.com/quote/"
SAVE_PATH = "./raw_data/crawl_statistics/"


class FinancialCrawler:
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
        # kiểm tra xem dữ liệu ngày đó đã được crawl chưa
        if not os.path.exists(path):
            print(f"Thư mục {path} không tồn tại")
            exit(1)
        if not os.listdir(path):
            print(f"Thư mục {path} không chứa file HTML nào.")
            exit(1)

        avalable_html = [file for file in os.listdir(path) if file.endswith('.html')]
        for html_file in avalable_html:
            ticker = html_file.split('_')[0]
            save_path = os.path.join(path, f"{ticker.upper()}_key_statistics_parsed.csv")

            print(f"\nParsing key statistics for {ticker} from {html_file}")
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

    def transpose_data(self, rows_data):
        # Chuyển đổi dữ liệu từ dạng hàng sang dạng cột
        transposed = list(map(list, zip(*rows_data)))
        return transposed


if __name__ == "__main__":
    # đường dẫn lưu rawl html và parsed csv
    crawl_date = date.today().strftime("%Y_%m_%d")
    path = os.path.join(SAVE_PATH, f"crawled_on_{crawl_date}")
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Đã tạo thư mục lưu trữ: {path}")

    # crawl dữ liệu
    tickers = ["NVDA"] # cần thay bằng danh sách active ticker đã crawl được 
    crawler = FinancialCrawler()
    for ticker in tickers:
        crawler.crawl_statistics(ticker, path)
    print("Crawling completed.")
    crawler.quit()

    # parse dữ liệu
    parser = FinancialParser()
    parser.parse_all_html(path)
    print("Parsing completed.")