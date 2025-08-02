from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
from datetime import date
import os
from crawl_utils import save_html, save_to_csv
from bs4 import BeautifulSoup


BASE_URL = "https://finance.yahoo.com/quote/"
SAVE_PATH = "./sample_data/crawl_financials/"


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

    def crawl_financials(self, ticker, save_path, wait_time=5):
        # crawl income statement
        income_statement_url = os.path.join(BASE_URL, ticker, "financials")
        self.driver.get(income_statement_url)
        print(f"Crawling income statement from {self.driver.title}")
        time.sleep(wait_time)  # đợi trang load xong
        
        ## show quarterly data
        quarterly_btn = self.driver.find_element("id", "tab-quarterly")
        quarterly_btn.click()
        print("Đã bấm nút Quarterly.")
        time.sleep(2)  # đợi dữ liệu quarterly load lại

        html = self.driver.page_source
        html_path = os.path.join(save_path, "income_statement", f"{ticker.upper()}_income_statement.html")
        if not os.path.exists(os.path.dirname(html_path)):
            os.makedirs(os.path.dirname(html_path))
            print(f"Đã tạo thư mục lưu trữ: {os.path.dirname(html_path)}")
        save_html(html, html_path)

        # crawl balance sheet
        balance_sheet_url = os.path.join(BASE_URL, ticker, "balance-sheet")
        self.driver.get(balance_sheet_url)
        print(f"Crawling balance sheet from {self.driver.title}")
        time.sleep(wait_time)  # đợi trang load xong

        ## show quarterly data
        quarterly_btn = self.driver.find_element("id", "tab-quarterly")
        quarterly_btn.click()
        print("Đã bấm nút Quarterly.")
        time.sleep(2)  # đợi dữ liệu quarterly load lại

        html = self.driver.page_source
        html_path = os.path.join(save_path, "balance_sheet", f"{ticker.upper()}_balance_sheet.html")
        if not os.path.exists(os.path.dirname(html_path)):
            os.makedirs(os.path.dirname(html_path))
            print(f"Đã tạo thư mục lưu trữ: {os.path.dirname(html_path)}")
        save_html(html, html_path)

        # crawl cash flow
        cash_flow_url = os.path.join(BASE_URL, ticker, "cash-flow")
        self.driver.get(cash_flow_url)
        print(f"Crawling cash flow from {self.driver.title}")
        time.sleep(wait_time)  # đợi trang load xong

        ## show quarterly data
        quarterly_btn = self.driver.find_element("id", "tab-quarterly")
        quarterly_btn.click()
        print("Đã bấm nút Quarterly.")
        time.sleep(2)  # đợi dữ liệu quarterly load lại

        html = self.driver.page_source
        html_path = os.path.join(save_path, "cash_flow", f"{ticker.upper()}_cash_flow.html")
        if not os.path.exists(os.path.dirname(html_path)):
            os.makedirs(os.path.dirname(html_path))
            print(f"Đã tạo thư mục lưu trữ: {os.path.dirname(html_path)}")
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

        # parse income statement
        income_statement_path = os.path.join(path, "income_statement")
        self.parse_transposed_table(income_statement_path, data_type="income_statement")

        # parse balance sheet
        balance_sheet_path = os.path.join(path, "balance_sheet")
        self.parse_transposed_table(balance_sheet_path, data_type="balance_sheet")

        # parse cash flow
        cash_flow_path = os.path.join(path, "cash_flow")
        self.parse_transposed_table(cash_flow_path, data_type="cash_flow")

    def parse_transposed_table(self, html_path, data_type):
        avalable_html = [file for file in os.listdir(html_path) if file.endswith('.html')]
        for html_file in avalable_html:
            ticker = html_file.split('_')[0]
            save_path = os.path.join(html_path, f"{ticker.upper()}_{data_type}_parsed.csv")
            
            print(f"Parsing {data_type} for {ticker} from {html_file}")
            with open(os.path.join(html_path, html_file), 'r', encoding='utf-8') as f:
                html = f.read()
            soup = BeautifulSoup(html, 'html.parser')
            
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
        crawler.crawl_financials(ticker, path)
    print("Crawling completed.")
    crawler.quit()

    # parse dữ liệu
    parser = FinancialParser()
    parser.parse_all_html(path)
    print("Parsing completed.")