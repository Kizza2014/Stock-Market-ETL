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


BASE_URL = "https://finance.yahoo.com/quote/"
SAVE_PATH = "./raw_data/crawl_5_years_history/" # lưu trữ trên local, sau này sẽ thay bằng đường dẫn đến S3 bucket


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

    def show_5_years_histories(self):
        try:
            # click vào tuỳ chọn thời gian
            wait = WebDriverWait(self.driver, 20)
            button1 = wait.until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, "button.tertiary-btn.menuBtn.yf-1epmntv")
                )
            )
            button1.click()
            print("Đã click vào nút chọn khoảng thời gian!")

            # chọn dữ liệu 5 năm gần nhất
            wait = WebDriverWait(self.driver, 20)
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

    def parse_all_html(self, path):
        # kiểm tra xem dữ liệu ngày đó đã được crawl chưa
        if not os.path.exists(path):
            print(f"Thư mục {path} không tồn tại.")
            exit(1)
        if not os.listdir(path):
            print(f"Thư mục {path} không chứa file HTML nào.")
            exit(1)

        # parse html cho từng mã và lưu vào file csv
        available_html = [file for file in os.listdir(path) if file.endswith('.html')]
        for html_file in available_html:
            # lấy ticker từ tên file
            ticker = html_file.split('_')[0]

            # kết quả được lưu cùng đường dẫn với file HTML
            save_path = os.path.join(path, f"{ticker.upper()}_history_parsed.csv")
            
            print(f"Parsing file: {html_file}")
            html_path = os.path.join(path, html_file)
            self.parse_html(html_path, save_path)
        print(f"Đã xử lý xong {len(available_html)} file HTML.")


if __name__ == "__main__":
    # đường dẫn dùng để lưu raw html và parsed csv
    crawl_date = date.today().strftime("%Y_%m_%d")
    path = os.path.join(SAVE_PATH, f"crawled_on_{crawl_date}")
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Đã tạo thư mục lưu trữ: {path}")

    crawler = HistoryCrawler()
    ticker = "NVDA"  # Example ticker
    crawler.crawl_all_daily_histories(ticker, path)
    print("Crawling completed.")
    crawler.quit()

    # trong thực tế, chỉ thực hiện crawl 1 lần cho những mã chưa xuất hiện trong database, các mã đã có chỉ crawl daily
    # -> cần thêm logic xử lí sau này

    # parse dữ liệu và lưu vào csv
    parser = HistoryParser()
    parser.parse_all_html(path=path)
    print("Parsing completed.")

    