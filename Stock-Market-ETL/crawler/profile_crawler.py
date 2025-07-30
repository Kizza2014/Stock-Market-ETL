from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
from datetime import date
import os
from crawl_utils import save_html, save_to_csv
from bs4 import BeautifulSoup


BASE_URL = "https://finance.yahoo.com/quote/"
SAVE_PATH = "./sample_html/crawl_profile/"

class ProfileCrawler:
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

    def crawl_profile(self, ticker, save_path, wait_time=5):
        url = os.path.join(BASE_URL, ticker, "profile")
        self.driver.get(url)
        print(f"Crawling company profile from {self.driver.title}")

        # lấy toàn bộ HTML sau khi đã render và lưu lại
        time.sleep(wait_time)  # đợi trang load xong
        html = self.driver.page_source
        html_path = os.path.join(save_path, f"{ticker.upper()}_profile.html")
        save_html(html, html_path)

    def quit(self):
        self.driver.quit()


class ProfileParser:
    def parse_all_html(self, path):
        # kiểm tra xem dữ liệu ngày đó đã được crawl chưa
        if not os.path.exists(path):
            print(f"Thư mục {path} không tồn tại")
            exit(1)
        if not os.listdir(path):
            print(f"Thư mục {path} không chứa file HTML nào.")
            exit(1)

        # chỉ định các thông tin cần lấy từ html
        schema = [""]
        rows_data = []

        available_html = [file for file in os.listdir(path) if file.endswith('.html')]
        for html_file in available_html:
            with open(os.path.join(path, html_file), "r", encoding="utf-8") as file:
                html = file.read()
                soup = BeautifulSoup(html, 'html.parser')

                # lấy tên công ty
                company_name_tag = soup.find('h1', class_='yf-4vbjci').text.strip()  # ex: "NVIDIA Corporation (NVDA)"
                company_name = company_name_tag.split(' ')[0]
                company_ticker = company_name_tag.split(' ')[-1][1:-2]

                # lấy mô tả công ty
                description = soup.find('section', {'data-test': 'qsp-profile'}).find('p').text.strip() if soup.find('section', {'data-test': 'qsp-profile'}) else "N/A"
                schema.append("Description")
                rows_data[-1].append(description)

        # lưu dữ liệu vào file CSV
        save_path = os.path.join(path, "all_profiles_parsed.csv")
        save_to_csv(rows_data, schema, save_path)

if __name__ == "__main__":
    # đường dẫn lưu rawl html và parsed csv
    crawl_date = date.today().strftime("%Y_%m_%d")
    path = os.path.join(SAVE_PATH, f"crawled_at_{crawl_date}")
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Đã tạo thư mục lưu trữ: {path}")

    # crawl dữ liệu
    crawler = ProfileCrawler()
    ticker = "NVDA"  # Example ticker
    crawler.crawl_profile(ticker, path)
    print("Crawling completed.")
    crawler.quit()