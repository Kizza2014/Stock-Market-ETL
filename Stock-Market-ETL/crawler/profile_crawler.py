from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
from datetime import date
import os
from crawl_utils import save_html, save_to_csv, state_code_look_up
from bs4 import BeautifulSoup


BASE_URL = "https://finance.yahoo.com/quote/"
SAVE_PATH = "./sample_data/crawl_profile/"


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
        schema = [
            "company_name", "company_ticker", "exchange_name", "sector", "industry", "phone", "website",
            "street_address", "city", "state", "zip_code", "country", "full_address", "description"
        ]
        rows_data = []

        available_html = [file for file in os.listdir(path) if file.endswith('.html')]
        for html_file in available_html:
            with open(os.path.join(path, html_file), "r", encoding="utf-8") as file:
                html = file.read()
                soup = BeautifulSoup(html, 'html.parser')

                # lấy tên công ty và ticker
                company_name_tag = soup.find('h1', class_='yf-4vbjci').text.strip()  # ex: "NVIDIA Corporation (NVDA)"
                company_name = ' '.join(company_name_tag.split(' ')[:-1]) # lấy phần tên công ty
                company_ticker = company_name_tag.split(' ')[-1][1:-1]

                # lấy tên sàn giao dịch
                exchange_tag = soup.find('span', class_='exchange yf-15jhhmp')  # ex: "NASDAQ - NASDAQ Real Time Price"
                exchange_name = exchange_tag.find('span').text.strip().split("-")[0].strip()

                # lấy sector và industry
                sector_industry_tags = soup.find_all('a', class_='subtle-link fin-size-large yf-u4gyzs')
                sector = sector_industry_tags[0].text.strip() if len(sector_industry_tags) > 0 else ""
                industry = sector_industry_tags[1].text.strip() if len(sector_industry_tags) > 1 else ""

                # lấy số điện thoại
                phone_tag = soup.find('a', class_='primary-link fin-size-small noUnderline yf-u4gyzs', href=True)
                phone_number = phone_tag.text.strip() if phone_tag and phone_tag['href'].startswith('tel:') else ""

                # lấy website
                website_tag = soup.find(
                    'a',
                    class_='primary-link fin-size-small noUnderline yf-u4gyzs',
                    href=True,
                    attrs={'aria-label': 'website link'}
                )
                website = website_tag.text.strip() if website_tag else ""

                # lấy địa chỉ
                address_tag = soup.find('div', class_='address yf-wxp4ja')
                address_parts = [div.text.strip() for div in address_tag.find_all('div')]

                ## street address
                street_address = address_parts[0] if len(address_parts) > 0 else ""
                ## city, state, zip code
                city_state_zip = address_parts[1] if len(address_parts) > 1 else "" # ex: "Santa Clara, CA 95051"
                city_state_zip_parts = city_state_zip.split(',')
                city = city_state_zip_parts[0].strip()
                state_zip_parts = city_state_zip_parts[1].strip().split(' ')
                state = state_code_look_up(state_zip_parts[0])
                zip_code = state_zip_parts[1]
                ## country
                country = address_parts[2] if len(address_parts) > 2 else ""
                ## thêm 1 trường full address nếu cần
                full_address = ', '.join(address_parts)

                # lấy description
                description_tag = soup.find('section', {'data-testid': 'description'})
                if description_tag:
                    description_p = description_tag.find('p')
                    description = description_p.text.strip() if description_p else ""
                else:
                    description = ""

                # thêm thông tin công ty vào danh sách
                rows_data.append([
                    company_name, company_ticker, exchange_name, sector, industry, phone_number, website,
                    street_address, city, state, zip_code, country, full_address, description
                ])

        # lưu dữ liệu vào file CSV
        save_path = os.path.join(path, "all_profiles_parsed.csv")
        save_to_csv(rows_data, schema, save_path)

if __name__ == "__main__":
    # đường dẫn lưu rawl html và parsed csv
    crawl_date = date.today().strftime("%Y_%m_%d")
    path = os.path.join(SAVE_PATH, f"crawled_on_{crawl_date}")
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Đã tạo thư mục lưu trữ: {path}")

    # crawl dữ liệu
    tickers = ["NVDA"] # cần thay bằng danh sách active ticker đã crawl được 
    crawler = ProfileCrawler()
    for ticker in tickers:
        crawler.crawl_profile(ticker, path)
    print("Crawling completed.")
    crawler.quit()

    # parse dữ liệu
    parser = ProfileParser()
    parser.parse_all_html(path)
    print("Parsing completed.")