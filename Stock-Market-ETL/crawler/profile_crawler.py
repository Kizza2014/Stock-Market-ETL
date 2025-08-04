from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
from datetime import date
import os
from crawl_utils import save_html, save_to_csv, state_code_look_up
from bs4 import BeautifulSoup
import pandas as pd
import json


BASE_URL = "https://finance.yahoo.com/quote/"
SAVE_PATH = "./data_test/crawl_profile/"
MOST_ACTIVE_QUOTES_PATH = "./data_test/crawl_active_tickers/most_active_quotes_parsed.csv"  # đường dẫn đến file chứa các mã đã crawl
MAX_ATTEMPT = 5  # số lần thử tối đa khi crawl dữ liệu


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
            "full_address", "country", "description"
        ]

        # lấy danh sách các file HTML trong thư mục
        available_html = [file for file in os.listdir(path) if file.endswith('.html')]

        # khởi tạo danh sách để lưu dữ liệu
        rows_data = []
        parse_results = {
            "parse_date": date.today().strftime("%Y-%m-%d"),
            "total_tickers": len(available_html),
            "tickers": {},
            "need_to_crawl_again": []
        }

        for html_file in available_html:
            print(f"\nParsing {html_file}...")
            with open(os.path.join(path, html_file), "r", encoding="utf-8") as file:
                html = file.read()
                soup = BeautifulSoup(html, 'html.parser')
                ticker = html_file.split("_")[0].upper()  # lấy ticker từ tên file

                try:
                    # lấy tên công ty và ticker
                    company_name_tag = soup.find('h1', class_='yf-4vbjci').text.strip()  # ex: "NVIDIA Corporation (NVDA)"
                    company_name = ' '.join(company_name_tag.split(' ')[:-1]) # lấy phần tên công ty
                    company_ticker = ticker

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
                    if address_tag:
                        address_parts = [div.text.strip() for div in address_tag.find_all('div')]
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
                        full_address, country, description
                    ])

                    # in thông báo
                    print(f"Status: succeeded")
                    parse_results["tickers"][ticker] = "succeeded"
                    parse_results["total_succeeded"] = parse_results.get("total_succeeded", 0) + 1
                except Exception as e:
                    print(f"Status: failed")
                    parse_results["tickers"][ticker] = "failed"
                    parse_results["total_failed"] = parse_results.get("total_failed", 0) + 1
                    parse_results["need_to_crawl_again"].append(ticker) # thêm ticker vào danh sách cần crawl lại
                    continue

        # lưu dữ liệu vào file CSV
        save_path = os.path.join(path, "all_profiles_parsed.csv")
        save_to_csv(rows_data, schema, save_path)

        return parse_results

if __name__ == "__main__":
    print("\n\n================== PROFILE CRAWLING  ==================\n")
    
    # đường dẫn lưu rawl html và parsed csv
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

    for _ in range(MAX_ATTEMPT):
        # tìm file log mới nhất, nếu không thì crawl lại từ đầu
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


        ####### BẮT ĐẦU ########
        print(f"Attempt {attempt} - Tickers to crawl: {len(tickers)}")

        # crawl dữ liệu
        crawler = ProfileCrawler()
        for ticker in tickers:
            print(f"\nTicker: {ticker}")
            crawler.crawl_profile(ticker, path)
        print("\nCrawling completed.")
        crawler.quit()

        # trong thực tế, chỉ thực hiện crawl 1 lần cho những mã chưa xuất hiện trong database, các mã đã có chỉ crawl daily
        # -> cần thêm logic xử lí sau này

        # parse dữ liệu
        parser = ProfileParser()
        parse_results = parser.parse_all_html(path)

        # ghi lại log
        log_file_path = os.path.join(logs_path, f"attempt_{attempt}.json")
        with open(log_file_path, 'w') as f:
            f.write(json.dumps(parse_results, indent=4, ensure_ascii=False))
        print(f"\nĐã lưu log vào file: {log_file_path}")

        print("\nParsing completed.")

    print("\n\n================== PROFILE CRAWLING COMPLETED  ==================\n")
