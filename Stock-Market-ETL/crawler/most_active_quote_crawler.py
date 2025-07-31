from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
from datetime import date
import os
from bs4 import BeautifulSoup
import csv


URL = "https://finance.yahoo.com/markets/stocks/most-active/"
SAVE_PATH = "./sample_html/crawl_active_tickers/" # lưu trữ trên local, sau này sẽ thay bằng đường dẫn đến S3 bucket


class MostActiveQuoteCrawler:
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

    def crawl_tickers_from_idx(self, url, start=0, count=50):
        # connect to the URL with pagination parameters
        url_with_params = os.path.join(url, f"?start={start}&count={count}")
        self.driver.get(url_with_params)
        print(f"Crawling {self.driver.title} from index {start} with count {count}")
        html = self.driver.page_source
        return html

    def crawl_all_tickers(self, url, save_path, count=50):
        # connect to the URL
        self.driver.get(url)
        print(f"Crawling all tickers from {self.driver.title}")

        # get the total number of most active tickers
        total = self.get_total()
        for i in range(0, total, count):
            html = self.crawl_tickers_from_idx(url, start=i, count=count)
            save_file = os.path.join(save_path, f"stocks_most_active_page{i // count + 1}.html")
            self.save_html(html, save_file)
            time.sleep(1) # Thêm thời gian chờ để tránh bị chặn

    def get_total(self):
        try:
            total_div = self.driver.find_element(By.CSS_SELECTOR, 'div.total.yf-1tdhqb1')
            total_text = total_div.text
            print(f"Total text: {total_text}")
            # lấy số cuối cùng (ví dụ: "1-80 of 80" -> 80)
            import re
            match = re.search(r'of (\d+)', total_text)
            if match:
                total_number = int(match.group(1))
                print(f"Tổng số: {total_number}")
                return total_number
            return None
        except Exception as e:
            print(f"Không tìm thấy tổng số: {e}")
            return None

    def save_html(self, html, save_path):
        with open(save_path, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"Đã lưu toàn bộ HTML vào {save_path}")

    def quit(self):
        self.driver.quit()


class MostActiveQuoteParser:
    def __init__(self):
        pass

    def normalize_schema(self, schema):
        # Chuyển đổi schema về dạng chữ thường và loại bỏ khoảng trắng
        return [col.lower().replace(' ', '_') for col in schema]

    def parse_html(self, html):
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
            print(f"Tổng số dòng: {len(rows_data)}")
        return rows_data, schema

    def parse_all_html(self, path):
        # kiểm tra xem dữ liệu ngày đó đã được crawl chưa
        if not os.path.exists(path):
            print(f"Thư mục {path} không tồn tại.")
            exit(1)
        if not os.listdir(path):
            print(f"Thư mục {path} không chứa file HTML nào.")
            exit(1)

        # kết quả được lưu cùng đường dẫn với html
        save_path = os.path.join(path, "most_active_quotes_parsed.csv")

        files = os.listdir(path)
        rows_data =[]
        schema = None
        for file in files:
            if file.endswith('.html'):
                print(f"Parsing file: {file}")
                with open(os.path.join(path, file), 'r', encoding='utf-8') as f:
                    html = f.read()
                parsed_data, parsed_schema = self.parse_html(html)

                # Kiểm tra schema
                if schema is None:
                    schema = parsed_schema
                if parsed_schema != schema:
                    print(f"Cảnh báo: Schema không khớp trong file {file}")
                rows_data.extend(parsed_data)
        
        if save_path:
            self.save_to_csv(rows_data, schema, save_path)
        print(f"Đã xử lí {len(rows_data)} dòng dữ liệu với schema: {schema}")
    
    def save_to_csv(self, rows_data, schema, save_path):
        if not rows_data or not schema:
            print("Không có dữ liệu để ghi ra file CSV.")
            return
        
        with open(save_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(schema)
            for row in rows_data:
                writer.writerow(row)
        print(f"Đã ghi dữ liệu ra file {save_path}")


if __name__ == "__main__":
    # đường dẫn lưu rawl html và parsed csv
    crawl_date = date.today().strftime("%Y_%m_%d")
    path = os.path.join(SAVE_PATH, f"crawled_on_{crawl_date}")
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Đã tạo thư mục lưu trữ: {path}")

    # crawl dữ liệu
    crawler = MostActiveQuoteCrawler()
    crawler.crawl_all_tickers(URL, save_path=path)
    print("Crawling completed.")
    crawler.quit()
    
    # parse dữ liệu và lưu vào csv
    parser = MostActiveQuoteParser()
    parser.parse_all_html(path=path)
    print("Parsing completed.")