from selenium.webdriver.common.by import By
import time
from datetime import date
import os
from bs4 import BeautifulSoup
from crawl_utils import save_html, save_to_csv, check_valid_folder, create_folder_if_not_exists
from base_crawler import BaseCrawler


URL = "https://finance.yahoo.com/markets/stocks/most-active/"
SAVE_PATH = "./test/crawl_active_tickers/" # lưu trữ trên local, sau này sẽ thay bằng đường dẫn đến S3 bucket


class MostActiveQuoteCrawler(BaseCrawler):
    def __init__(self):
        super().__init__()

    def crawl_tickers_from_idx(self, url, start=0, count=50):
        # connect to the URL with pagination parameters
        url_with_params = os.path.join(url, f"?start={start}&count={count}")
        self.driver.get(url_with_params)
        print(f"\nCrawling {self.driver.title} from index {start} with count {count}")
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
            save_html(html, save_file)
            time.sleep(2) # Thêm thời gian chờ để tránh bị chặn

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
                print(f"\nParsing file: {file}")
                with open(os.path.join(path, file), 'r', encoding='utf-8') as f:
                    html = f.read()
                parsed_data, parsed_schema = self.parse_html(html)

                # Kiểm tra schema
                if schema is None:
                    schema = parsed_schema
                if parsed_schema != schema:
                    print(f"Cảnh báo: Schema không khớp trong file {file}")
                rows_data.extend(parsed_data)
        
        save_to_csv(rows_data, schema, save_path)
        print(f"Đã xử lí {len(rows_data)} dòng dữ liệu với schema: {schema}")


if __name__ == "__main__":
    print("\n\n================== MOST ACTIVE QUOTES CRAWLING ==================\n")

    # đường dẫn lưu rawl html và parsed csv
    crawl_date = date.today().strftime("%Y_%m_%d")
    print(f"Crawling date: {crawl_date}")
    path = os.path.join(SAVE_PATH, f"crawled_on_{crawl_date}")
    print(f"Path to save crawled data: {path}")
    create_folder_if_not_exists(path)

    # crawl dữ liệu
    crawler = MostActiveQuoteCrawler()
    crawler.crawl_all_tickers(URL, save_path=path)
    print("\nCrawling completed.")
    crawler.quit()
    
    # parse dữ liệu và lưu vào csv
    parser = MostActiveQuoteParser()
    parser.parse_all_html(path=path)
    print("\nParsing completed.")

    print("\n\n================== MOST ACTIVE QUOTES CRAWLING COMPLETED ==================\n")
