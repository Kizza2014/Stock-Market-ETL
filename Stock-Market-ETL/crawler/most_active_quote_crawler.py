from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
from datetime import date
import os

URL = "https://finance.yahoo.com/markets/stocks/most-active/"
SAVE_PATH = "./sample_html/crawl_active_tickers/"

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

    def crawl_all_tickers(self, url, count=50):
        # connect to the URL
        self.driver.get(url)
        print(f"Crawling all tickers from {self.driver.title}")

        # create a directory to save the crawled HTML files
        crawl_date = date.today().strftime("%Y_%m_%d")
        crawl_path = os.path.join(SAVE_PATH, crawl_date)
        if not os.path.exists(crawl_path):
            os.makedirs(crawl_path)
            print(f"Đã tạo thư mục lưu trữ: {crawl_path}")

        # get the total number of most active tickers
        total = self.get_total()
        for i in range(0, total, count):
            html = self.crawl_tickers_from_idx(url, start=i, count=count)
            save_file = os.path.join(crawl_path, f"stocks_most_active_page{i // count + 1}.html")
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

if __name__ == "__main__":
    crawler = MostActiveQuoteCrawler()
    crawler.crawl_all_tickers(URL)
    crawler.quit()