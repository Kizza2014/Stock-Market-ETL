from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import random


import random

USER_AGENTS = [
    # Chrome
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    # Firefox
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:118.0) Gecko/20100101 Firefox/118.0",
    # Edge
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    # Safari
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
]

class BaseCrawler:
    def __init__(self):
        self.driver = self.setup_driver()
        self.crawling_results = {
            "data_type": None,
            "total_tickers": 0,
            "tickers": {},
            "total_succeeded": 0,
            "total_failed": 0,
            "need_to_crawl_again": []
        }

    def setup_driver(self):
        options = Options()
        # options.add_argument("--start-maximized")  # Chỉ giữ nếu không dùng headless
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-extensions")
        # options.add_argument("--no-sandbox")  # Chỉ giữ nếu chạy trên Linux/Docker
        # options.add_argument("--disable-dev-shm-usage")  # Chỉ giữ nếu chạy trên Docker
        options.add_argument("--disable-gpu")
        options.add_argument('--headless=new')

        # Chọn user-agent ngẫu nhiên
        user_agent = random.choice(USER_AGENTS)
        options.add_argument(f"user-agent={user_agent}")

        # Tắt các tùy chọn không cần thiết
        # Xóa: --ignore-certificate-errors, --ignore-ssl-errors, --disable-web-security, --allow-running-insecure-content
        return webdriver.Chrome(options=options)
    
    def is_error_html_content(self, html_content):
        # Chuyển nội dung HTML về chữ thường để kiểm tra không phân biệt case
        html_lower = html_content.lower()
        
        error_indicators = [
            "will be right back...",
            "thank you for your patience.",
            "not found on server",
            '<!-- status code : 404 -->',
            '<p id="message-1"> thank you for your patience.</p>',
            '<p id="message-2"> our engineers are working quickly to resolve the issue.</p>',
            # Các indicator mới cho lỗi "Oops, something went wrong"
            "oops, something went wrong",
            "that page can't be found",
            "try exploring the menu or using search to find what you are looking for.",
            "skip to navigation"  # Một phần phổ biến trong trang lỗi của Yahoo
        ]
        
        # Kiểm tra nếu bất kỳ indicator nào tồn tại trong nội dung (không phân biệt case)
        return any(indicator.lower() in html_lower for indicator in error_indicators)
    
    def mark_ticker_as_succeeded(self, ticker):
        self.crawling_results["tickers"][ticker] = "succeeded"
        self.crawling_results["total_succeeded"] += 1

    def mark_ticker_as_failed(self, ticker):
        self.crawling_results["tickers"][ticker] = "failed"
        self.crawling_results["total_failed"] += 1
        self.crawling_results["need_to_crawl_again"].append(ticker)

    def show_quarterly_data(self, wait_time=20):
        try:
            # Đợi nút quarterly xuất hiện rồi mới click
            quarterly_btn = WebDriverWait(self.driver, wait_time).until(
                EC.element_to_be_clickable((By.ID, "tab-quarterly"))
            )
            quarterly_btn.click()
            print("Đã bấm nút Quarterly.")
            time.sleep(2)  # đợi dữ liệu quarterly load lại
        except Exception as e:
            print(f"Error clicking Quarterly button: {str(e)}")

    def wait_random_delay(self, min_seconds=2, max_seconds=5):
        delay = random.uniform(min_seconds, max_seconds)
        print(f"[Random Delay] Sleeping for {delay:.2f} seconds...")
        time.sleep(delay)

    def quit(self):
        self.driver.quit()

