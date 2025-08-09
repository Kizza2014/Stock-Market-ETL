from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import json


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
        options.add_argument("--no-sandbox")  # Chỉ giữ nếu chạy trên Linux/Docker
        # options.add_argument("--disable-dev-shm-usage")  # Chỉ giữ nếu chạy trên Docker
        options.add_argument("--disable-gpu")
        options.add_argument('--headless=new')

        # Thêm User-Agent để mô phỏng trình duyệt thực
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

        # Tắt các tùy chọn không cần thiết
        # Xóa: --ignore-certificate-errors, --ignore-ssl-errors, --disable-web-security, --allow-running-insecure-content
        return webdriver.Chrome(options=options)
    
    def is_error_html_content(self, html_content):
        error_indicators = [
            "Will be right back...",
            "Thank you for your patience.",
            "Not Found on Server",
            '<!-- status code : 404 -->',
            '<p id="message-1"> Thank you for your patience.</p>',
            '<p id="message-2"> Our engineers are working quickly to resolve the issue.</p>'
        ]
        return any(indicator in html_content for indicator in error_indicators)
    
    def mark_ticker_as_succeeded(self, ticker):
        self.crawling_results["tickers"][ticker] = "succeeded"
        self.crawling_results["total_succeeded"] += 1

    def mark_ticker_as_failed(self, ticker):
        self.crawling_results["tickers"][ticker] = "failed"
        self.crawling_results["total_failed"] += 1
        self.crawling_results["need_to_crawl_again"].append(ticker)

    def quit(self):
        self.driver.quit()

