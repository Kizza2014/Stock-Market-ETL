from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
from datetime import date, timedelta
import os
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


URL = "https://finance.yahoo.com/quote/"
SAVE_PATH = "./sample_html/crawl_5_years_history/"

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

    def crawl_all_daily_histories(self, ticker, wait_time=30):
        url = os.path.join(URL, ticker, "history")
        self.driver.get(url)
        print(f"Crawling all daily histories from {self.driver.title}")

        # chuyển đến phần xem tất cả lịch sử
        self.turn_on_all_history_view()

        # đợi render đầy đủ bảng lịch sử
        time.sleep(wait_time)

        # lấy toàn bộ HTML sau khi đã render
        html = self.driver.page_source
        save_path = os.path.join(SAVE_PATH, date.today().strftime("%Y_%m_%d"))
        if not os.path.exists(save_path):
            os.makedirs(save_path)
            print(f"Đã tạo thư mục {save_path} để lưu trữ HTML.")
        save_path = os.path.join(save_path, f"{ticker.upper()}_history_{date.today().strftime('%Y_%m_%d')}.html")
        self.save_html(html, save_path)

    def count_weekdays(start_date, end_date):
        count = 0
        current = start_date
        while current <= end_date:
            if current.weekday() < 5:  # 0-4 là thứ 2 đến thứ 6
                count += 1
            current += timedelta(days=1)
        return count

    def turn_on_all_history_view(self):
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

            # chọn tất cả lịch sử
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

    def save_html(self, html, save_path):
        with open(save_path, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"Đã lưu toàn bộ HTML vào {save_path}")

    def quit(self):
        self.driver.quit()

if __name__ == "__main__":
    crawler = HistoryCrawler()
    ticker = "NVDA"  # Example ticker
    crawler.crawl_all_daily_histories(ticker)
    crawler.quit()