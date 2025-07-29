from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
import os

URL = "https://finance.yahoo.com/markets/stocks/most-active/"
SAVE_PATH = "./sample_html/test_downloader/"

def download_page(self):
    if not self.is_connected():
        self.driver.get(self.url)
        print(f"Crawling {self.driver.title}")
    html = self.driver.page_source
    return html

def download_all_pages(self):
    self.driver.get(self.url)
    print(f"Downloading all pages from {self.driver.title}")
    # tải trang đầu tiên
    page_number = 1
    html = self.download_page()
    self.save_html(html, os.path.join(SAVE_PATH, f"stocks_most_active_page{page_number}.html"))
    page_number += 1

    # kiểm tra và tải các trang tiếp theo
    while self.has_next_button() is not None:
        next_button = self.get_next_button()
        next_button.click()
        print(f"Chuyển sang trang {page_number}")
        html = self.download_page()
        self.save_html(html, os.path.join(SAVE_PATH, f"stocks_most_active_page{page_number}.html"))
        time.sleep(3)
        page_number += 1

def has_next_button(self):
    try:
        next_button = self.driver.find_element(By.CSS_SELECTOR, 'button[data-testid="next-page-button"]')
        return next_button.is_displayed() and next_button.is_enabled()
    except Exception as e:
        print(f"Không tìm thấy nút Next: {e} -> Dừng tải.")
        return None
    
def get_next_button(self):
    try:
        next_button = self.driver.find_element(By.CSS_SELECTOR, 'button[data-testid="next-page-button"]')
        return next_button
    except Exception as e:
        print(f"Không tìm thấy nút Next: {e} -> Dừng tải.")
        return None

def save_html(self, html, save_path):
    with open(save_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Đã lưu toàn bộ HTML vào {save_path}")


if __name__ == "__main__":
    options = Options()
    options.add_argument('--start-maximized')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-extensions')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    # options.add_argument("--disable-gpu")
    # options.add_argument("--ignore-certificate-errors")
    # options.add_argument("--ignore-ssl-errors")
    # options.add_argument("--disable-web-security")
    # options.add_argument("--allow-running-insecure-content")

    driver = webdriver.Chrome(options=options)
    driver.get(URL)

    button = driver.find_element(By.CSS_SELECTOR, 'button[data-testid="next-page-button"]')
    if button.is_displayed() and button.is_enabled():
        print("Nút Next có sẵn và có thể nhấn.")
        button.click()
    else:
        print("Nút Next không có sẵn hoặc không thể nhấn.")