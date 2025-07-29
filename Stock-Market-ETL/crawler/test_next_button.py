from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
import os

URL = "https://finance.yahoo.com/markets/stocks/most-active/"
SAVE_PATH = "./sample_html/test_downloader/"

options = Options()
options.add_argument('--start-maximized')
options.add_argument('--disable-blink-features=AutomationControlled')
options.add_argument('--disable-extensions')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')

driver = webdriver.Chrome(options=options)
driver.get(URL)

page_number = 1
while True:
    # Lưu HTML của trang hiện tại
    html = driver.page_source
    save_file = os.path.join(SAVE_PATH, f"stocks_most_active_page{page_number}.html")
    with open(save_file, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Đã lưu trang {page_number} vào {save_file}")

    # Tìm và click nút Next nếu có
    try:
        button = driver.find_element(By.CSS_SELECTOR, 'button[data-testid="next-page-button"]')
        if button and button.is_displayed() and button.is_enabled():
            button.click()
            print(f"Đã nhấn Next sang trang {page_number + 1}")
            time.sleep(3)  # Chờ trang mới tải xong
            page_number += 1
        else:
            print("Nút Next không còn khả dụng.")
            break
    except Exception as e:
        print(f"Không tìm thấy nút Next -> Dừng tải.")
        break

driver.quit()