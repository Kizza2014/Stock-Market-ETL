from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time

options = Options()
options.add_argument('--start-maximized')
options.add_argument('--disable-blink-features=AutomationControlled')
options.add_argument('--disable-extensions')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--headless=new')

driver = webdriver.Chrome(options=options)
driver.get("https://finance.yahoo.com/quote/NVDA/analysis/") 

print(f"Crawling {driver.title}")
# button1 = driver.find_element(By.CSS_SELECTOR, "button.tertiary-btn.menuBtn.yf-1epmntv")
# button1.click()
# print("Button 1 clicked")

# time.sleep(2)

# button2 = driver.find_element(By.CSS_SELECTOR, "button.tertiary-btn.tw-w-full.tw-justify-center.yf-1epmntv")
# button2.click()
# print("Button 2 clicked")
# time.sleep(60)

# Lấy toàn bộ HTML sau khi đã render
html = driver.page_source

# Ghi vào file
filename = "./sample_html/stocks_quote_analysis.html"
with open(filename, "w", encoding="utf-8") as f:
    f.write(html)

print(f"Đã lưu toàn bộ HTML sau khi JavaScript render xong vào {filename}")

driver.quit()
