from bs4 import BeautifulSoup

# Đọc file HTML
with open('./sample_html/test_downloader/stocks_most_active_page5.html', 'r', encoding='utf-8') as f:
    html = f.read()

soup = BeautifulSoup(html, 'html.parser')

# Tìm bảng đầu tiên (duy nhất)
table = soup.find('table')

# Lấy tất cả các dòng (tr) trong bảng
rows = table.find_all('tr')

i = 0
for row in rows:
    # Lấy tất cả các ô (td hoặc th) trong dòng
    cells = row.find_all(['td', 'th'])
    cell_values = [cell.get_text(strip=True) for cell in cells]
    print(cell_values)
    i += 1
print(i)