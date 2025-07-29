from bs4 import BeautifulSoup
import csv

# Đọc file HTML
with open('./sample_html/test_downloader/stocks_most_active_page1.html', 'r', encoding='utf-8') as f:
    html = f.read()

# Parse HTML
soup = BeautifulSoup(html, 'html.parser')

# Lấy ra bảng duy nhất
table = soup.find('table')

rows_data = []
if table:
    # Sau khi có bảng, bạn có thể lấy các thành phần như td
    i = 1
    rows = table.find_all('tr')
    for row in rows:
        cells = row.find_all(['td'])
        cell_values = [cell.get_text(strip=True) for cell in cells]
        print(f"Row {i}: {cell_values}")
        if cell_values:
            rows_data.append(cell_values)
        i += 1
    print(f"Tổng số dòng: {i-1}")
    # Ghi ra file CSV
    with open('output.csv', 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        for row in rows_data:
            writer.writerow(row)
    print('Đã ghi dữ liệu ra file output.csv')
else:
    print('Không tìm thấy bảng trong file HTML')