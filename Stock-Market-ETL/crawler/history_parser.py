from bs4 import BeautifulSoup
import csv
import os
from datetime import date

SAVE_PATH = "./sample_html/crawl_5_years_history/"

class HistoryParser:
    def __init__(self):
        pass

    def normalize_schema(self, schema):
        # Chuyển đổi schema về dạng chữ thường và loại bỏ khoảng trắng
        return [col.lower().replace(' ', '_') for col in schema]

    def parse_html(self, html_path, save_path):
        with open(html_path, 'r', encoding='utf-8') as f:
            html = f.read()
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
            print(f"Tổng số dòng: {len(rows_data) - 1}") # trừ đi header row
        
        if save_path:
            self.save_to_csv(rows_data, schema, save_path)

    def parse_all_html(self, path):
        # ngày hiện tại trùng với ngày crawl dữ liệu
        parse_date = date.today().strftime("%Y_%m_%d")
        path = os.path.join(SAVE_PATH, parse_date)

        # kiểm tra xem dữ liệu ngày đó đã được crawl chưa
        if not os.path.exists(path):
            print(f"Thư mục {path} không tồn tại.")
            exit(1)
        if not os.listdir(path):
            print(f"Thư mục {path} không chứa file HTML nào.")
            exit(1)

        # parse html cho từng mã và lưu vào file csv
        available_html = [file for file in os.listdir(path) if file.endswith('.html')]
        for html_file in available_html:
            # lấy ticker từ tên file
            ticker = html_file.split('_')[0]

            # kết quả được lưu cùng đường dẫn với file HTML
            save_path = os.path.join(path, f"{ticker.upper()}_history_{parse_date}_parsed.csv")
            
            print(f"Parsing file: {html_file}")
            html_path = os.path.join(path, html_file)
            self.parse_html(html_path, save_path)
        print(f"Đã xử lý xong {len(available_html)} file HTML.")
    
    def save_to_csv(self, rows_data, schema, save_path):
        if not rows_data or not schema:
            print("Không có dữ liệu để ghi ra file CSV.")
            return
        
        with open(save_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(schema)
            for row in rows_data:
                writer.writerow(row)
        print(f"Đã ghi dữ liệu ra file {save_path}")
    
if __name__ == "__main__":
    parser = HistoryParser()
    parser.parse_all_html(SAVE_PATH)
    print("Parsing completed.")