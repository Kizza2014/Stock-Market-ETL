from bs4 import BeautifulSoup
import csv
import os
from datetime import date

SAVE_PATH = "./sample_html/crawl_active_tickers/"

class MostActiveQuoteParser:
    def __init__(self):
        pass

    def normalize_schema(self, schema):
        # Chuyển đổi schema về dạng chữ thường và loại bỏ khoảng trắng
        return [col.lower().replace(' ', '_') for col in schema]

    def parse_html(self, html):
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
            print(f"Tổng số dòng: {len(rows_data)}")
        return rows_data, schema

    def parse_all_html(self, path):
        # kiểm tra xem dữ liệu ngày đó đã được crawl chưa
        if not os.path.exists(path):
            print(f"Thư mục {path} không tồn tại.")
            exit(1)
        if not os.listdir(path):
            print(f"Thư mục {path} không chứa file HTML nào.")
            exit(1)

        # kết quả được lưu cùng đường dẫn với html
        save_path = os.path.join(path, "most_active_quotes_parsed.csv")

        files = os.listdir(path)
        rows_data =[]
        schema = None
        for file in files:
            if file.endswith('.html'):
                print(f"Parsing file: {file}")
                with open(os.path.join(path, file), 'r', encoding='utf-8') as f:
                    html = f.read()
                parsed_data, parsed_schema = self.parse_html(html)

                # Kiểm tra schema
                if schema is None:
                    schema = parsed_schema
                if parsed_schema != schema:
                    print(f"Cảnh báo: Schema không khớp trong file {file}")
                rows_data.extend(parsed_data)
        
        if save_path:
            self.save_to_csv(rows_data, schema, save_path)
        print(f"Đã xử lí {len(rows_data)} dòng dữ liệu với schema: {schema}")
    
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
    # ngày hiện tại trùng với ngày crawl dữ liệu
    parse_date = date.today().strftime("%Y_%m_%d")
    path = os.path.join(SAVE_PATH, parse_date)

    parser = MostActiveQuoteParser()
    parser.parse_all_html(path)
    print("Parsing completed.")
