import os
from bs4 import BeautifulSoup
from .parse_utils import save_to_csv, check_valid_folder
from datetime import date


class HistoryParser:
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
        
        # lưu dữ liệu vào file CSV
        save_to_csv(rows_data, schema, save_path)

        return len(rows_data) > 0 # nếu có dữ liệu, ngược lại tính là fail


    def parse_all_html(self, path, tickers):
        check_valid_folder(path)

        tickers = [ticker.upper() for ticker in tickers]  # đảm bảo ticker là chữ hoa
        available_html = {ticker: file for file in os.listdir(path) if file.endswith('.html') and file.split('_')[0] in tickers}
        # parse html cho từng mã và lưu vào file csv
        parse_results = {
            "parse_date": date.today().strftime("%Y-%m-%d"),
            "data_type": "history",
            "total_tickers": len(available_html),
            "tickers": {}
        }
        for ticker, html_file in available_html.items():
            # kết quả được lưu cùng đường dẫn với file HTML
            save_path = os.path.join(path, f"{ticker}_history_parsed.csv")

            print(f"\nParsing file: {html_file}")
            html_path = os.path.join(path, html_file)
            parse_status = self.parse_html(html_path, save_path)
            print(f"Status: {'succeeded' if parse_status else 'failed'}")
            parse_results["tickers"][ticker] = "succeeded" if parse_status else "failed"
        print(f"\nĐã xử lý xong {len(available_html)} file HTML.")

        parse_results["total_succeeded"] = sum(1 for status in parse_results["tickers"].values() if status == "succeeded")
        parse_results["total_failed"] = sum(1 for status in parse_results["tickers"].values() if status == "failed")
        parse_results["need_to_crawl_again"] = [ticker for ticker, status in parse_results["tickers"].items() if status == "failed"]
        return parse_results