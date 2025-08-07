from .parse_utils import save_to_csv, check_valid_folder
from bs4 import BeautifulSoup
import os
from datetime import date


class FinancialParser:
    def parse_all_html(self, path):
        check_valid_folder(path)

        data_types = ["income_statement", "balance_sheet", "cash_flow"]
        parse_results = {}
        for data_type in data_types:
            # parse dữ liệu cho từng loại tương ứng
            data_type_path = os.path.join(path, data_type)
            data_type_results = self.parse_transposed_table(data_type_path, data_type=data_type)
            parse_results[data_type] = data_type_results

        return parse_results

    def parse_transposed_table(self, html_path, data_type):
        available_html = [file for file in os.listdir(html_path) if file.endswith('.html')]
        parse_results = {
            "parse_date": date.today().strftime("%Y-%m-%d"),
            "data_type": data_type,
            "total_tickers": len(available_html),
            "tickers": {},
            "need_to_crawl_again": []
        }
        for html_file in available_html:
            ticker = html_file.split('_')[0].upper() # lấy mã chứng khoán từ tên file
            save_path = os.path.join(html_path, f"{ticker}_{data_type}_parsed.csv")
            
            print(f"\nParsing {data_type} for {ticker} from {html_file}")
            with open(os.path.join(html_path, html_file), 'r', encoding='utf-8') as f:
                html = f.read()
            soup = BeautifulSoup(html, 'html.parser')
            
            try:
                # Tìm container bảng
                table_container = soup.find('div', class_='tableContainer')
                if not table_container:
                    print(f"Không tìm thấy bảng trong file {html_file}.")
                    continue

                # Lấy header
                header_row = table_container.find('div', class_='tableHeader').find('div', class_='row')
                time_row = [col.get_text(strip=True).lower().replace(' ', '_') for col in header_row.find_all('div', class_='column')]

                # Lấy dữ liệu các dòng
                rows_data = []
                for row in table_container.find('div', class_='tableBody').find_all('div', class_='row'):
                    breakdown_div = row.find('div', class_='column')
                    row_title = breakdown_div.find('div', class_='rowTitle')
                    if row_title:
                        # Lấy thuộc tính title nếu có, nếu không thì lấy text
                        breakdown = row_title.get('title') or row_title.get_text(strip=True)
                    else:
                        # Nếu không có rowTitle, lấy toàn bộ text (loại bỏ thẻ con như button)
                        breakdown = ''.join([t for t in breakdown_div.strings]).strip()
        
                    # Lấy các giá trị
                    values = [col.get_text(strip=True) for col in row.find_all('div', class_='column') if 'sticky' not in col.get('class', [])]
                    values.insert(0, breakdown)
                    rows_data.append(values)

                rows_data.insert(0, time_row)  # Thêm time_row vào đầu danh sách

                # chuyển đổi dữ liệu từ dạng hàng sang dạng cột
                transposed_data = self.transpose_data(rows_data)
                schema = transposed_data[0]  # lấy schema từ hàng đầu tiên
                transposed_data = transposed_data[1:]  # loại bỏ schema khỏi dữ liệu

                print(f"Tổng số dòng: {len(transposed_data)}")
                save_to_csv(transposed_data, schema, save_path)

                # ghi log
                print("Status: succeeded")
                parse_results["tickers"][ticker] = "succeeded"
                parse_results["total_succeeded"] = parse_results.get("total_succeeded", 0) + 1
            except Exception as e:
                print("Status: failed")
                parse_results["tickers"][ticker] = "failed"
                parse_results["total_failed"] = parse_results.get("total_failed", 0) + 1
                parse_results["need_to_crawl_again"].append(ticker)
            
        return parse_results