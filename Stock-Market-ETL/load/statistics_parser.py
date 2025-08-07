from .parse_utils import save_to_csv, check_valid_folder
from bs4 import BeautifulSoup
from datetime import date
import os


class StatisticsParser:
    def parse_all_html(self, path):
        check_valid_folder(path)

        avalable_html = [file for file in os.listdir(path) if file.endswith('.html')]
        parse_results = {
            "parse_date": date.today().strftime("%Y-%m-%d"),
            "data_type": "key_statistics",
            "total_tickers": len(avalable_html),
            "tickers": {},
            "need_to_crawl_again": [],
            "total_succeeded": 0,
            "total_failed": 0
        }
        for html_file in avalable_html:
            ticker = html_file.split('_')[0]
            save_path = os.path.join(path, f"{ticker.upper()}_key_statistics_parsed.csv")

            print(f"\nParsing key statistics for {ticker} from {html_file}")
            try:
                with open(os.path.join(path, html_file), 'r', encoding='utf-8') as f:
                    html = f.read()
                soup = BeautifulSoup(html, 'html.parser')
                
                # Tìm container bảng
                table_container = soup.find('div', class_='table-container')
                if not table_container:
                    print(f"Không tìm thấy bảng trong file {html_file}.")
                    continue

                # Tìm bảng <table>
                table = table_container.find('table')
                if not table:
                    print(f"Không tìm thấy thẻ <table> trong file {html_file}.")
                    continue

                # Lấy header (bảng này cũng bị transposed)
                header_row = table.find('thead').find('tr')
                time_row = [th.get_text(strip=True).lower().replace(' ', '_') for th in header_row.find_all('th')]
                time_row[0] = "breakdown"

                # Lấy dữ liệu các dòng
                rows_data = []
                for tr in table.find('tbody').find_all('tr'):
                    values = [td.get_text(strip=True) for td in tr.find_all('td')]
                    rows_data.append(values)

                rows_data.insert(0, time_row)  # Thêm header vào đầu danh sách
                transposed_data = self.transpose_data(rows_data)
                header_row = transposed_data[0]  # Lấy header từ dữ liệu đã chuyển đổi
                transposed_data = transposed_data[1:]  # Bỏ header ra khỏi dữ liệu

                # Lưu ra CSV
                save_to_csv(transposed_data, header_row, save_path)

                print(f"Status: succeeded")
                parse_results["tickers"][ticker] = "succeeded"
                parse_results["total_succeeded"] += 1
            except Exception as e:
                print(f"Status: failed")
                parse_results["need_to_crawl_again"].append(ticker)
                parse_results["tickers"][ticker] = "failed"
                parse_results["total_failed"] += 1
                continue

        return parse_results

    def transpose_data(self, rows_data):
        # Chuyển đổi dữ liệu từ dạng hàng sang dạng cột
        transposed = list(map(list, zip(*rows_data)))
        return transposed