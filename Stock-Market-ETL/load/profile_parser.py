from .parse_utils import save_to_csv, check_valid_folder
from bs4 import BeautifulSoup
from datetime import date
import os


class ProfileParser:
    def parse_all_html(self, path):
        # kiểm tra xem dữ liệu ngày đó đã được crawl chưa
        check_valid_folder(path)

        # chỉ định các thông tin cần lấy từ html
        schema = [
            "company_name", "company_ticker", "exchange_name", "sector", "industry", "phone", "website",
            "full_address", "country", "description"
        ]

        # lấy danh sách các file HTML trong thư mục
        available_html = [file for file in os.listdir(path) if file.endswith('.html')]

        # khởi tạo danh sách để lưu dữ liệu
        rows_data = []
        parse_results = {
            "parse_date": date.today().strftime("%Y-%m-%d"),
            "total_tickers": len(available_html),
            "tickers": {},
            "need_to_crawl_again": [],
            "total_succeeded": 0,
            "total_failed": 0
        }

        for html_file in available_html:
            print(f"\nParsing {html_file}...")
            with open(os.path.join(path, html_file), "r", encoding="utf-8") as file:
                html = file.read()
                soup = BeautifulSoup(html, 'html.parser')
                ticker = html_file.split("_")[0].upper()  # lấy ticker từ tên file

                try:
                    # lấy tên công ty và ticker
                    company_name_tag = soup.find('h1', class_='yf-4vbjci').text.strip()  # ex: "NVIDIA Corporation (NVDA)"
                    company_name = ' '.join(company_name_tag.split(' ')[:-1]) # lấy phần tên công ty
                    company_ticker = ticker

                    # lấy tên sàn giao dịch
                    exchange_tag = soup.find('span', class_='exchange yf-15jhhmp')  # ex: "NASDAQ - NASDAQ Real Time Price"
                    exchange_name = exchange_tag.find('span').text.strip().split("-")[0].strip()

                    # lấy sector và industry
                    sector_industry_tags = soup.find_all('a', class_='subtle-link fin-size-large yf-u4gyzs')
                    sector = sector_industry_tags[0].text.strip() if len(sector_industry_tags) > 0 else ""
                    industry = sector_industry_tags[1].text.strip() if len(sector_industry_tags) > 1 else ""

                    # lấy số điện thoại
                    phone_tag = soup.find('a', class_='primary-link fin-size-small noUnderline yf-u4gyzs', href=True)
                    phone_number = phone_tag.text.strip() if phone_tag and phone_tag['href'].startswith('tel:') else ""

                    # lấy website
                    website_tag = soup.find(
                        'a',
                        class_='primary-link fin-size-small noUnderline yf-u4gyzs',
                        href=True,
                        attrs={'aria-label': 'website link'}
                    )
                    website = website_tag.text.strip() if website_tag else ""

                    # lấy địa chỉ
                    address_tag = soup.find('div', class_='address yf-wxp4ja')
                    if address_tag:
                        address_parts = [div.text.strip() for div in address_tag.find_all('div')]
                        ## country
                        country = address_parts[-1] if len(address_parts) > 2 else ""
                        ## thêm 1 trường full address nếu cần
                        full_address = ', '.join(address_parts)

                    # lấy description
                    description_tag = soup.find('section', {'data-testid': 'description'})
                    if description_tag:
                        description_p = description_tag.find('p')
                        description = description_p.text.strip() if description_p else ""
                    else:
                        description = ""

                    # thêm thông tin công ty vào danh sách
                    rows_data.append([
                        company_name, company_ticker, exchange_name, sector, industry, phone_number, website,
                        full_address, country, description
                    ])

                    # in thông báo
                    print(f"Status: succeeded")
                    parse_results["tickers"][ticker] = "succeeded"
                    parse_results["total_succeeded"] += 1
                except Exception as e:
                    print(f"Status: failed")
                    parse_results["tickers"][ticker] = "failed"
                    parse_results["total_failed"] += 1
                    parse_results["need_to_crawl_again"].append(ticker)  # thêm ticker vào danh sách cần crawl lại
                    continue

        # lưu dữ liệu vào file CSV
        save_path = os.path.join(path, "all_profiles_parsed.csv")
        save_to_csv(rows_data, schema, save_path)

        return parse_results