from bs4 import BeautifulSoup
import os
from .base_parser import BaseParser
from minio_utils import MinioClient
import json


LANDING_BUCKET = os.getenv("LANDING_BUCKET", "landing")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")
ROOT_SAVE_PATH = os.getenv("PROFILES_ROOT_PATH", "type=profile")


class ProfileParser(BaseParser):
    def __init__(self):
        super().__init__()
        self.parsing_results["data_type"] = "profile"

    def parse_all_html(self, parse_date):
        # tạo minio client 1 lần duy nhất
        minio_client = MinioClient()

        # kiểm tra xem dữ liệu ngày đó đã được crawl chưa
        files_path = os.path.join(ROOT_SAVE_PATH, f"date={parse_date}")
        files_list = minio_client.list_files_in_folder(LANDING_BUCKET, files_path)
        html_files = [file for file in files_list if file.endswith('.html')]

        if html_files:
            # chỉ định các thông tin cần lấy từ html
            schema = [
                "company_name", "company_ticker", "exchange_name", "sector", "industry", "phone", "website",
                "full_address", "country", "description"
            ]

            # khởi tạo danh sách để lưu dữ liệu
            rows_data = []
            for html_file in html_files:
                filename = html_file.split("/")[-1]
                ticker = filename.split("_")[0].upper()  # lấy ticker từ tên file

                print(f"\nParsing {html_file}...")
                html_content = minio_client.read_html_content_from_minio(LANDING_BUCKET, html_file)
                soup = BeautifulSoup(html_content, 'html.parser')

                try:
                    # lấy tên công ty và ticker
                    company_name_tag = soup.find('h1', class_='yf-4vbjci').text.strip()  # ex: "NVIDIA Corporation (NVDA)"
                    company_name = ' '.join(company_name_tag.split(' ')[:-1]) # lấy phần tên công ty
                    company_ticker = ticker

                    # lấy tên sàn giao dịch
                    exchange_tag = soup.find('span', class_='exchange yf-15jhhmp')  # ex: "NASDAQ - NASDAQ Real Time Price"
                    exchange_name = exchange_tag.find('span').text.strip().split("-")[0].strip() if exchange_tag else ""

                    # lấy sector và industry
                    sector = ""
                    industry = ""
                    company_stats_dl = soup.find('dl', class_='company-stats yf-wxp4ja')
                    if company_stats_dl:
                        for div in company_stats_dl.find_all('div', recursive=False):
                            dt = div.find('dt')
                            if dt:
                                dt_text = dt.text.strip()
                                if dt_text.startswith("Sector"):
                                    a_tag = div.find('a')
                                    sector = a_tag.text.strip() if a_tag else ""
                                elif dt_text.startswith("Industry"):
                                    a_tag = div.find('a')
                                    industry = a_tag.text.strip() if a_tag else ""

                    # lấy thông tin công ty (địa chỉ, phone, website) an toàn hơn
                    company_info_div = soup.find('div', class_='company-info yf-wxp4ja')
                    phone_number = ""
                    website = ""
                    full_address = ""
                    country = ""

                    if company_info_div:
                        # Địa chỉ
                        address_tag = company_info_div.find('div', class_='address yf-wxp4ja')
                        if address_tag:
                            address_parts = [div.text.strip() for div in address_tag.find_all('div')]
                            full_address = ', '.join(address_parts)
                            country = address_parts[-1] if address_parts else ""

                        # Phone & Website
                        for a_tag in company_info_div.find_all('a', href=True):
                            aria_label = a_tag.get('aria-label', '').lower()
                            href = a_tag['href']
                            if 'phone' in aria_label or href.startswith('tel:'):
                                phone_number = a_tag.text.strip()
                            elif 'website' in aria_label or a_tag.get('rel') == ['noopener', 'noreferrer']:
                                website = a_tag.text.strip()

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
                except Exception as e:
                    print(f"Status: failed - {e}")
                    continue

            # lưu dữ liệu lên minio dưới dạng parquet
            if rows_data:
                save_path = os.path.join(files_path, "all_profiles_parsed.parquet")
                minio_client.upload_to_minio_as_parquet(rows_data, schema, save_path, BRONZE_BUCKET)
            else:
                print("No valid profile data found to save.")
            
            # lưu lại kết quả parse lên minio
            self.parsing_results["parse_date"] = parse_date
            self.parsing_results["total_tickers"] = len(html_files)
            self.parsing_results["total_succeeded"] = len(rows_data)
            self.parsing_results["total_failed"] = len(html_files) - len(rows_data)
            results_json = json.dumps(self.parsing_results, indent=4)
            results_path = os.path.join(files_path, "parsing_results.json")
            minio_client.upload_json_content_to_minio(BRONZE_BUCKET, results_json, results_path)