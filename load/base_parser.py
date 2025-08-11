import re

class BaseParser:
    def __init__(self):
        self.parsing_results = {
            "data_type": None,
            "total_tickers": 0,
            "parse_date": None
        }

    def parse_html(self, html):
        raise NotImplementedError("Subclasses should implement this method.")

    def normalize_schema(self, schema):
        # Chuyển đổi schema về dạng chữ thường và loại bỏ khoảng trắng
        schema = [re.sub(r'[()\.,!@#%$^&*]', '', col) for col in schema]
        return [col.lower().replace(' ', '_') for col in schema]

    def transpose_data(self, rows_data):
        # Chuyển đổi dữ liệu từ dạng hàng sang dạng cột
        transposed = list(map(list, zip(*rows_data)))
        return transposed