import re

class BaseParser:
    def __init__(self):
        pass

    def parse_html(self, html):
        raise NotImplementedError("Subclasses should implement this method.")

    def normalize_schema(self, schema):
        # Chuyển đổi schema về dạng chữ thường và loại bỏ khoảng trắng
        schema = [re.sub(r'[()\.,!@#%$^&*]', '', col) for col in schema]
        return [col.lower().replace(' ', '_') for col in schema]