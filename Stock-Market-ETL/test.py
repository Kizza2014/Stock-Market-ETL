import pandas as pd
import os

path = './extract/data_test/crawl_financials/crawled_on_2025_08_02/cash_flow'
files = os.listdir(path)
for filename in files:
    # Đọc file CSV
    if filename.endswith('.csv'):
        df = pd.read_csv(os.path.join(path, filename))

        # Lưu dưới dạng Parquet
        new_filename = filename.split(".")[0] + ".parquet"
        df.to_parquet(os.path.join(path, new_filename), index=False)