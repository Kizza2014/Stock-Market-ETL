from base_transformer import BaseTransformer
import os

class CompanyTransformer(BaseTransformer):
    def __init__(self):
        super().__init__()

    def transform(self, df):
        print("Starting transformation for company data...")

        # Loại bỏ trùng lặp và bản ghi thiếu company_ticker
        df_transformed = df.dropDuplicates(["company_ticker"]).na.drop(subset=["company_ticker"])

        # # Chuẩn hóa tên công ty (nếu có)
        # if "name" in df.columns:
        #     from pyspark.sql.functions import trim, upper
        #     df_transformed = df_transformed.withColumn("name", upper(trim(df_transformed["name"])))

        # # Có thể thêm enrich hoặc các bước khác ở đây
        return df_transformed

if __name__ == "__main__":
    transformer = CompanyTransformer()
    # # Ví dụ sử dụng
    transformer.process(path="type=profile/date=2025_08_02/")
    print("Company data transformation completed.")