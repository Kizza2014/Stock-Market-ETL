from .base_transformer import BaseTransformer

class CompanyTransformer(BaseTransformer):
    def __init__(self):
        super().__init__()

    def transform(self, df):
        print("Starting transformation for company data...")

        # Loại bỏ trùng lặp và bản ghi thiếu company_ticker
        df_transformed = df.dropDuplicates(["company_ticker"]).na.drop(subset=["company_ticker"])

        # # Có thể thêm enrich hoặc các bước khác ở đây
        return df_transformed

if __name__ == "__main__":
    transformer = CompanyTransformer()
    # # Ví dụ sử dụng
    transformer.process(path="type=profile/date=2025_08_02/")
    print("Company data transformation completed.")