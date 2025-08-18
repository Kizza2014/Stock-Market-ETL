from .base_transformer import BaseTransformer
from pyspark.sql.functions import col, to_date, trim, regexp_replace
from pyspark.sql.types import DoubleType


class BalanceSheetTransformer(BaseTransformer):
    def __init__(self):
        super().__init__()

    def transform(self, df):
        df_transformed = df

        # Đổi tên các cột theo schema chuẩn
        rename_map = {
            "ticker": "symbol",
            "breakdown": "date",
        }

        # Áp dụng đổi tên cột
        for old_name, new_name in rename_map.items():
            df_transformed = df_transformed.withColumnRenamed(old_name, new_name)

        # Loại bỏ các dòng có breakdown = 'current' hoặc 'ttm'
        if "date" in df_transformed.columns:
            df_transformed = df_transformed.filter(~col("breakdown").isin(["current", "ttm"]))

        # chuẩn hoá date
        df_transformed = df_transformed.withColumn(
            "date",
            to_date(trim(col("date")), "M/d/yyyy")
        )

        # Thay các giá trị '--' bằng None (NaN)
        df_transformed = df_transformed.replace('--', None)

        # Loại bỏ dấu phẩy ở các trường số trước khi cast (eg: "1,000,000")
        for num_col in [c for c in df_transformed.columns if c not in ["symbol", "date"]]:
            df_transformed = df_transformed.withColumn(
                num_col, regexp_replace(col(num_col), ",", "")
            )

        # Chuẩn hoá kiểu dữ liệu: cast tất cả các trường về double ngoại trừ symbol và date
        for col_name in [c for c in df_transformed.columns if c not in ["symbol", "date"]]:
            df_transformed = df_transformed.withColumn(col_name, col(col_name).cast(DoubleType()))

        # Additional transformations can be added here
        return df_transformed
    

if __name__ == "__main__":
    transformer = BalanceSheetTransformer()
    # Specify the path to the history data in the bronze bucket
    transformer.process("type=balance_sheet/date=2025_08_02/")
    # The transformed data will be written to the silver bucket
    # Uncomment the line below if you want to write the transformed data
    # transformer.write(df_transformed, SILVER_BUCKET, "type=history/date=2025_08_02/")