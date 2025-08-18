from .base_transformer import BaseTransformer
from pyspark.sql.functions import col, to_date, trim, regexp_replace
from pyspark.sql.types import StringType, DoubleType, DateType


class HistoryTransformer(BaseTransformer):
    def __init__(self):
        super().__init__()

    def transform(self, df):
        # drop na do đó là dữ liệu của những ngày không giao dịch
        df_transformed = df.dropna(how="any")

        # chuẩn hoá date
        df_transformed = df_transformed.withColumn(
            "date",
            to_date(trim(col("date")), "MMMM d, yyyy")
        )

        # Đổi tên các cột theo schema chuẩn
        rename_map = {
            "ticker": "symbol",
            "date": "date",
            "open": "open",
            "high": "high",
            "low": "low",
            "closeclose_price_adjusted_for_splits": "close",
            "adj_closeadjusted_close_price_adjusted_for_splits_and_dividend_and/or_capital_gain_distributions": "adjusted_close",
            "volume": "volume"
        }
        for old, new in rename_map.items():
            df_transformed = df_transformed.withColumnRenamed(old, new)

        # Loại bỏ dấu phẩy ở các trường số trước khi cast (eg: "1,000,000")
        for num_col in ["volume"]:
            if num_col in df_transformed.columns:
                df_transformed = df_transformed.withColumn(
                    num_col, regexp_replace(col(num_col), ",", "")
                )

        # chuẩn hoá kiểu dữ liệu
        df_transformed = df_transformed.select(
            col("symbol").cast(StringType()),
            col("date").cast(DateType()),
            col("open").cast(DoubleType()),
            col("high").cast(DoubleType()),
            col("low").cast(DoubleType()),
            col("close").cast(DoubleType()),
            col("adjusted_close").cast(DoubleType()),
            col("volume").cast(DoubleType())
        )

        # Additional transformations can be added here
        return df_transformed


if __name__ == "__main__":
    transformer = HistoryTransformer()
    # Specify the path to the history data in the bronze bucket
    transformer.process("type=history/date=2025_08_02/")
    # The transformed data will be written to the silver bucket
    # Uncomment the line below if you want to write the transformed data
    # transformer.write(df_transformed, SILVER_BUCKET, "type=history/date=2025_08_02/")