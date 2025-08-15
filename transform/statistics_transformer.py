from base_transformer import BaseTransformer
from pyspark.sql.functions import col, to_date, trim, regexp_replace, regexp_extract, when
from pyspark.sql.types import StringType, DoubleType, DateType


class StatisticsTransformer(BaseTransformer):
    def __init__(self):
        super().__init__()

    def transform(self, df):
        df_transformed = df

        # Đổi tên các cột theo schema chuẩn
        rename_map = {
            "ticker": "symbol",
            "breakdown": "date",
            "Market Cap": "market_cap",
            "Enterprise Value": "enterprise_value",
            "Trailing P/E": "trailing_pe",
            "Forward P/E": "forward_pe",
            "PEG Ratio (5yr expected)": "peg_ratio_5yr_expected",
            "Price/Sales": "ps_ratio",
            "Price/Book": "pb_ratio",
            "Enterprise Value/Revenue": "ev_revenue_ratio",
            "Enterprise Value/EBITDA": "ev_ebitda_ratio"
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

        # chuẩn hoá đơn vị cho market_cap và enterprise_value
        for col_name in ["market_cap", "enterprise_value"]:
            df_transformed = df_transformed.withColumn(
                f"{col_name}_number", regexp_extract(col(col_name), r"([\d\.]+)", 1).cast(DoubleType())
            ).withColumn(
                f"{col_name}_unit", regexp_extract(col(col_name), r"([BMK])$", 1)
            ).withColumn(
                col_name,
                when(col(f"{col_name}_unit") == "B", col(f"{col_name}_number") * 1_000_000_000)
                .when(col(f"{col_name}_unit") == "M", col(f"{col_name}_number") * 1_000_000)
                .when(col(f"{col_name}_unit") == "K", col(f"{col_name}_number") * 1_000)
                .otherwise(col(f"{col_name}_number"))
            ).drop(f"{col_name}_number", f"{col_name}_unit")

        # chuẩn hoá kiểu dữ liệu
        df_transformed = df_transformed.select(
            col("symbol").cast(StringType()),
            col("date").cast(DateType()),
            col("market_cap").cast(DoubleType()),
            col("enterprise_value").cast(DoubleType()),
            col("trailing_pe").cast(DoubleType()),
            col("forward_pe").cast(DoubleType()),
            col("peg_ratio_5yr_expected").cast(DoubleType()),
            col("ps_ratio").cast(DoubleType()),
            col("pb_ratio").cast(DoubleType()),
            col("ev_revenue_ratio").cast(DoubleType()),
            col("ev_ebitda_ratio").cast(DoubleType())
        )

        # Additional transformations can be added here
        return df_transformed


if __name__ == "__main__":
    transformer = StatisticsTransformer()
    # Specify the path to the history data in the bronze bucket
    transformer.process("type=statistics/date=2025_08_02/")
    # The transformed data will be written to the silver bucket
    # Uncomment the line below if you want to write the transformed data
    # transformer.write(df_transformed, SILVER_BUCKET, "type=history/date=2025_08_02/")