import os
from pyspark.sql import SparkSession, DataFrame


BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")
SILVER_BUCKET = os.getenv("SILVER_BUCKET", "silver")
BRONZE_PATH = os.getenv("COMPANY_BRONZE_PATH", "type=company")
SILVER_PATH = os.getenv("COMPANY_SILVER_PATH", "type=company_silver")


class BaseTransformer:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.df = self.read()

    def read(self, path: str) -> DataFrame:
        # Đọc dữ liệu từ bronze layer (ví dụ: parquet)
        return self.spark.read.parquet(path)

    def transform(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError

    def write(self, df: DataFrame, path: str):
        df.write.format("delta").mode("overwrite").save(path)

class CompanyTransformer(BaseTransformer):
    def transform(self, df: DataFrame) -> DataFrame:
        # Ví dụ: Làm sạch, chuẩn hóa dữ liệu công ty
        return df.dropDuplicates(["company_id"]).na.drop(subset=["company_id"])

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("TransformCompany") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    bronze_path = f"/mnt/lakehouse/{BRONZE_PATH}/company.parquet"
    silver_path = f"/mnt/lakehouse/{SILVER_PATH}/company"

    transformer = CompanyTransformer(spark)
    df = transformer.read(bronze_path)
    df_transformed = transformer.transform(df)
    transformer.write(df_transformed, silver_path)