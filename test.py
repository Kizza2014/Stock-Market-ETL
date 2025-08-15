from pyspark.sql import SparkSession
from transform.schema import COMPANY_SCHEMA, HISTORY_SCHEMA

# spark = SparkSession.builder \
#     .appName("TransformCompany") \
#     .master("local[*]") \
#     .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk:1.12.262") \
#     .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
#     .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
#     .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#     .config("spark.driver.memory", "2g") \
#     .config("spark.executor.memory", "2g") \
#     .getOrCreate()

# df = spark.read.schema(HISTORY_SCHEMA).parquet("s3a://bronze/type=history/date=2025_08_02/")
# df.show()
# df.printSchema()

# import pandas as pd

# for ticker in ["BTI", "CFG"]:
#     df = pd.read_csv(f"{ticker}_history_parsed.csv")
#     # thêm cột chứa ticker
#     df['ticker'] = ticker
#     df = df.astype(str)
#     # sắp xếp lại thứ tự các cột
#     df = df[['ticker'] + [col for col in df.columns if col != 'ticker']]
#     # lưu lại vào parquet
#     save_path = f"s3://bronze/type=history/date=2025_08_02/{ticker}_history_parsed.parquet"
#     df.to_parquet(
#         save_path, 
#         engine="pyarrow",
#         storage_options={
#             "key": "minioadmin",
#             "secret": "minioadmin123",
#             "endpoint_url": "http://localhost:9000"
#         }
#     )

# df = pd.read_parquet(
#     f"s3://bronze/type=history/date=2025_08_02/TSLA_history_parsed.parquet",
#     engine="pyarrow",
#     storage_options={
#         "key": "minioadmin",
#         "secret": "minioadmin123",
#         "endpoint_url": "http://localhost:9000"
#     }
# )
# print(df)
# print(df.dtypes)

# import pandas as pd

# df = pd.read_parquet("extract/data_test/crawl_5_years_history/crawled_on_2025_08_02/BTI_history_parsed.parquet")
# print(df.dtypes)

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TransformData") \
    .master("local[*]") \
    .config(
        "spark.jars.packages", 
        "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk:1.12.262,io.delta:delta-core_2.12:2.4.0"
    ) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

df = spark.read.format("delta").load("s3a://silver/type=history/date=2025_08_02/")
df.show()