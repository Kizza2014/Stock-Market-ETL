from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = (
    SparkSession.builder.appName("DeltaHiveIntegration")
    .config("spark.sql.catalogImplementation", "hive")  # dùng Hive metastore
    .config("hive.metastore.uris", "thrift://localhost:9083")  # metastore service
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.sql.warehouse.dir", "s3a://silver/warehouse")  # Thêm dòng này
    .config("hive.metastore.warehouse.dir", "s3a://silver/warehouse")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# register dim_company
spark.sql("""
    CREATE TABLE dim_company
    USING DELTA
    LOCATION 's3a://silver/type=profile/date=2025_08_02/'
""")

# # register fact_price
# spark.sql("""
#     CREATE TABLE fact_price
#     USING DELTA
#     LOCATION 's3a://silver/type=history/date=2025_08_02/'
# """)

# # register fact_income_statement
# spark.sql("""
#     CREATE TABLE fact_income_statement
#     USING DELTA
#     LOCATION 's3a://silver/type=income_statement/date=2025_08_02/'
# """)

# # register fact_balance_sheet
# spark.sql("""
#     CREATE TABLE fact_balance_sheet
#     USING DELTA
#     LOCATION 's3a://silver/type=balance_sheet/date=2025_08_02/'
# """)

# # register fact_cash_flow
# spark.sql("""
#     CREATE TABLE fact_cash_flow
#     USING DELTA
#     LOCATION 's3a://silver/type=cash_flow/date=2025_08_02/'
# """)

# # register fact_statistics
# spark.sql("""
#     CREATE TABLE fact_statistics
#     USING DELTA
#     LOCATION 's3a://silver/type=statistics/date=2025_08_02/'
# """)

