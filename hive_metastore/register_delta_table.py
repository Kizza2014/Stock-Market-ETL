from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, quarter, dayofweek, explode


builder = (
    SparkSession.builder.appName("DeltaHiveIntegration")
    .config("spark.sql.catalogImplementation", "hive")  # dùng Hive metastore
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.sql.warehouse.dir", "s3a://silver/warehouse")
    .config("hive.metastore.warehouse.dir", "s3a://silver/warehouse")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .enableHiveSupport()
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# register dim_company
spark.sql("""
    CREATE TABLE IF NOT EXISTS dim_company
    USING DELTA
    LOCATION 's3a://silver/type=profile/date=2025_08_02'
""")
spark.sql("DESCRIBE FORMATTED dim_company").show(truncate=False)

# register dim_date
start_date = "2020-01-01"
end_date = "2025-12-31"

dates_df = (
    spark.sql(f"SELECT sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day) as date_seq")
    .select(explode(col("date_seq")).alias("date"))
    .withColumn("day", dayofmonth(col("date")))
    .withColumn("month", month(col("date")))
    .withColumn("year", year(col("date")))
    .withColumn("quarter", quarter(col("date")))
    .withColumn("day_of_week", dayofweek(col("date")))  # 1=Sunday, 7=Saturday
)

# Lưu vào Delta Table
dates_df.write.format("delta").mode("overwrite").save("s3a://silver/type=date_dim/date=all")

# Đăng ký bảng dim_date
spark.sql("""
    CREATE TABLE IF NOT EXISTS dim_date (
        date DATE,
        day INT,
        month INT,
        year INT,
        quarter INT,
        day_of_week INT
    )
    USING DELTA
    LOCATION 's3a://silver/type=date_dim/date=all'
""")
spark.sql("DESCRIBE FORMATTED dim_date").show(truncate=False)

# register fact_price
spark.sql("""
    CREATE TABLE fact_price
    USING DELTA
    LOCATION 's3a://silver/type=history/date=2025_08_02/'
""")
spark.sql("DESCRIBE FORMATTED fact_price").show(truncate=False)

# register fact_income_statement
spark.sql("""
    CREATE TABLE fact_income_statement
    USING DELTA
    LOCATION 's3a://silver/type=income_statement/date=2025_08_02/'
""")
spark.sql("DESCRIBE FORMATTED fact_income_statement").show(truncate=False)


# register fact_balance_sheet
spark.sql("""
    CREATE TABLE fact_balance_sheet
    USING DELTA
    LOCATION 's3a://silver/type=balance_sheet/date=2025_08_02/'
""")
spark.sql("DESCRIBE FORMATTED fact_balance_sheet").show(truncate=False)


# register fact_cash_flow
spark.sql("""
    CREATE TABLE fact_cash_flow
    USING DELTA
    LOCATION 's3a://silver/type=cash_flow/date=2025_08_02/'
""")
spark.sql("DESCRIBE FORMATTED fact_cash_flow").show(truncate=False)


# register fact_statistics
spark.sql("""
    CREATE TABLE fact_statistics
    USING DELTA
    LOCATION 's3a://silver/type=statistics/date=2025_08_02/'
""")
spark.sql("DESCRIBE FORMATTED fact_statistics").show(truncate=False)


