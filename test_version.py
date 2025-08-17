from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


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

spark.sql("SELECT fp.symbol, dc.company_name, fp.date, fp.open, fp.high, fp.low, fp.close FROM fact_price fp JOIN dim_company dc ON fp.symbol=dc.company_ticker").show(truncate=False)