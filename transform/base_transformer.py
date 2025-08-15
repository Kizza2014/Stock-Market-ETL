import os
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from py4j.protocol import Py4JJavaError


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")
SILVER_BUCKET = os.getenv("SILVER_BUCKET", "silver")


class BaseTransformer:
    def __init__(self):
        self.spark = self.create_spark_session()

    def create_spark_session(self):
        spark = SparkSession.builder \
            .appName("TransformData") \
            .master("local[*]") \
            .config(
                "spark.jars.packages", 
                "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk:1.12.262,io.delta:delta-core_2.12:2.4.0"
            ) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .getOrCreate()
        return spark

    def process(self, path):
        print(f"Reading data from s3a://{BRONZE_BUCKET}/{path}")
        df = self.read(BRONZE_BUCKET, path)
        print("Sample data:")
        df.show()
        print(f"Count data: {df.count()}")
        print("Schema:")
        df.printSchema()

        print(f"Transforming data...")
        df_transformed = self.transform(df)
        print("Sample data:")
        df_transformed.show()
        print(f"Count data: {df_transformed.count()}")
        print("Schema:")
        df_transformed.printSchema()

        print(f"Writing data to s3a://{SILVER_BUCKET}/{path}")
        self.write(df_transformed, SILVER_BUCKET, path)

    def read(self, bucket_name, path, schema=None):
        # nếu chỉ để đường dẫn tới folder, đọc tất cả các file trong thư mục
        try:
            if schema:
                return self.spark.read.schema(schema).parquet(f"s3a://{bucket_name}/{path}")
            else:
                return self.spark.read.parquet(f"s3a://{bucket_name}/{path}")
        except (AnalysisException, Py4JJavaError, Exception) as e:
            print(f"Error reading data: {e}")
            raise

    def transform(self, df):
        raise NotImplementedError("This method should be implemented by subclasses")

    def write(self, df, bucket_name, path):
        # ghi lại dưới định dạng delta
        try:
            df.write.format("delta").mode("overwrite").save(f"s3a://{bucket_name}/{path}")
        except (AnalysisException, Py4JJavaError, Exception) as e:
            print(f"Error writing data: {e}")
            raise