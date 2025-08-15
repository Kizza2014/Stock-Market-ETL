import os
import io
from dotenv import load_dotenv
from minio import Minio
import pandas as pd


class MinioClient:
    def __init__(self):
        self.client = self.create_minio_client()

    def get_minio_config(self):
        load_dotenv()  # Load environment variables from .env file
        return {
            "endpoint": "localhost:9000",
            "access_key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            "secret_key": os.getenv("MINIO_SECRET_KEY", "minioadmin123")
        }

    def create_minio_client(self):
        config = self.get_minio_config()
        client = Minio(
            config["endpoint"],
            access_key=config["access_key"],
            secret_key=config["secret_key"],
            secure=False # Set to True if using HTTPS
        )
        return client

    def ensure_bucket_exists(self, bucket_name):
        """Ensure that the specified bucket exists, creating it if necessary."""
        if not self.client.bucket_exists(bucket_name):
            self.client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' created.")
        else:
            print(f"Bucket '{bucket_name}' already exists.")

    def is_bucket_exists(self, bucket_name):
        """Check if the specified bucket exists."""
        return self.client.bucket_exists(bucket_name)

    def is_folder_exists(self, bucket_name, folder_prefix):
        # folder_prefix ví dụ: 'myfolder/' (có dấu / ở cuối)
        """Check if a folder exists in the specified MinIO bucket."""
        if not folder_prefix.endswith('/'):
            folder_prefix += '/'
        objects = self.client.list_objects(bucket_name, prefix=folder_prefix, recursive=True)
        return any(objects)
    
    def list_files_in_folder(self, bucket_name, prefix):
        """List all files in the specified MinIO bucket with a given prefix (folder)."""
        if self.is_bucket_exists(bucket_name):
            objects = self.client.list_objects(bucket_name, prefix=prefix, recursive=True)
            file_list = [obj.object_name for obj in objects]
            return file_list
        else:
            print(f"Bucket '{bucket_name}' does not exist. Cannot list files.")
            return []

    def upload_html_content_to_minio(self, bucket_name, html_content, object_name):
        """Upload HTML content (string) directly to MinIO without saving to disk."""
        if self.is_bucket_exists(bucket_name):
            data = html_content.encode("utf-8")
            self.client.put_object(
                bucket_name,
                object_name,
                io.BytesIO(data),   # serialize trước khi upload vào minio, do minio chỉ hỗ trợ stream hoặc file
                length=len(data),
                content_type="text/html"
            )
            print(f"HTML content uploaded to bucket '{bucket_name}' as '{object_name}'.")
        else:
            print(f"Bucket '{bucket_name}' does not exist. Cannot upload HTML content.")

    def upload_json_content_to_minio(self, bucket_name, json_content, object_name):
        """Upload JSON content (string) directly to MinIO without saving to disk."""
        if self.is_bucket_exists(bucket_name):
            data = json_content.encode("utf-8")
            self.client.put_object(
                bucket_name,
                object_name,
                io.BytesIO(data),   # serialize trước khi upload vào minio, do minio chỉ hỗ trợ stream hoặc file
                length=len(data),
                content_type="application/json"
            )
            print(f"JSON content uploaded to bucket '{bucket_name}' as '{object_name}'.")
        else:
            print(f"Bucket '{bucket_name}' does not exist. Cannot upload JSON content.")

    def upload_to_minio_as_parquet(self, rows_data, schema, object_name, bucket_name):
        """Convert dữ liệu thành parquet và upload trực tiếp lên MinIO (không lưu file vật lý)."""
        if not rows_data or not schema:
            print("Không có dữ liệu hoặc schema để lưu.")
            return

        df = pd.DataFrame(rows_data, columns=schema)
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        self.client.put_object(
            bucket_name,
            object_name,
            buffer,
            length=buffer.getbuffer().nbytes,
            content_type="application/octet-stream"
        )
        print(f"Parquet file uploaded to bucket '{bucket_name}' as '{object_name}'.")

    def read_html_content_from_minio(self, bucket_name, object_name):
        """Read HTML content directly from a file in MinIO bucket as a string."""
        if self.is_bucket_exists(bucket_name):
            response = self.client.get_object(bucket_name, object_name)
            try:
                html_content = response.read().decode("utf-8")
                return html_content
            finally:
                response.close()
                response.release_conn()
        else:
            print(f"Bucket '{bucket_name}' does not exist. Cannot read HTML content.")
            return None
        
    def read_parquet_from_minio(self, bucket, object_name):
        # Lấy file parquet từ MinIO dưới dạng bytes
        if self.is_bucket_exists(bucket):
            parquet_data = self.client.get_object(bucket, object_name)
            # Đọc parquet từ bytes bằng pandas
            parquet_bytes = io.BytesIO(parquet_data.read())
            df = pd.read_parquet(parquet_bytes)
            return df
        else:
            print(f"Bucket '{bucket}' does not exist. Cannot read Parquet file.")
            return None