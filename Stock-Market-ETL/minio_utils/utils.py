import os
import io
from dotenv import load_dotenv
from minio import Minio


def get_minio_config():
    load_dotenv()  # Load environment variables from .env file
    return {
        "endpoint": os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        "access_key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        "secret_key": os.getenv("MINIO_SECRET_KEY", "minioadmin123")
    }

def create_minio_client():
    config = get_minio_config()
    client = Minio(
        config["endpoint"],
        access_key=config["access_key"],
        secret_key=config["secret_key"],
        secure=False # Set to True if using HTTPS
    )
    return client

def ensure_bucket_exists(client, bucket_name):
    """Ensure that the specified bucket exists, creating it if necessary."""
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' created.")
    else:
        print(f"Bucket '{bucket_name}' already exists.")

def upload_file_to_minio(client, bucket_name, file_path, object_name):
    """Upload a file to the specified MinIO bucket."""
    ensure_bucket_exists(client, bucket_name)
    client.fput_object(bucket_name, object_name, file_path)
    print(f"File '{file_path}' uploaded to bucket '{bucket_name}' as '{object_name}'.")

def upload_html_content_to_minio(client, bucket_name, html_content, object_name):
    """Upload HTML content (string) directly to MinIO without saving to disk."""
    ensure_bucket_exists(client, bucket_name)
    data = html_content.encode("utf-8")
    client.put_object(
        bucket_name,
        object_name,
        io.BytesIO(data),   # serialize trước khi upload vào minio, do minio chỉ hỗ trợ stream hoặc file
        length=len(data),
        content_type="text/html"
    )
    print(f"HTML content uploaded to bucket '{bucket_name}' as '{object_name}'.")

def download_file_from_minio(client, bucket_name, object_name, file_path):
    """Download a file from the specified MinIO bucket."""
    client.fget_object(bucket_name, object_name, file_path)
    print(f"File '{object_name}' downloaded from bucket '{bucket_name}' to '{file_path}'.")

def list_files_in_bucket(client, bucket_name):
    """List all files in the specified MinIO bucket."""
    objects = client.list_objects(bucket_name, recursive=True)
    file_list = [obj.object_name for obj in objects]
    return file_list

def delete_file_from_minio(client, bucket_name, object_name):
    """Delete a file from the specified MinIO bucket."""
    client.remove_object(bucket_name, object_name)
    print(f"File '{object_name}' deleted from bucket '{bucket_name}'.")