import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from io import BytesIO
import os
from dotenv import load_dotenv

load_dotenv()

RAW_PATH = "/home/zap/raw_data/crawl_companies_list"
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_KEY = os.getenv("MINIO_KEY")
MINIO_SECRET = os.getenv("MINIO_SECRET")
BUCKET_NAME = "bronze"

def get_latest_file_from_path(path, extension=".json"):
    # Get a list of files in the directory with the specified extension
    files = [os.path.join(path, f) for f in os.listdir(path) if f.endswith(extension)]
    
    # If there are no files in the directory, return None
    if not files:
        return None
    
    # Find the latest file based on modification time
    latest_file = max(files, key=os.path.getmtime)
    return latest_file


def load_data_from_src_to_s3(src):
    # get latest file
    latest_file = get_latest_file_from_path(src)

    if latest_file:
        # read json
        data = pd.read_json(latest_file)
        print(f"Read {latest_file}")

        # save to minio in parquet format
        filename = os.path.splitext(latest_file)[0]

        # convert from dataframe to parquet and save to buffer
        buffer = BytesIO()
        table = pa.Table.from_pandas(data)
        pq.write_table(table, buffer)
        buffer.seek(0)

        # create minio client
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_KEY,
            secret_key=MINIO_SECRET,
            secure=False
        )

        # make object name
        object_name = "companies_list/" + filename.split("/")[-1] + ".parquet"

        # make sure that bucket is exist
        if not client.bucket_exists(BUCKET_NAME):
            client.make_bucket(BUCKET_NAME)

        # Upload từ buffer lên MinIO
        client.put_object(
            bucket_name=BUCKET_NAME,
            object_name=object_name,
            data=buffer,
            length=buffer.getbuffer().nbytes,
            content_type="application/octet-stream"
        )
        print(f"Data saved to {BUCKET_NAME + "/" + object_name}")
    else:
        print("No file found")

if __name__ == "__main__":
    load_data_from_src_to_s3(RAW_PATH)