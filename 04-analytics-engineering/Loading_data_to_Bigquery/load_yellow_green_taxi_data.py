import os
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden
import time
import itertools
import pandas as pd

# Change this to your bucket name
BUCKET_NAME = "ny-taxi-rides-485210-bucket"

# If you authenticated through the GCP SDK you can comment out these two lines
CREDENTIALS_FILE = "gcs.json"
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
# If commented initialize client with the following
# client = storage.Client(project='zoomcamp-mod3-datawarehouse')


BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
COLORS = ["yellow", "green"]
YEARS = ["2019", "2020"]
MONTHS = [f"{i:02d}" for i in range(1, 13)] #from 1 to 12
DOWNLOAD_DIR = "."

CHUNK_SIZE = 8 * 1024 * 1024

SCHEMAS = {
    "yellow": {
        "passenger_count": "float64",
        "trip_distance": "float64",
        "RatecodeID": "float64",
        "fare_amount": "float64",
        "extra": "float64",
        "mta_tax": "float64",
        "tip_amount": "float64",
        "tolls_amount": "float64",
        "improvement_surcharge": "float64",
        "total_amount": "float64",
        "congestion_surcharge": "float64",
        "airport_fee": "float64",
        "VendorID": "Int64",
        "PULocationID": "Int64",
        "DOLocationID": "Int64",
        "payment_type": "Int64"
    },
    "green": {
        "RatecodeID": "float64",
        "passenger_count": "float64",
        "trip_distance": "float64",
        "fare_amount": "float64",
        "extra": "float64",
        "mta_tax": "float64",
        "tip_amount": "float64",
        "tolls_amount": "float64",
        "ehail_fee": "float64",
        "improvement_surcharge": "float64",
        "total_amount": "float64",
        "payment_type": "float64",
        "trip_type": "float64",
        "congestion_surcharge": "float64",
        "VendorID": "Int64",
        "PULocationID": "Int64",
        "DOLocationID": "Int64"
    }
}

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

bucket = client.bucket(BUCKET_NAME)


def download_file(color,year,month):
    url = f"{BASE_URL}{color}_tripdata_{year}-{month}.parquet"
    file_path = os.path.join(DOWNLOAD_DIR, f"{color}_tripdata_{year}-{month}.parquet")

    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, file_path)
        print(f"Downloaded: {file_path}")
        return file_path
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None



def download_and_fix(color, year, month):
    # 1. Run your existing download function
    local_path = download_file(color, year, month)

    if local_path:
        # 2. Read the file into Pandas
        df = pd.read_parquet(local_path)
        
        # 3. Apply the strict schema for this color
        color_schema = SCHEMAS.get(color.lower(), {})
        for col, dtype in color_schema.items():
            if col in df.columns:
                df[col] = df[col].astype(dtype)
        
        # 4. Save the file back to disk (This updates the Parquet metadata)
        df.to_parquet(local_path, engine='pyarrow', index=False)
        return local_path
    return None


def create_bucket(bucket_name):
    try:
        # Get bucket details
        bucket = client.get_bucket(bucket_name)

        # Check if the bucket belongs to the current project
        project_bucket_ids = [bckt.id for bckt in client.list_buckets()]
        if bucket_name in project_bucket_ids:
            print(
                f"Bucket '{bucket_name}' exists and belongs to your project. Proceeding..."
            )
        else:
            print(
                f"A bucket with the name '{bucket_name}' already exists, but it does not belong to your project."
            )
            sys.exit(1)

    except NotFound:
        # If the bucket doesn't exist, create it
        bucket = client.create_bucket(bucket_name)
        print(f"Created bucket '{bucket_name}'")
    except Forbidden:
        # If the request is forbidden, it means the bucket exists but you don't have access to see details
        print(
            f"A bucket with the name '{bucket_name}' exists, but it is not accessible. Bucket name is taken. Please try a different bucket name."
        )
        sys.exit(1)


def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)


def upload_to_gcs(file_path, max_retries=3):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    create_bucket(BUCKET_NAME)

    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")

            if verify_gcs_upload(blob_name):
                print(f"Verification successful for {blob_name}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {file_path} to GCS: {e}")

        time.sleep(5)

    print(f"Giving up on {file_path} after {max_retries} attempts.")


if __name__ == "__main__":
    create_bucket(BUCKET_NAME)

    #using itertools.product to iterate every colors, year and month output is tuple (c,y,m)
    task_list = list(itertools.product(COLORS, YEARS, MONTHS))
    colors, years, months = zip(*task_list)

    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(download_and_fix, colors, years, months))

    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_gcs, filter(None, file_paths))  # Remove None values

    print("All files processed and verified.")