
import os
import tempfile
import concurrent.futures
from azure.storage.blob import BlobServiceClient

# Configuration
BLOB_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "data"
BLOB_FILE_NAMES = [
    "stnx_items.parquet",
    "stnx_sales.parquet",
    "inv_df.parquet",
    "customer_df.parquet",
    "industry_df.parquet",
    "material_df.parquet",
    "chp.parquet",
    "dt_df.parquet"
]

blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)
TEMP_DIR = tempfile.gettempdir()

def download_blob(blob_name):
    download_path = os.path.join(TEMP_DIR, blob_name)
    if os.path.exists(download_path):
        print(f"[INFO] File already exists locally: {blob_name}")
        return download_path
    print(f"[INFO] Downloading {blob_name}...")
    blob_client = container_client.get_blob_client(blob_name)
    with open(download_path, "wb") as f:
        blob_data = blob_client.download_blob()
        blob_data.readinto(f)
    print(f"[SUCCESS] Downloaded {blob_name} to {download_path}")
    return download_path

def preload_all_blobs():
    print("[START] Downloading all Parquet files...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(download_blob, BLOB_FILE_NAMES))
    print("[DONE] All files downloaded successfully.")

if __name__ == "__main__":
    preload_all_blobs()
