import os
import tempfile
import concurrent.futures
from azure.storage.blob import BlobServiceClient

# שליפת מחרוזת החיבור
BLOB_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "data"

# שמות הקבצים הדרושים בפועל לפי הקוד שלך
BLOB_FILE_NAMES = [
    "AGGR_MONTHLY_DW_CHP.parquet",
    "AGGR_MONTHLY_DW_FACT_STORENEXT_BY_INDUSTRIES_SALES.parquet",
    "AGGR_MONTHLY_DW_INVOICES.parquet",

    "AGGR_WEEKLY_DW_CHP.parquet",
    "AGGR_WEEKLY_DW_FACT_STORENEXT_BY_INDUSTRIES_SALES.parquet",
    "AGGR_WEEKLY_DW_INVOICES.parquet",

    "DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS.parquet",
    "DW_DIM_CUSTOMERS.parquet",
    "DW_DIM_INDUSTRIES.parquet",
    "DW_DIM_MATERIAL.parquet"
]

blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)
TEMP_DIR = tempfile.gettempdir()

def download_blob(blob_name):
    download_path = os.path.join(TEMP_DIR, blob_name)
    if os.path.exists(download_path):
        print(f"[INFO] כבר קיים מקומית: {blob_name}")
        return download_path
    try:
        print(f"[INFO] מוריד {blob_name}...")
        blob_client = container_client.get_blob_client(blob_name)
        with open(download_path, "wb") as f:
            blob_data = blob_client.download_blob()
            blob_data.readinto(f)
        print(f"[SUCCESS] נשמר ל: {download_path}")
        return download_path
    except Exception as e:
        print(f"[ERROR] כשל בהורדה של {blob_name}: {e}")
        return None

def preload_all_blobs():
    print("[START] מתחיל להוריד את קבצי ה־Parquet הדרושים...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        results = list(executor.map(download_blob, BLOB_FILE_NAMES))
    print("[DONE] הורדה הסתיימה.")
    return results

if __name__ == "__main__":
    preload_all_blobs()
