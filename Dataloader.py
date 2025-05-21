import os
import pandas as pd
import tempfile
from pyluach import dates
import streamlit as st
import time
import concurrent.futures
from azure.storage.blob import BlobServiceClient
import tempfile


class DataLoader:
    def __init__(self):
        """Initialize the DataLoader."""
        self.running_local = self.detect_local_environment()
        self.parquet_dir = self.get_parquet_dir()

    def detect_local_environment(self):
        """Detect if running locally or in production."""
        return os.getenv("RUNNING_IN_PRODUCTION") != "true"

    def get_parquet_dir(self):
        """Return the path to the parquet files."""
        if self.running_local:
            print("[INFO] Running in LOCAL mode.")
            return os.path.join(os.getcwd(), "parquet_files")
        else:
            print("[INFO] Running in PRODUCTION mode.")
            return tempfile.gettempdir()

    def gregorian_to_hebrew(self, year, month, day):
        """Convert a Gregorian date to Hebrew date string."""
        return dates.GregorianDate(year, month, day).to_heb().hebrew_date_string()

    def get_jewish_holidays(self, year, month, day):
        """Get Jewish holidays for a given date."""
        return dates.GregorianDate(year, month, day).festival(hebrew=True)

    def create_date_dataframe(self, start_date, end_date):
        """Create a DataFrame with date range, Hebrew dates, and holidays."""
        dt_df = pd.DataFrame({'DATE': pd.date_range(start=start_date, end=end_date)})
        dt_df['HEBREW_DATE'] = dt_df['DATE'].apply(lambda x: self.gregorian_to_hebrew(x.year, x.month, x.day))
        dt_df['HOLIDAY'] = dt_df['DATE'].apply(lambda x: self.get_jewish_holidays(x.year, x.month, x.day))
        return dt_df

def read_parquet_file(file_path):
    """Helper function to read a parquet file."""
    try:
        df = pd.read_parquet(file_path, engine="pyarrow")
        for col in ['Day', 'DATE']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        return df
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None

@st.cache_resource
def load_data_with_progress():

    loader = st.session_state["Dataloader"]
    dataframes = {}
    max_workers = 32  # תרגיש חופשי להעלות ל־64 אם המחשב/שרת שלך סוחב

    def read_parquet_file(file_path):
        try:
            df = pd.read_parquet(file_path, engine="pyarrow")
            for col in ['Day', 'DATE']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            return df
        except Exception as e:
            print(f"[ERROR] בקריאת {file_path}: {e}")
            return None

    def download_and_read_blob(blob_name):
        path = os.path.join(tempfile.gettempdir(), blob_name)
        if not os.path.exists(path):
            blob_client = container_client.get_blob_client(blob_name)
            with open(path, "wb") as f:
                f.write(blob_client.download_blob().readall())
        df = read_parquet_file(path)
        return blob_name.replace(".parquet", ""), df

    if loader.running_local:
        files = [f for f in os.listdir(loader.parquet_dir) if f.endswith(".parquet")]
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                f: executor.submit(read_parquet_file, os.path.join(loader.parquet_dir, f))
                for f in files
            }
            for file_name, future in futures.items():
                df = future.result()
                if df is not None:
                    key = file_name.replace(".parquet", "")
                    dataframes[key] = df
    else:
        # Azure blob setup
        BLOB_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        CONTAINER_NAME = "data"
        BLOB_FILE_NAMES = [
            "AGGR_WEEKLY_DW_CHP.parquet",
            "AGGR_MONTHLY_DW_INVOICES.parquet",
            "AGGR_WEEKLY_DW_FACT_STORENEXT_BY_INDUSTRIES_SALES.parquet",
            "DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS.parquet",
            "DW_DIM_CUSTOMERS.parquet",
            "DW_DIM_INDUSTRIES.parquet",
            "DW_DIM_MATERIAL.parquet"
        ]
        blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = executor.map(download_and_read_blob, BLOB_FILE_NAMES)
            for key, df in results:
                if df is not None:
                    dataframes[key] = df

    # תוספת טבלת חגים
    if 'AGGR_WEEKLY_DW_FACT_STORENEXT_BY_INDUSTRIES_SALES' in dataframes:
        df = dataframes['AGGR_WEEKLY_DW_FACT_STORENEXT_BY_INDUSTRIES_SALES']
        start_date = df['Day'].min()
        end_date = df['Day'].max()
        dt_df = loader.create_date_dataframe(start_date, end_date)
        dataframes['DATE_HOLIAY_DATA'] = dt_df

    st.success("✅ All the files are loaded sucsessfuly")
    return dataframes
