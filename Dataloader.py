import os
import pandas as pd
import tempfile
from pyluach import dates
import streamlit as st
import time
import concurrent.futures
import numpy as np

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

def load_selected_parquets(parquet_dir="parquet_files"):
    """
    Load specific Parquet files from a given directory.

    Returns:
        dict[str, pd.DataFrame]: Dictionary of DataFrames keyed by filename (without .parquet).
    """
    selected_files = [
    "AGGR_MONTHLY_DW_CHP.parquet",
    "AGGR_MONTHLY_DW_FACT_STORENEXT_BY_INDUSTRIES_SALES.parquet",
    "AGGR_MONTHLY_DW_INVOICES.parquet",
    "DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS.parquet",
    "DW_DIM_CUSTOMERS.parquet",
    "DW_DIM_INDUSTRIES.parquet",
    "DW_DIM_MATERIAL.parquet"
    ]

    dataframes = {}

    # Use ThreadPoolExecutor to parallelize reading
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = {}
        for file_name in selected_files:
            file_path = os.path.join(parquet_dir, file_name)
            futures[file_name] = executor.submit(read_parquet_file, file_path)

        for i, (file_name, future) in enumerate(futures.items()):
            df = future.result()
            print(f"✅ Loaded {file_name:<60} → {df.shape[0]:,} rows")
            if df is not None:
                key = file_name.replace(".parquet", "")
                dataframes[key] = df

    # Load entity table with embeddings
    stnx_entities = pd.read_parquet("embeddings/stnx_entities.parquet")
    chp_entities = pd.read_parquet("embeddings/chp_entities.parquet")
    customer_entities = pd.read_parquet("embeddings/customer_entities.parquet")
    combined_entities = pd.concat([stnx_entities, chp_entities,customer_entities], ignore_index=True)

    def clean_and_tag_metadata(row):
        meta = row.get("metadata", {})
        if isinstance(meta, dict):
            cleaned_meta = {k: v for k, v in meta.items() if v is not None}
            cleaned_meta["source_table"] = row.get("source_table")
            cleaned_meta["column"] = row.get("type")
            return cleaned_meta
        return {}

    combined_entities["metadata"] = combined_entities.apply(clean_and_tag_metadata, axis=1)
    dataframes['vector_database'] = combined_entities
    
    return dataframes


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
def load_data_with_progress(parquet_dir: str):
    """Load parquet files with Streamlit progress bar (excluding vector_database.parquet)."""
    EXCLUDED_PARQUETS = {
    "vector_database.parquet",
    "stnx_entities_meta.parquet",
    "chp_entities_meta.parquet",
    "customer_entities_meta.parquet"
    }

    dataframes = {}
    files = [f for f in os.listdir(parquet_dir) if f.endswith(".parquet") and f not in EXCLUDED_PARQUETS]

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = {}
        for file_name in files:
            file_path = os.path.join(parquet_dir, file_name)
            futures[file_name] = executor.submit(read_parquet_file, file_path)

        for i, (file_name, future) in enumerate(futures.items()):
            df = future.result()
            if df is not None:
                key = file_name.replace(".parquet", "")
                dataframes[key] = df

    # Load date table if needed
    if 'AGGR_WEEKLY_DW_FACT_STORENEXT_BY_INDUSTRIES_SALES' in dataframes:
        loader = DataLoader()
        start_date = dataframes['AGGR_WEEKLY_DW_FACT_STORENEXT_BY_INDUSTRIES_SALES']['Day'].min()
        end_date = dataframes['AGGR_WEEKLY_DW_FACT_STORENEXT_BY_INDUSTRIES_SALES']['Day'].max()
        dataframes['DATE_HOLIAY_DATA'] = loader.create_date_dataframe(start_date, end_date)

    st.success("✅ Main data loaded successfully")
    print(dataframes.keys())
    return dataframes


def load_vector_database(parquet_dir: str):
    def load_entity_pair(name):
        meta_path = os.path.join(parquet_dir, f"{name}_meta.parquet")
        embedding_path = os.path.join(parquet_dir, f"{name}_embedding.npy")

        df = pd.read_parquet(meta_path)
        embeddings = np.load(embedding_path)

        # ודא שכל embedding הוא np.ndarray
        df["embedding"] = [np.array(vec, dtype=np.float32) for vec in embeddings]
        return df

    stnx_entities = load_entity_pair("stnx_entities")
    chp_entities = load_entity_pair("chp_entities")
    customer_entities = load_entity_pair("customer_entities")

    combined_entities = pd.concat([stnx_entities, chp_entities, customer_entities], ignore_index=True)

    def clean_and_tag_metadata(row):
        meta = row.get("metadata", {})
        if isinstance(meta, dict):
            cleaned_meta = {k: v for k, v in meta.items() if v is not None}
            cleaned_meta["source_table"] = row.get("source_table")
            cleaned_meta["column"] = row.get("type")
            return cleaned_meta
        return {}

    combined_entities["metadata"] = combined_entities.apply(clean_and_tag_metadata, axis=1)

    if combined_entities is not None:
        st.success("✅ Vector database loaded")
    return combined_entities