import os
import pandas as pd
import tempfile
from pyluach import dates
import streamlit as st
import time
import concurrent.futures

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
def load_data_with_progress(parquet_dir: str):
    """Load parquet files with a Streamlit progress bar, cached and parallelized."""
    dataframes = {}
    files = [f for f in os.listdir(parquet_dir) if f.endswith(".parquet")]
    total_files = len(files)

    # Styling for progress bar
    st.markdown("""
        <style>
        .progress-container {
            width: 100%;
            background-color: #e0e0e0;
            border-radius: 10px;
            overflow: hidden;
            direction: rtl;
            height: 15px;
            margin-bottom: 10px;
            position: relative;
        }
        .progress-bar {
            height: 100%;
            background: linear-gradient(45deg, #8000FF, #FF00FF);
            width: 0%;
            border-radius: 10px;
            transition: width 0.5s;
        }
        </style>
    """, unsafe_allow_html=True)

    progress_html = st.empty()

    # Use ThreadPoolExecutor to parallelize reading
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = {}
        for file_name in files:
            file_path = os.path.join(parquet_dir, file_name)
            futures[file_name] = executor.submit(read_parquet_file, file_path)

        for i, (file_name, future) in enumerate(futures.items()):
            df = future.result()
            if df is not None:
                key = file_name.replace(".parquet", "")
                dataframes[key] = df

            progress_percentage = int(((i + 1) / total_files) * 100)
            progress_html.markdown(
                f"""
                <div class="progress-container">
                    <div class="progress-bar" style="width: {progress_percentage}%;"></div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    st.success("✅ כל הקבצים נטענו בהצלחה!")
    progress_html.empty()

    # Add dates dataframe
    if 'AGGR_WEEKLY_DW_FACT_STORENEXT_BY_INDUSTRIES_SALES' in dataframes:
        loader = DataLoader()
        start_date = dataframes['AGGR_WEEKLY_DW_FACT_STORENEXT_BY_INDUSTRIES_SALES']['Day'].min()
        end_date = dataframes['AGGR_WEEKLY_DW_FACT_STORENEXT_BY_INDUSTRIES_SALES']['Day'].max()
        dt_df = loader.create_date_dataframe(start_date, end_date)
        dataframes['DATE_HOLIAY_DATA'] = dt_df

    return {
        'chp': dataframes.get('AGGR_WEEKLY_DW_CHP'),
        'inv_df': dataframes.get('AGGR_MONTHLY_DW_INVOICES'),
        'stnx_sales': dataframes.get('AGGR_WEEKLY_DW_FACT_STORENEXT_BY_INDUSTRIES_SALES'),
        'stnx_items': dataframes.get('DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS'),
        'customer_df': dataframes.get('DW_DIM_CUSTOMERS', pd.DataFrame()).drop_duplicates(subset=['CUSTOMER_CODE']),
        'industry_df': dataframes.get('DW_DIM_INDUSTRIES'),
        'material_df': dataframes.get('DW_DIM_MATERIAL', pd.DataFrame()).drop_duplicates(subset=['MATERIAL_NUMBER']),
        'dt_df': dataframes.get('DATE_HOLIAY_DATA')
    }
