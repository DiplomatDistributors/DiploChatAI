import os
import tempfile
import pandas as pd
import streamlit as st
from pyluach import dates
import concurrent.futures
import time

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

    def load_single_file(self, file_path):
        """Load a single parquet file, convert dates if needed."""
        try:
            df = pd.read_parquet(file_path, engine="pyarrow")
            for col in ['Day', 'DATE']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            key = os.path.basename(file_path).replace(".parquet", "")
            return key, df
        except Exception as e:
            print(f"[ERROR] Failed to load {file_path}: {e}")
            return None, None

    def load_data_with_progress(self):
        """Load all parquet files with a Streamlit progress bar."""
        dataframes = {}
        files = [os.path.join(self.parquet_dir, f) for f in os.listdir(self.parquet_dir) if f.endswith(".parquet")]
        total_files = len(files)

        if total_files == 0:
            st.error("❌ לא נמצאו קבצי פרקט לטעינה!")
            st.stop()

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

        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
            futures = {executor.submit(self.load_single_file, file_path): file_path for file_path in files}

            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                key, df = future.result()
                if key and df is not None:
                    dataframes[key] = df

                # Update progress bar
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

        # יצירת טבלת תאריכים
        if 'AGGR_WEEKLY_DW_FACT_STORENEXT_BY_INDUSTRIES_SALES' in dataframes:
            start_date = dataframes['AGGR_WEEKLY_DW_FACT_STORENEXT_BY_INDUSTRIES_SALES']['Day'].min()
            end_date = dataframes['AGGR_WEEKLY_DW_FACT_STORENEXT_BY_INDUSTRIES_SALES']['Day'].max()
            dt_df = self.create_date_dataframe(start_date, end_date)
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
