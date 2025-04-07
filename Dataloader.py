import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import os
from pyluach import dates
from dotenv import load_dotenv
import time
import schedule
import threading
from concurrent.futures import ThreadPoolExecutor
import streamlit as st
load_dotenv()


class DataLoader:
    def __init__(self):
        """Initialize the DataLoader with database connection details."""
        self.db_password = os.getenv("DB_PASSWORD")
        self.server = "diplomat-analytics-server.database.windows.net"
        self.database = "Diplochat-DB"
        self.user = "analyticsadmin"
        self.parquet_dir = "parquet_files"  # Directory to save Parquet files
        os.makedirs(self.parquet_dir, exist_ok=True)  # Ensure directory exists

    def _get_engine(self):
        """Create a SQLAlchemy engine."""
        connection_string = (
            f"mssql+pyodbc://{self.user}:{self.db_password}@{self.server}/{self.database}?"
            "driver=ODBC+Driver+17+for+SQL+Server"
        )
        return create_engine(connection_string)

    def gregorian_to_hebrew(self, year, month, day):
        """Convert a Gregorian date to Hebrew date string."""
        return dates.GregorianDate(year, month, day).to_heb().hebrew_date_string()

    def get_jewish_holidays(self, year, month, day):
        """Get Jewish holidays for a given date."""
        return dates.GregorianDate(year, month, day).festival(hebrew=True)

    def create_date_dataframe(self, start_date, end_date):
        """Create a DataFrame with the date range, Hebrew dates, and Jewish holidays."""
        dt_df = pd.DataFrame({'DATE': pd.date_range(start=start_date, end=end_date)})
        dt_df['HEBREW_DATE'] = dt_df['DATE'].apply(lambda x: self.gregorian_to_hebrew(x.year, x.month, x.day))
        dt_df['HOLIDAY'] = dt_df['DATE'].apply(lambda x: self.get_jewish_holidays(x.year, x.month, x.day))
        return dt_df

    def load_table_chunked(self, name, query, engine, chunk_size=10000):
        """Load a table in chunks and return the concatenated DataFrame."""
        start_time = time.time()
        chunks = []
        try:
            for chunk in pd.read_sql_query(query, engine, chunksize=chunk_size):
                chunks.append(chunk)
            df = pd.concat(chunks, ignore_index=True)
            duration = time.time() - start_time
            print(f"Table '{name}' loaded with {len(df)} rows in {duration:.2f} seconds.")
            return name, df
        except Exception as e:
            print(f"Error loading table '{name}': {e}")
            return name, None

    def save_to_parquet(self, name, df):
        """Save DataFrame to Parquet."""
        filepath = os.path.join(self.parquet_dir, f"{name}.parquet")
        df.to_parquet(filepath, index=False)
        print(f"Table '{name}' saved to {filepath}.")

    def load_data_to_parquet(self):
        """Load all Monthly data from the database using threads."""
        engine = self._get_engine()

        # Define all tables and queries for Weekly resolution
        tables = {

            # CHP
            'AGGR_WEEKLY_DW_CHP': """
                SELECT DATE, BARCODE, CHAIN, AVG_PRICE, AVG_SELLOUT_PRICE, SELLOUT_DESCRIPTION, NUMBER_OF_STORES
                FROM [dbo].[AGGR_WEEKLY_DW_CHP]
            """,

            'AGGR_MONTHLY_DW_CHP': """
                SELECT DATE, BARCODE, CHAIN, AVG_PRICE, AVG_SELLOUT_PRICE, SELLOUT_DESCRIPTION, NUMBER_OF_STORES
                FROM [dbo].[AGGR_WEEKLY_DW_CHP]
            """,

            # INVOICES
            'AGGR_WEEKLY_DW_INVOICES': """
                SELECT [DATE], [SALES_ORGANIZATION_CODE], [MATERIAL_CODE], [INDUSTRY_CODE], [CUSTOMER_CODE], [Gross],
                        [Net], [Net VAT], [Gross VAT], [Units]
                FROM [dbo].[AGGR_MONTHLY_DW_INVOICES]
            """,

            'AGGR_MONTHLY_DW_INVOICES': """
                SELECT [DATE], [SALES_ORGANIZATION_CODE], [MATERIAL_CODE], [INDUSTRY_CODE], [CUSTOMER_CODE], [Gross],
                        [Net], [Net VAT], [Gross VAT], [Units]
                FROM [dbo].[AGGR_MONTHLY_DW_INVOICES]
            """,

            # STORNEXT
            'AGGR_WEEKLY_DW_FACT_STORENEXT_BY_INDUSTRIES_SALES': """
                SELECT Day, Barcode, Format_Name, Sales_NIS, Sales_Units, Price_Per_Unit
                FROM [dbo].[AGGR_WEEKLY_DW_FACT_STORENEXT_BY_INDUSTRIES_SALES]
            """,

            'AGGR_MONTHLY_DW_FACT_STORENEXT_BY_INDUSTRIES_SALES': """
                SELECT Day, Barcode, Format_Name, Sales_NIS, Sales_Units, Price_Per_Unit
                FROM [dbo].[AGGR_WEEKLY_DW_FACT_STORENEXT_BY_INDUSTRIES_SALES]
            """,

            'DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS': """
                SELECT Barcode, Item_Name, Category_Name, Sub_Category_Name, Brand_Name, Sub_Brand_Name, Supplier_Name
                FROM [dbo].[DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS]
            """,

            'DW_DIM_CUSTOMERS': """
                SELECT [CUSTOMER_CODE], [CUSTOMER], [CITY], [CUSTOMER_ADDRESS], [CUST_LATITUDE], [CUST_LONGITUDE]
                FROM [dbo].[DW_DIM_CUSTOMERS]
            """,

            'DW_DIM_INDUSTRIES': """
                SELECT [INDUSTRY], [INDUSTRY_CODE]
                FROM [dbo].[DW_DIM_INDUSTRIES]
            """,

            'DW_DIM_MATERIAL': """
                SELECT [MATERIAL_NUMBER], [MATERIAL_EN], [MATERIAL_HE], [MATERIAL_DIVISION], [BRAND_HEB], [BRAND_ENG],
                        [SUB_BRAND_HEB], [SUB_BRAND_ENG], [CATEGORY_HEB], [CATEGORY_ENG], [BARCODE_EA],
                        [SALES_UNIT], [BOXING_SIZE]
                FROM [dbo].[DW_DIM_MATERIAL]
            """,

            'USERS_DIM': """
                SELECT * FROM [dbo].[DW_DIM_USERS]
            """,

            'AI_LOG': """
                SELECT [ID], [Conversation_ID], [Timestamp], [User_Name], [User_Prompt], [LLM_Responses],
                        [Code_Extractions], [Final_Answer], [Num_Attempts], [Num_LLM_Calls], [Errors], [Total_Time],
                        [User_Ratings], [Usage]
                FROM [dbo].[AI_LOG]
            """
        }


        # Load all tables in parallel using threads
        with ThreadPoolExecutor(max_workers=8) as executor:
            results = executor.map(lambda item: self.load_table_chunked(item[0], item[1], engine), tables.items())

        # Collect results into a dictionary
        dataframes = {name: df for name, df in results if df is not None}

        # Save to Parquet and add holiday data
        for name, df in dataframes.items():
            self.save_to_parquet(name, df)

        # Create holiday data
        start_date = dataframes['AGGR_WEEKLY_DW_FACT_STORENEXT_BY_INDUSTRIES_SALES']['Day'].min()
        end_date = dataframes['AGGR_WEEKLY_DW_FACT_STORENEXT_BY_INDUSTRIES_SALES']['Day'].max()
        dt_df = self.create_date_dataframe(start_date, end_date)
        self.save_to_parquet("DATE_HOLIAY_DATA", dt_df)

        return dataframes

    def load_parquets(self):
        parquet_dir = "parquet_files"
        dataframes = {}
        files = [f for f in os.listdir(parquet_dir) if f.endswith(".parquet")]

        for i, file_name in enumerate(files):
            file_path = os.path.join(parquet_dir, file_name)
            df = pd.read_parquet(file_path)
            
            dataframes[file_name.replace('.parquet', '')] = df
            for col in ['Day', 'DATE']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')

        # Save the dataframes on the states in a language that the model allready knew (By the prompt)
        return {
             'chp': dataframes['AGGR_WEEKLY_DW_CHP'],
             'inv_df': dataframes['AGGR_MONTHLY_DW_INVOICES'],
             'stnx_sales': dataframes['AGGR_WEEKLY_DW_FACT_STORENEXT_BY_INDUSTRIES_SALES'],
             'stnx_items': dataframes['DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS'],
             'customer_df': dataframes['DW_DIM_CUSTOMERS'].drop_duplicates(subset = ['CUSTOMER_CODE']),
             'industry_df':  dataframes['DW_DIM_INDUSTRIES'],
             'material_df': dataframes['DW_DIM_MATERIAL'].drop_duplicates(subset = ['MATERIAL_NUMBER']),
             'dt_df': dataframes['DATE_HOLIAY_DATA'],
             'log_df': dataframes['AI_LOG']
        }
    
    def load_data_with_progress(self):
        # CSS מותאם אישית לגרדיאנט ולכיוון RTL
        st.markdown(
            """
            <style>
            .progress-container {
                width: 100%;
                background-color: #e0e0e0;
                border-radius: 10px;
                overflow: hidden;
                direction: rtl; /* שינוי כיוון ל-RTL */
                height: 15px;
                margin-bottom: 10px;
                position: relative;
            }
            .progress-bar {
                height: 100%;
                background: linear-gradient(45deg, #8000FF, #FF00FF); /* גרדיאנט מותאם */
                width: 0%; /* יתחיל מ-0 */
                border-radius: 10px;
                transition: width 0.5s; /* אנימציה חלקה */
            }
            </style>
            """,
            unsafe_allow_html=True,
        )

        parquet_dir = "parquet_files"
        dataframes = {}
        files = [f for f in os.listdir(parquet_dir) if f.endswith(".parquet")]
        total_files = len(files)

        # יצירת Progress Bar מותאם
        progress_html = st.empty()

        for i, file_name in enumerate(files):
            file_path = os.path.join(parquet_dir, file_name)
            df = pd.read_parquet(file_path)
            
            dataframes[file_name.replace('.parquet', '')] = df
            for col in ['Day', 'DATE']:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    
            # חישוב אחוז ההתקדמות
            progress_percentage = int(((i + 1) / total_files) * 100)

            # הצגת Progress Bar ב-HTML
            progress_html.markdown(
                f"""
                <div class="progress-container">
                    <div class="progress-bar" style="width: {progress_percentage}%;"></div>
                </div>
                """,
                unsafe_allow_html=True,
            )

            time.sleep(0.5)

        st.success("הנתונים הועלו בהצלחה")
        progress_html.empty()

        # Save the dataframes on the states in a language that the model allready knew (By the prompt)
        return {
             'chp': dataframes['AGGR_WEEKLY_DW_CHP'],
             'inv_df': dataframes['AGGR_MONTHLY_DW_INVOICES'],
             'stnx_sales': dataframes['AGGR_WEEKLY_DW_FACT_STORENEXT_BY_INDUSTRIES_SALES'],
             'stnx_items': dataframes['DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS'],
             'customer_df': dataframes['DW_DIM_CUSTOMERS'].drop_duplicates(subset = ['CUSTOMER_CODE']),
             'industry_df':  dataframes['DW_DIM_INDUSTRIES'],
             'material_df': dataframes['DW_DIM_MATERIAL'].drop_duplicates(subset = ['MATERIAL_NUMBER']),
             'dt_df': dataframes['DATE_HOLIAY_DATA'],
             'log_df': dataframes['AI_LOG']
        }



# Manual run
if __name__ == "__main__":
    loader = DataLoader()
    loader.load_data_to_parquet()
