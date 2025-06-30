import streamlit as st
import numpy as np
import pandas as pd
import base64
from io import BytesIO
import matplotlib.pyplot as plt
from streamlit_elements import elements, mui
import os
from streamlit_navigation_bar import st_navbar
import json
from sqlalchemy import create_engine
import streamlit as st
from openai import AzureOpenAI
from st_bridge import bridge, html

def load_css():
    st.markdown(
    """
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha3/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-KyZXEAg3QhqLMpG8r+Knujsl5/IIY6yy06zjsb4p6IeFDUnqfCpCILW9vpfl8RWz" crossorigin="anonymous">
    """,
    unsafe_allow_html=True,
    )
    with open("style.css") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)


def get_base64_image(image_path):
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode()

def create_navigation_bar():
    pages = ["Home Page", "General Settings"]
    logo_path = "logo.svg"

    styles = {
        "nav": {
            "background": "linear-gradient(0deg, rgba(239,236,245,1) 0%, rgba(246,246,246,1) 22%, rgba(255,255,255,1) 56%)",
            "height": "6.25rem",
            "font-size": "25px",
            "width": "100%",
            "font-family": "Calibri, sans-serif",
            "direction": "ltr",
            "display": "flex",
            "justify-content": "space-between",  # אם יש לוגו מצד אחד ותפריט מצד שני
            "align-items": "center",
            "padding": "0 20px",
            "box-shadow": "0px 4px 10px rgba(0, 0, 0, 0.3)"
        },
        "img": {
            "height": "7.875rem",
            "margin-right": "65px",
        },
        "li": {
            "padding": "0 35px",
            "white-space": "nowrap",
            "margin-left": "13px",
            "transition": "background-color 0.2s, color 0.3s",
        },
        "span": {
            "border-radius": "0.5rem",
            "color": "#726983",
            "padding": "0.4375rem 0.625rem",
            "transition": "background-color 0.2s, color 0.1s"
        },
        "active": {
            "background": "linear-gradient(0deg, rgba(185,166,224,1) 0%, rgba(139,116,186,1) 100%)",
            "color": "white"
        },
        "hover": {
            "background-color": "#bbb8c1",
            "color": "white"
        },
    }
    options = {
        "show_menu": False,
        "show_sidebar": False,
    }

    selected_page = st_navbar(pages, logo_path=logo_path, styles=styles, options=options)
    st.session_state['page'] = selected_page

def load_lottie_file(filepath: str):
    with open(filepath, "r") as f:
        return json.load(f)

    return cleaned_code

def get_local_scope():
    dataframes = st.session_state['Dataframes']
    scope = {}
    if st.session_state.get('aggregation_mode') == 'Monthly':
        scope = {
            'chp': dataframes.get('AGGR_MONTHLY_DW_CHP'),
            'inv_df': dataframes.get('AGGR_MONTHLY_DW_INVOICES'),
            'stnx_sales': dataframes.get('AGGR_MONTHLY_DW_FACT_STORENEXT_BY_INDUSTRIES_SALES'),
            'stnx_items': dataframes.get('DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS'),
            'customer_df': dataframes.get('DW_DIM_CUSTOMERS', pd.DataFrame()).drop_duplicates(subset=['CUSTOMER_CODE']),
            'industry_df': dataframes.get('DW_DIM_INDUSTRIES'),
            'material_df': dataframes.get('DW_DIM_MATERIAL', pd.DataFrame()).drop_duplicates(subset=['MATERIAL_NUMBER']),
            'dt_df': dataframes.get('DATE_HOLIAY_DATA'),
            'vector_database' : dataframes.get('vector_database')
        }
    
    elif st.session_state.get('aggregation_mode') == 'Weekly':
        scope = {
            'chp': dataframes.get('AGGR_WEEKLY_DW_CHP'),
            'inv_df': dataframes.get('AGGR_WEEKLY_DW_INVOICES'),
            'stnx_sales': dataframes.get('AGGR_WEEKLY_DW_FACT_STORENEXT_BY_INDUSTRIES_SALES'),
            'stnx_items': dataframes.get('DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS'),
            'customer_df': dataframes.get('DW_DIM_CUSTOMERS', pd.DataFrame()).drop_duplicates(subset=['CUSTOMER_CODE']),
            'industry_df': dataframes.get('DW_DIM_INDUSTRIES'),
            'material_df': dataframes.get('DW_DIM_MATERIAL', pd.DataFrame()).drop_duplicates(subset=['MATERIAL_NUMBER']),
            'dt_df': dataframes.get('DATE_HOLIAY_DATA'),
            'vector_database' : dataframes.get('vector_database')
        }

    st.session_state['local_scope'] = scope
    st.session_state['local_scope'].update({'pd':pd,'np':np,'base64':base64,'BytesIO':BytesIO,'plt':plt})
    return st.session_state['local_scope']

def create_aggregation_option():
    
    def set_aggregation_mode(value):
        st.session_state["aggregation_mode"] = value

    with elements("aggregation_mode_buttons"):
        mui.Typography(
            " What level of aggregation would you like to use for the analysis?",
            variant="subtitle1",
            sx={
                "fontFamily": "Calibri",
                "fontWeight": "bold",
                "color": "#4A4A68",
                "marginBottom": "10px",
                "textAlign": "left",
                "direction": "ltr"
            }
        )

        with mui.Box(
            sx={
                "display": "flex",
                "flexDirection": "row",
                "gap": "12px",
                "justifyContent": "flex-start",
                "marginBottom": "20px",
                "direction": "ltr",
                "width": "100%",
            }
        ):
            mui.Button(
                "Monthly",
                startIcon=mui.icon.CalendarTodayTwoTone,
                variant="contained" if st.session_state["aggregation_mode"] == "Monthly" else "outlined",
                color="secondary",
                onClick=lambda: set_aggregation_mode("Monthly"),
                sx={
                    "minWidth": "100px",
                    "fontWeight": "bold",
                    "paddingInline": "10px",
                    "border": "1px solid #EBE9F0",
                    "gap": "10px"
                }
            )

            mui.Button(
                "Weekly",
                startIcon=mui.icon.EventTwoTone,
                variant="contained" if st.session_state["aggregation_mode"] == "Weekly" else "outlined",
                color="secondary",
                onClick=lambda: set_aggregation_mode("Weekly"),
                sx={
                    "minWidth": "100px",
                    "fontWeight": "bold",
                    "paddingInline": "10px",
                    "border": "1px solid #EBE9F0",
                    "gap": "10px"
                }
            )


def rate_response():
    with elements("rating_section"):
        with mui.Box(sx={"display": "flex-end", "flexDirection": "column", "alignItems": "right", "marginTop": "20px"}):

            def handle_rating_change(event, value):
                st.session_state["Rating"] = value


                if "Logs" in st.session_state and not st.session_state["Logs"].empty:
                    last_index = st.session_state["Logs"].index[-1]
                    st.session_state["Logs"].at[last_index, "rating"] = value

            mui.Rating(
                name="feedback-rating",
                value=st.session_state.get("Rating", 0),
                onChange=handle_rating_change,
                size="small",
                precision=1
            )

def get_embedding_model():
    embedding_model = AzureOpenAI(azure_endpoint = os.getenv("AZURE_ENDPOINT"), api_key = os.getenv("OPENAI_API_KEY"), api_version = "2024-08-01-preview")
    return embedding_model


@st.cache_resource
def get_sql_engine():
    db_password = os.getenv("DB_PASSWORD")
    conn_str = (
        "mssql+pyodbc://analyticsadmin:"
        f"{db_password}@diplomat-analytics-server.database.windows.net/"
        "Diplochat-DB?driver=ODBC+Driver+17+for+SQL+Server&charset=utf8"
    )
    return create_engine(conn_str, fast_executemany=True)

def write_logs_to_sql(log_df: pd.DataFrame):

    if log_df.empty:
        return

    engine = get_sql_engine()

    try:
        log_df.to_sql("logs", con=engine, if_exists="append", index=False)
        st.session_state["Logs"] = pd.DataFrame(columns=log_df.columns)
        st.session_state["Rating"] = 0

    except Exception as e:
        st.error(f"Error on saving rows into logs table {e}")




def create_floating_sidebar():
    """
    This function uses the "Bridge" library in order to send JavaScript requests to Streamlit 
    and thus produce a floating menu of options for the user.
    Return: the updated choice stored in the Streamlit session state.
    """
    if 'bridg_counter' not in st.session_state:
        st.session_state.bridge_counter = 0
        
    unique_key = f"{st.session_state.bridge_counter}"
    button_click = bridge(name="button-click-bridge", default=None, key=unique_key)

    html(f"""
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">
        <style>
        /* Sidebar Container */
        .sidebar-container {{
            position: fixed;
            top: 50%;
            right: 20px;
            transform: translateY(-50%);
            width: 60px;
            height: auto;
            background-color: #f5f7fa;
            border-radius: 30px;
            box-shadow: 0px 4px 10px rgba(0, 0, 0, 0.1);
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            padding: 10px 0;
            z-index: 1000;
            direction: rtl;
        }}

        /* General Button Styles */
        .icon-btn {{
            background-color: #ffffff;
            color: #48547c;
            border: 2px solid #ffffff;
            padding: 10px;
            border-radius: 50%;
            font-size: 20px;
            box-shadow: 0px 2px 5px rgba(0, 0, 0, 0.1);
            cursor: pointer;
            width: 45px;
            height: 45px;
            display: flex;
            align-items: center;
            justify-content: center;
            margin-bottom: 10px;
            outline: none;
            position: relative;
        }}

        /* Hover Effect */
        .icon-btn:hover {{
            background-color: #707998;
            color: #ffffff;
        }}
        /* Specific Button Background Colors */
        .icon-btn-save {{
            background-color: #5f84ff;
            color: #3855b5;
        }}
         
        .icon-btn-save:hover {{
            background-color: #5274e3;
            color: #ffffff;
        }}

        .icon-btn-run {{
            background-color: #69bd52;
            color: #198600;

        }}
         
        .icon-btn-run:hover {{
            background-color: #388E3C;
            color: #ffffff;

        }}

        .icon-btn-filter {{
            background-color: #ffbb32;
            color: #863600;
        }}
        
         .icon-btn-filter:hover {{
            background-color: #d99100;
            color: #ffffff;
        }}

        .icon-btn-refresh {{
            background-color: #a271ff;
        }}
         
         
        .icon-btn-refresh:hover {{
            background-color: #8c57f1;
            color: #ffffff;
        }}
         
        .icon-btn-download {{
            background-color: #00bfa5;
            color:  #009688;
        }}
        
        .icon-btn-download:hover {{
            background-color: #00bfa5;
            color: white;
        }}
        .icon-btn-comax {{
            background-color: #ffffff;
            color: #69bd52;
        }}
        
         .icon-btn-comax:hover {{
            background-color: #dfdfdf;
            color: #69bd52;

        }} 
         
        .icon-btn-restore {{
            background-color: #FF6363;
            color: #CC4B4B;
        }}
        
         .icon-btn-restore:hover {{
            background-color: #DE5F5F;
            color: white;

        }} 



        /* Tooltip Styles */
        .tooltip {{
            position: absolute;
            top: 50%;
            right: 100%;
            transform: translateY(-50%) translateX(-15px);
            background-color: rgba(0, 0, 0, 0.75);
            color: white;
            padding: 5px 10px;
            border-radius: 5px;
            font-family: 'Calibri', sans-serif;
            font-size: 14px;
            white-space: nowrap;
            opacity: 0;
            pointer-events: none;
            transition: opacity 0.3s ease, transform 0.3s ease;
        }}

        .icon-btn:hover .tooltip {{
            opacity: 1;
            transform: translateY(-50%) translateX(-25px);
        }}
        </style>

        <div class="sidebar-container">
            <!-- Run Button -->
            <button class="icon-btn icon-btn-run {{ 'active' if st.session_state['side_bar_options'] == 'run' else '' }}" 
                    onClick="stBridges.send('button-click-bridge', 'run');">
                <i class="fa-solid fa-person-running"></i>
                <div class="tooltip">הרץ את האלגוריתם</div>
            </button>

            <!-- Save Button -->
            <button class="icon-btn icon-btn-save {{ 'active' if st.session_state['side_bar_options'] == 'save' else '' }}" 
                    onClick="stBridges.send('button-click-bridge', 'save');">
                <i class="fa-solid fa-floppy-disk"></i>
                <div class="tooltip">שמור שינויים</div>
            </button>

            <!-- Filter Button -->
            <button class="icon-btn icon-btn-filter {{ 'active' if st.session_state['side_bar_options'] == 'filter' else '' }}" 
                    onClick="stBridges.send('button-click-bridge', 'filter');">
                <i class="fa-solid fa-filter"></i>
                <div class="tooltip">סינון תוצאות המודל</div>
            </button>
            
            <!-- Refresh Button -->
            <button class="icon-btn icon-btn-refresh {{ 'active' if st.session_state['side_bar_options'] == 'refresh' else '' }}" 
                    onClick="stBridges.send('button-click-bridge', 'refresh');">
                <i class="fa-solid fa-chart-line"></i>
                <div class="tooltip">תוצאות המודל</div>
            </button>
            
            <!-- Download Current Dataframe Button -->
            <button class="icon-btn icon-btn-download" onClick="stBridges.send('button-click-bridge', 'download');">
                <i class="fa-solid fa-download"></i>
                <div class="tooltip">הורד קובץ CSV</div>
            </button>

            <!-- COMAX Button -->
            <button class="icon-btn icon-btn-comax  {{ 'active' if st.session_state['side_bar_options'] == 'comax' else ''}}" 
                    onClick="stBridges.send('button-click-bridge', 'comax');">
                <i class="fa-solid fa-c"></i>
                <div class="tooltip">הזנת נתונים למערכת COMAX</div>
            </button>
         
            <!-- RESTORE Button -->
            <button class="icon-btn icon-btn-restore  {{ 'active' if st.session_state['side_bar_options'] == 'restore' else ''}}" 
                    onClick="stBridges.send('button-click-bridge', 'restore');">
                <i class="fa-solid fa-arrow-rotate-right"></i>
                <div class="tooltip">שחזר נתונים לנקודה האחרונה</div>
            </button>
        </div>
    """)
    
    
    return button_click