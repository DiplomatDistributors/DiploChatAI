import streamlit as st
import numpy as np
import pandas as pd
import base64
from io import BytesIO
import matplotlib.pyplot as plt
import re
from alive_progress import alive_bar
from streamlit_elements import elements, mui
import sys
from pathlib import Path
import os
from streamlit_navigation_bar import st_navbar
import time
import msal
import json
import requests
from langchain.memory import ConversationBufferMemory
from langchain_core.messages import HumanMessage, AIMessage

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
    pages = ["דף הבית", "הגדרות כלליות"]
    logo_path = "logo.svg"

    styles = {
        "nav": {
            "background": "linear-gradient(0deg, rgba(239,236,245,1) 0%, rgba(246,246,246,1) 22%, rgba(255,255,255,1) 56%)",
            "height": "6.25rem",
            "font-size": "25px",
            "width": "100%",
            "font-family": "Calibri, sans-serif",
            "direction": "rtl",
            "justify-content": "space-between",
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

    if 'local_scope' not in st.session_state:
        st.session_state['local_scope'] = st.session_state['Dataframes']
        st.session_state['local_scope'].update({'pd':pd,'np':np,'base64':base64,'BytesIO':BytesIO,'plt':plt})

    return st.session_state['local_scope']

def rate_response():
    with elements("rating_section"):
        with mui.Box(sx={"display": "flex", "flexDirection": "column", "alignItems": "right", "marginTop": "20px"}):

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