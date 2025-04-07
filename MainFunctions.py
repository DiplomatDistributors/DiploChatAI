import streamlit as st
import numpy as np
import pandas as pd
import base64
from io import BytesIO
import matplotlib.pyplot as plt
import re
from alive_progress import alive_bar
import time
import msal
import json
import requests

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

def load_lottie_file(filepath: str):
    with open(filepath, "r") as f:
        return json.load(f)
    
def clean_code_answer(python_code):
    cleaned_code = re.sub(r'```python', '', python_code)  # הסרת תגית הפתיחה
    cleaned_code = re.sub(r'```', '', cleaned_code)  # הסרת תגית הסגירה

    # שלב 2: ניקוי הקוד (הסרת print ו-import)
    cleaned_code = re.sub(r"^(\s*)print\(", r"\1#print(", cleaned_code, flags=re.MULTILINE)
    cleaned_code = re.sub(r"^(\s*)import\s", r"\1#import ", cleaned_code, flags=re.MULTILINE)
    return cleaned_code

def get_local_scope():

    if 'local_scope' not in st.session_state:
        st.session_state['local_scope'] = st.session_state['Dataframes']
        st.session_state['local_scope'].update({'pd':pd,'np':np,'base64':base64,'BytesIO':BytesIO,'plt':plt})

    return st.session_state['local_scope']