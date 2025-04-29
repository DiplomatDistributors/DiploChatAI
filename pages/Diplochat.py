import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))

from DiploModel import *
from Dataloader import *
from MainFunctions import * 
from Homepage import *
from agents.generator import *
from agents.decorator import *

import streamlit as st
from streamlit_navigation_bar import st_navbar
from streamlit_lottie import st_lottie

import os
from dotenv import load_dotenv
import traceback
import urllib.parse
import ast


load_css()

if 'page' not in st.session_state:
    st.session_state['page'] = None

if 'Agents' not in st.session_state:
    st.session_state['Agents'] = {'Generator' : GeneratorAgent() , 'Decorator' : DecoratorAgent()}

if 'Diplochat' not in st.session_state:
    st.session_state['Diplochat'] = DiploChat()

if 'Dataloader' not in st.session_state:
    st.session_state['Dataloader'] = DataLoader()

if 'Dataframes' not in st.session_state:
    st.session_state['Dataframes'] = None

if "Conversation" not in st.session_state:  
        st.session_state['Conversation'] = []
        
if 'last_prompt_sent' not in st.session_state:
    st.session_state.last_prompt_sent = None

if "user" not in st.session_state or not st.session_state["user"]:

    query_params = st.query_params
    if "user_data" in query_params:
        try:
            decoded_user = urllib.parse.unquote(query_params["user_data"])
            st.session_state["user"] = ast.literal_eval(decoded_user)
        except Exception as e:
            st.error("❌ שגיאה בשחזור נתוני המשתמש")
            st.stop()


thinking_animation = load_lottie_file(os.path.join(parent_dir, "progress-animation.json"))
load_dotenv()

    
pages = ["דף הבית", "הגדרות כלליות"]
logo_path = os.path.join(parent_dir, "logo.svg")

styles = {
    "nav": {
        "background": "linear-gradient(0deg, rgba(239,236,245,1) 0%, rgba(246,246,246,1) 22%, rgba(255,255,255,1) 56%)",
        "height": "6.25rem",
        "font-size": "25px",
        "width": "100%",  # Set the width to 80% of the container
        "font-family": "Calibri, sans-serif",
        "direction": "rtl",  # Set direction to RTL
        "justify-content": "space-between",
        "box-shadow": "0px 4px 10px rgba(0, 0, 0, 0.3)" # Adds a shadow effect

    },
    "img": {
        "height": "7.875rem",
        "margin-right": "65px",
    },
    "li": {
        "padding": "0 35px",
        "white-space": "nowrap",
        "margin-left" : "13px",
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

st.session_state['page'] = st_navbar(pages,logo_path=logo_path,styles=styles,options=options)

if st.session_state['page'] == 'Home':
    st.write(f"✅ ברוך הבא  {st.session_state['user']['displayName']} !")
    st.write(f"📧 אימייל : {st.session_state['user']['mail']}")

if st.session_state['page'] == 'דף הבית':
    
    if st.session_state['Dataframes'] is None:
        st.session_state['Dataframes'] = st.session_state['Dataloader'].load_data_with_progress()
        time.sleep(0.5)
        st.rerun()
    
    else:    

        for message in st.session_state['Conversation']:  
            if message["role"] == 'assistant':  
                with st.chat_message("assistant"):                   
                    st.markdown(message["content"], unsafe_allow_html=True)

            elif message["role"] == 'user':  
                with st.chat_message("user"):     
                    st.markdown(message["content"])


        if prompt := st.chat_input("איך אפשר לעזור?"):
            st.session_state['Conversation'].append({"role": "user", "content": prompt})

            with st.chat_message("user"):
                st.markdown(prompt)

            thinking_placeholder = st.empty()
            with thinking_placeholder:
                st_lottie(thinking_animation, height=70, key="loading")

            time.sleep(1.5)
            thinking_placeholder.empty()

            with st.chat_message("assistant"):
                generator_agent = st.session_state['Agents']['Generator']
                decorator_agent = st.session_state['Agents']['Decorator']
                
                answer = generator_agent.response(prompt)
                local_scope = get_local_scope()
                
                max_retries = 15
                retries = 0
                success = False

                while not success and retries < max_retries:
                    try:

                        exec(answer.python_code, {}, local_scope)

                        agent_result = local_scope.get("result", "⚠️ לא נמצאה תשובה.")
                        st.code(answer.python_code)
                        decorator_result = decorator_agent.decorate(prompt, answer.python_code_explanation, agent_result)

                        placeholder = st.empty()
                        streamed_text = ""
                        for char in decorator_result:
                            streamed_text += char
                            placeholder.markdown(streamed_text, unsafe_allow_html=True)
                            time.sleep(0.01)

                        st.session_state['Conversation'].append({"role": "assistant", "content": decorator_result})
                        success = True
                        time.sleep(1)
                        
                    except Exception as e:
                        retries += 1
                        if retries >= max_retries:
                            st.error(f"❌ נכשל לאחר {max_retries} ניסיונות. שגיאה: {str(e)}")

