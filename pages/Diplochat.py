import sys
from pathlib import Path
from st_bridge import bridge , html

parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))

from DiploModel import *
from Dataloader import *
from MainFunctions import *
from Homepage import *
from agents.extractor import *
from agents.planner import *
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
import uuid
import time

load_css()
load_dotenv()

if 'page' not in st.session_state:
    st.session_state['page'] = None

if "Logs" not in st.session_state:
    st.session_state["Logs"] = pd.DataFrame(columns=["user","timestamp","agent","attempts","calls","error","num_exec","exec_error","retry","duration","question","answer","rating"])

if 'Last_log' not in st.session_state:
    st.session_state['Last_log'] = {}

if "aggregation_mode" not in st.session_state:
    st.session_state["aggregation_mode"] = None

if "local_scope" not in st.session_state:
    st.session_state["local_scope"] = {}

if "last_scope_mode" not in st.session_state:
    st.session_state["last_scope_mode"] = None
    
if 'Conversation' not in st.session_state:
    st.session_state['Conversation'] = InMemoryChatMessageHistory()

if "Rating" not in st.session_state:
    st.session_state["Rating"] = 0

if 'Dataloader' not in st.session_state:
    st.session_state['Dataloader'] = DataLoader()

if 'Dataframes' not in st.session_state:
    st.session_state['Dataframes'] = None

if 'Agents' not in st.session_state:
    st.session_state['Agents'] = {}

if "user" not in st.session_state or not st.session_state["user"]:
    query_params = st.query_params
    if "user_data" in query_params:
        try:
            decoded_user = urllib.parse.unquote(query_params["user_data"])
            st.session_state["user"] = ast.literal_eval(decoded_user)
        except Exception as e:
            st.error("❌ שגיאה בשחזור נתוני המשתמש")
            st.stop()

is_admin = st.session_state["user"]["mail"] == "doleva@diplomat-global.com"

thinking_animation = load_lottie_file(os.path.join(parent_dir, "progress-animation.json"))
create_navigation_bar()


if st.session_state['page'] == 'Home':
    st.write(f"✅ Wellcome {st.session_state['user']['displayName']} !")
    st.write(f"📧 Email : {st.session_state['user']['mail']}")

if st.session_state['page'] == "Home Page":
    
    if st.session_state['Dataframes'] is None:
        loader = st.session_state['Dataloader']
        if loader.running_local:
            st.session_state['Dataframes'] = load_selected_parquets()
        else:    
            st.session_state['Dataframes'] = load_data_with_progress(loader.parquet_dir)

        if 'vector_database' not in st.session_state:
            st.session_state['vector_database'] = load_vector_database(loader.parquet_dir)

        st.session_state['Agents']['ExtractorAgent'] = ExtractorAgent(st.session_state['vector_database'])
        st.session_state['Agents']['PlannerAgent'] = PlannerAgent()
        st.session_state['Agents']['GeneratorAgent'] = GeneratorAgent()
        st.session_state['Agents']['DecoratorAgent'] = DecoratorAgent()
        time.sleep(0.15)
        st.rerun()
    
    else:
        create_aggregation_option()
        conversation_history = st.session_state["Conversation"]   
             
        if conversation_history:
            for message in conversation_history.messages:
                if message.type == 'ai':
                    with st.chat_message("assistant"):
                        st.markdown(message.content, unsafe_allow_html=True)
                elif message.type == 'human':
                    with st.chat_message("user"):
                        st.markdown(message.content)

        if not st.session_state["Logs"].empty:
            write_logs_to_sql(st.session_state["Logs"])
            
        if prompt := st.chat_input("How can i assist you?"):

            conversation_history.add_user_message(prompt)

            with st.chat_message("user"):
                st.markdown(prompt)

            thinking_placeholder = st.empty()
            with thinking_placeholder:
                st_lottie(thinking_animation, height=150, key="loading")


            extracor_agent = st.session_state['Agents']['ExtractorAgent']
            planner_agent =  st.session_state['Agents']['PlannerAgent']
            generator_agent = st.session_state['Agents']['GeneratorAgent']
            decorator_agent = st.session_state['Agents']['DecoratorAgent']

            entities = extracor_agent.response(prompt)
            plan = planner_agent.response(prompt , entities)
            answer = generator_agent.response(plan , prompt)

            local_scope = get_local_scope()

            max_retries = 15
            retries = 0
            success = False

            while not success and retries < max_retries:
                try:
                    exec(answer.python_code, {}, local_scope)
                    agent_result = local_scope.get("result", "⚠️ לא נמצאה תשובה.")
                    if is_admin:
                        st.code(plan)
                        st.code(answer.python_code)
                    decorator_result = decorator_agent.decorate(prompt, plan , agent_result)
                    
                    thinking_placeholder.empty()

                    with st.chat_message("assistant"):
                    
                        placeholder = st.empty()
                        streamed_text = ""
                        for char in decorator_result:
                            streamed_text += char
                            placeholder.markdown(streamed_text, unsafe_allow_html=True)
                            time.sleep(0.01)

                        conversation_history.add_ai_message(decorator_result)
                        
                        success = True
                        generator_agent.update_memory(plan , prompt , answer , decorator_result) # We need to do that becuase the generator returns pydantic object

                except Exception as e:
                    
                    retries += 1
                    if retries >= max_retries:
                        st.error(f"❌ נכשל לאחר {max_retries} ניסיונות. שגיאה: {str(e)}")
                        break
                    
                    error_message = traceback.format_exc()
                    generator_agent.set_num_exec(retries)
                    generator_agent.set_exec_error(e)
                    generator_agent.set_log("GeneratorAgent", st.session_state["user"]["mail"] , was_retry = True)

                    answer = generator_agent.retry_with_error(user_question = prompt, previous_code = answer.python_code, error_message = error_message)

            if success:
                generator_agent.set_log("GeneratorAgent", st.session_state["user"]["mail"])
                decorator_agent.set_log("DecoratorAgent", st.session_state["user"]["mail"])
                rate_response()